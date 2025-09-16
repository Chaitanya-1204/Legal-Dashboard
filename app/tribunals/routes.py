# app/tribunals/routes.py
# Routes for the Tribunals section (uses fast 'catalog' when available; falls back to main collection).
# Templates are kept compatible by returning the same variable names your HTML expects.
import traceback
from flask import Blueprint, render_template, abort, request, jsonify, url_for as _url_for  # NEW (url_for alias)
from urllib.parse import unquote
from bs4 import BeautifulSoup
from jinja2 import TemplateNotFound
from werkzeug.routing import BuildError  # NEW

from app import db  # shared PyMongo database handle
from opennyai_html_ner import OpenNyAIHtmlNER

tribunals_bp = Blueprint("tribunals", __name__, template_folder="../templates")

# -----------------------
# NER: lazy singleton
# -----------------------
_NER_ENGINE = None
def _get_ner_engine():
    global _NER_ENGINE
    if _NER_ENGINE is None:
        _NER_ENGINE = OpenNyAIHtmlNER(
            use_gpu=False,
            model_name="en_legal_ner_trf",
            do_sentence_level=True,
            do_postprocess=False,
            prefer_spacy_direct=True,
        )
    return _NER_ENGINE

# -----------------------
# Helpers
# -----------------------
def _catalog_has(kind: str, level: str, extra: dict | None = None) -> bool:
    """Return True if the catalog has at least one row for the given filter."""
    filt = {"kind": kind, "level": level}
    if extra:
        filt.update(extra)
    return "catalog" in db.list_collection_names() and db.catalog.count_documents(filt, limit=1) > 0

def _find_one_both(col, base):
    """Try with doc_id as int & str (covers mixed storage)."""
    did = base.get("doc_id")
    q = [{"doc_id": did}, {"doc_id": str(did)}] if isinstance(did, int) else [{"doc_id": did}, {"doc_id": int(did)}] if str(did).isdigit() else [base]
    for cond in q:
        doc = col.find_one(cond, {"_id": 0})
        if doc:
            return doc
    return None

def prepare_tribunal_html_and_roles(full_html: str):
    """Trim page chrome, keep annotated body, and collect [data-structure] roles."""
    soup = BeautifulSoup(full_html or "", "html.parser")

    # If a full page was captured, keep the inner ".judgments" region; else keep body/root.
    container = soup.select_one(".judgments") or soup.body or soup

    # Drop noisy tags
    for t in container.select("script, style, noscript"):
        t.decompose()

    # If annotations exist, trim to first..last annotated block
    annotated = container.select("[data-structure]")
    if annotated:
        # Trim prelude
        first = annotated[0]
        top_first = first
        while top_first.parent and top_first.parent is not container:
            top_first = top_first.parent
        for sib in list(top_first.previous_siblings):
            sib.extract()
        # Trim tail
        last = annotated[-1]
        top_last = last
        while top_last.parent and top_last.parent is not container:
            top_last = top_last.parent
        for sib in list(top_last.next_siblings):
            sib.extract()

    # Remove common chrome that doesn't contain annotations
    for sel in [
        ".header", "header", ".navbar", "#navbar", ".footer", "footer",
        ".sidebar", "#sidebar", ".adv", ".ads", ".ad", ".share", ".tools",
        ".translate", "#google_translate_element", ".logo", ".search",
    ]:
        for el in container.select(sel):
            if not el.select_one("[data-structure]"):
                el.decompose()

    cleaned_html = str(container)

    # Compute role order (first appearance) and assign colors
    role_order = []
    seen = set()
    for el in container.select("[data-structure]"):
        raw = (el.get("data-structure") or "").strip()
        k = raw.lower()
        if raw and k not in seen:
            seen.add(k)
            role_order.append(raw)

    palette = [
        "rgba(96,165,250,.18)",  # blue
        "rgba(251,191,36,.20)",  # amber
        "rgba(167,139,250,.18)", # violet
        "rgba(52,211,153,.18)",  # emerald
        "rgba(244,114,182,.18)", # pink
        "rgba(248,113,113,.18)", # red
        "rgba(56,189,248,.18)",  # sky
        "rgba(250,204,21,.18)",  # yellow
        "rgba(163,230,53,.18)",  # lime
        "rgba(251,113,133,.18)", # rose
    ]
    role_colors = {r.lower(): palette[i % len(palette)] for i, r in enumerate(role_order)}

    return cleaned_html, role_order, role_colors

# -----------------------
# Local safe url_for (shadows Jinja's url_for for this render only)
# -----------------------
def _safe_url_for(endpoint: str, **values) -> str:
    """Return app url or empty string if endpoint doesn't exist (prevents BuildError in template)."""
    try:
        return _url_for(endpoint, **values)
    except BuildError:
        return ""  # empty action attr is harmless and keeps page rendering

# -----------------------
# Routes
# -----------------------

@tribunals_bp.route("/")
def show_categories():
    """
    Show all tribunal types (distinct category_name).
    Uses catalog (kind=tribunal, level=category) if present; otherwise falls back to db.tribunals.
    Template expects: grouped_data=[{_id: "Tribunals", categories: [..]}]
    """
    try:
        if _catalog_has("tribunal", "category"):
            cats = list(
                db.catalog.find(
                    {"kind": "tribunal", "level": "category"},
                    {"_id": 0, "category_name": 1},
                ).sort("category_name", 1)
            )
            grouped = [{"_id": "Tribunals", "categories": [c["category_name"] for c in cats]}]
        else:
            # Fallback: group from the main collection
            pipeline = [
                {"$group": {"_id": "$law_type", "categories": {"$addToSet": "$category_name"}}},
                {"$sort": {"_id": 1}},
            ]
            grouped = list(db.tribunals.aggregate(pipeline))
            for g in grouped:
                g["categories"] = sorted(g.get("categories", []))

        return render_template("tribunals_categories.html", grouped_data=grouped)
    except Exception as e:
        print(f"[TRIBUNALS][show_categories] {e}")
        abort(500)

@tribunals_bp.route("/<category_name>/")
def show_years(category_name):
    """
    For a given tribunal (category_name), show all years.
    Template expects: years=[...], category_name=...
    """
    category_name = unquote(category_name)
    try:
        if _catalog_has("tribunal", "year", {"category_name": category_name}):
            years = [
                d["year"]
                for d in db.catalog.find(
                    {"kind": "tribunal", "level": "year", "category_name": category_name},
                    {"_id": 0, "year": 1},
                ).sort("year", 1)
            ]
        else:
            years = sorted(db.tribunals.distinct("year", {"category_name": category_name}))

        if not years:
            abort(404)
        return render_template("tribunals_years.html", years=years, category_name=category_name)
    except Exception as e:
        print(f"[TRIBUNALS][show_years] {e}")
        abort(500)

@tribunals_bp.route("/<category_name>/<int:year>/")
def list_tribunals_by_year(category_name, year):
    """
    List docs (title + doc_id) for a given (category_name, year).
    Template expects: tribunals=[{doc_id, full_title}], category_name, year
    """
    category_name = unquote(category_name)
    try:
        if _catalog_has("tribunal", "doc", {"category_name": category_name, "year": year}):
            items = list(
                db.catalog.find(
                    {
                        "kind": "tribunal",
                        "level": "doc",
                        "category_name": category_name,
                        "year": year,
                    },
                    {"_id": 0, "doc_id": 1, "full_title": 1},
                ).sort("full_title", 1)
            )
        else:
            items = list(
                db.tribunals.find(
                    {"category_name": category_name, "year": year},
                    {"_id": 0, "doc_id": 1, "full_title": 1},
                ).sort("full_title", 1)
            )

        if not items:
            abort(404)
        return render_template(
            "tribunals_list.html", tribunals=items, category_name=category_name, year=year
        )
    except Exception as e:
        print(f"[TRIBUNALS][list_tribunals_by_year] {e}")
        abort(500)

@tribunals_bp.route("/view/<int:doc_id>")
def view_tribunals(doc_id: int):
    """
    Render a single tribunal document (HTML in 'content').
    IMPORTANT:
    - Do NOT redirect here (no ?highlight=). Tribunal docs open themselves.
    - Self-rows in `document_links` would otherwise cause an infinite loop.
    """
    try:
        # ❌ REMOVE any child→parent redirect logic here. Just fetch and render.
        # If your collection sometimes stores doc_id as string, try both:
        doc = db.tribunals.find_one({"doc_id": doc_id}, {"_id": 0})
        if not doc:
            doc = db.tribunals.find_one({"doc_id": str(doc_id)}, {"_id": 0})
        if not doc:
            abort(404, description="Tribunal doc not found")

        # Normalize fields your template expects
        try:
            doc["doc_id"] = int(doc.get("doc_id", doc_id))
        except Exception:
            doc["doc_id"] = doc_id
        doc["full_title"] = doc.get("full_title") or doc.get("title") or f"Doc {doc_id}"
        doc.setdefault("year", "")
        doc.setdefault("category_name", "")
        doc.setdefault("law_type", "")

        # (Optional) clean HTML before display; otherwise pass-through
        cleaned_html = (doc.get("content") or "")
        doc["content"] = cleaned_html

        # Render with a SAFE url_for injected (shadows Flask's url_for only for this render).
        # This prevents BuildError in template when summary blueprint isn't registered.
        try:
            return render_template(
                "view_tribunals.html",
                tribunals=doc,
                url_for=_safe_url_for,  # NEW: safe override only in this template render
            )
        except TemplateNotFound:
            # (Kept for compatibility; renders the same template name)
            return render_template(
                "view_tribunals.html",
                tribunals=doc,
                url_for=_safe_url_for,  # NEW
            )

    except Exception as e:
        # Print full traceback to your console so you can see the exact cause
        print("[TRIBUNALS][view_tribunals] EXCEPTION")
        traceback.print_exc()
        abort(500)

@tribunals_bp.route("/api/ner/<int:doc_id>")
def api_ner_html(doc_id: int):
    """
    API endpoint: returns NER-annotated HTML for a tribunal doc.
    Response: { html: "<annotated html>" }
    """
    try:
        base = _find_one_both(db.tribunals, {"doc_id": doc_id})
        if not base:
            abort(404)

        cleaned_html, _, _ = prepare_tribunal_html_and_roles(base.get("content", ""))
        annotated = _get_ner_engine().annotate_html(cleaned_html) if cleaned_html else ""
        return jsonify({"html": annotated})
    except Exception as e:
        print(f"[TRIBUNALS][api_ner_html] {e}")
        return jsonify({"error": str(e)}), 500

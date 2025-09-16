# app/districtcourt/routes.py
# Routes for District Courts / High Courts section.
# Uses optional `catalog` (kind="district_court") if present; falls back to main collection `district_court`.

from flask import Blueprint, render_template, abort, redirect, url_for, request, jsonify
from urllib.parse import unquote
from bs4 import BeautifulSoup

from app import db  # shared PyMongo handle
from opennyai_html_ner import OpenNyAIHtmlNER

districtcourt_bp = Blueprint("districtcourt", __name__, template_folder="../templates")

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
    """True if `catalog` has at least one row matching."""
    filt = {"kind": kind, "level": level}
    if extra:
        filt.update(extra)
    return "catalog" in db.list_collection_names() and db.catalog.count_documents(filt, limit=1) > 0


def _prepare_dc_html_and_roles(full_html: str):
    """Trim page chrome, keep annotated body, and collect [data-structure] roles."""
    soup = BeautifulSoup(full_html or "", "html.parser")
    container = soup.select_one(".judgments") or soup.body or soup

    # remove noise
    for t in container.select("script, style, noscript"):
        t.decompose()

    annotated = container.select("[data-structure]")
    if annotated:
        # Trim everything before first annotated block
        first = annotated[0]
        top_first = first
        while top_first.parent and top_first.parent is not container:
            top_first = top_first.parent
        for sib in list(top_first.previous_siblings):
            sib.extract()
        # Trim everything after last annotated block
        last = annotated[-1]
        top_last = last
        while top_last.parent and top_last.parent is not container:
            top_last = top_last.parent
        for sib in list(top_last.next_siblings):
            sib.extract()

    # Drop common chrome-only elements that don’t contain annotations
    for sel in [
        ".header", "header", ".navbar", "#navbar", ".footer", "footer",
        ".sidebar", "#sidebar", ".adv", ".ads", ".ad", ".share", ".tools",
        ".translate", "#google_translate_element", ".logo", ".search",
    ]:
        for el in container.select(sel):
            if not el.select_one("[data-structure]"):
                el.decompose()

    cleaned_html = str(container)

    # collect unique role order
    role_order, seen = [], set()
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
# Routes
# -----------------------

@districtcourt_bp.route("/")
def show_categories():
    """
    Categories page: list all distinct courts (category_name).
    Context for template `dc_categories.html`:
      grouped_data = [{ "_id": "District Courts", "categories": [<court names>]}]
    """
    try:
        if _catalog_has("district_court", "category"):
            cats = list(
                db.catalog.find(
                    {"kind": "district_court", "level": "category"},
                    {"_id": 0, "category_name": 1},
                ).sort("category_name", 1)
            )
            grouped = [{"_id": "District Courts", "categories": [c["category_name"] for c in cats]}]
        else:
            # Fallback: from main collection
            names = sorted(db.district_court.distinct("category_name"))
            grouped = [{"_id": "District Courts", "categories": names}]

        return render_template("districtcourt_categories.html", grouped_data=grouped)
    except Exception as e:
        print(f"[DISTRICTCOURT][categories] {e}")
        abort(500)


@districtcourt_bp.route("/<category_name>/")
def show_years(category_name):
    """
    Years page for a given court (category_name).
    Context for template `dc_years.html`:
      years=[...], category_name=<court>
    """
    category_name = unquote(category_name)
    try:
        if _catalog_has("district_court", "year", {"category_name": category_name}):
            years = [
                d["year"]
                for d in db.catalog.find(
                    {"kind": "district_court", "level": "year", "category_name": category_name},
                    {"_id": 0, "year": 1},
                ).sort("year", 1)
            ]
        else:
            years = sorted(db.district_court.distinct("year", {"category_name": category_name}))

        if not years:
            abort(404)
        return render_template("districtcourt_years.html", years=years, category_name=category_name)
    except Exception as e:
        print(f"[DISTRICTCOURT][years] {e}")
        abort(500)


@districtcourt_bp.route("/<category_name>/<int:year>/")
def list_by_year(category_name, year):
    """
    List page for a given (category_name, year).
    Renders districtcourt_list.html with:
      docs=[{doc_id, full_title}], category_name, year
    """
    category_name = unquote(category_name)
    try:
        # Try catalog first with the exact filter
        items = []
        if _catalog_has("district_court", "doc", {
            "category_name": category_name, "year": int(year)
        }):
            items = list(
                db.catalog.find(
                    {
                        "kind": "district_court",
                        "level": "doc",
                        "category_name": category_name,
                        "year": int(year),
                    },
                    {"_id": 0, "doc_id": 1, "full_title": 1},
                ).sort([("full_title", 1), ("doc_id", 1)])
            )

        # Fallback if catalog is missing OR returned zero
        if not items:
            items = list(
                db.district_court.find(
                    {"category_name": category_name, "year": int(year)},
                    {"_id": 0, "doc_id": 1, "full_title": 1},
                ).sort([("full_title", 1), ("doc_id", 1)])
            )

        # quick visibility in the console
        print(f"[DISTRICTCOURT][list_by_year] {category_name}/{year} -> "
              f"{len(items)} items; sample: {[x.get('doc_id') for x in items[:3]]}")

        return render_template(
            "districtcourt_list.html",
            docs=items, category_name=category_name, year=year
        )
    except Exception as e:
        print(f"[DISTRICTCOURT][list_by_year] ERROR: {e}")
        abort(500)



@districtcourt_bp.route("/view/<int:doc_id>")
def view(doc_id: int):
    """
    View a single document by doc_id.
    Context for template `dc_view.html`:
      doc=<mongo_doc_with_content>, role_order=[...], role_colors={...}
    Note: if doc_id collisions across courts are possible, consider switching
    to '/<category_name>/view/<doc_id>' and query on both keys.
    """
    try:
        # Optional child→parent redirect via document_links
        link_info = db.document_links.find_one({"doc_id": doc_id})
        if link_info and link_info.get("parent_doc_id"):
            return redirect(
                url_for("districtcourt.view", doc_id=int(link_info["parent_doc_id"]), highlight=doc_id)
            )

        # If you expect collisions, change this to include category_name in the filter.
        doc = db.district_court.find_one({"doc_id": doc_id}, {"_id": 0})
        if not doc:
            abort(404)

        # Normalize a couple of fields for template
        doc["doc_id"] = int(doc.get("doc_id") or doc_id)
        doc["title"] = doc.get("full_title") or doc.get("title") or f"Doc {doc_id}"

        cleaned_html, role_order, role_colors = _prepare_dc_html_and_roles(doc.get("content", ""))
        doc["content"] = cleaned_html

        return render_template(
            "dc_view.html",
            doc=doc,
            role_order=role_order,
            role_colors=role_colors,
            ner_api_url=url_for("districtcourt.api_ner_html", doc_id=doc["doc_id"]),
        )
    except Exception as e:
        print(f"[DISTRICTCOURT][view] {e}")
        abort(500)


@districtcourt_bp.route("/api/ner/<int:doc_id>")
def api_ner_html(doc_id: int):
    """Return NER-annotated HTML for a district-court doc: { html: "<annotated>" }"""
    try:
        doc = db.district_court.find_one({"doc_id": doc_id}, {"_id": 0, "content": 1})
        if not doc:
            abort(404)

        cleaned_html, _, _ = _prepare_dc_html_and_roles(doc.get("content", ""))
        annotated = _get_ner_engine().annotate_html(cleaned_html) if cleaned_html else ""
        print(annotated)
        return jsonify({"html": annotated})
    except Exception as e:
        print(f"[DISTRICTCOURT][api_ner_html] {e}")
        return jsonify({"error": str(e)}), 500

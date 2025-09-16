# app/judgments/routes.py
from __future__ import annotations

import os
import threading
from typing import Iterable, Tuple

from flask import (
    Blueprint,
    render_template,
    abort,
    request,
    jsonify,
    Response,
    url_for as _url_for,  # alias to guard with _safe_url_for in templates
)
from werkzeug.routing import BuildError
from bs4 import BeautifulSoup

from app import db
from opennyai_html_ner import OpenNyAIHtmlNER

# ---- Optional .env loader (harmless to keep) --------------------------------
try:
    from dotenv import load_dotenv, find_dotenv

    _loaded = False
    for fname in [os.getenv("ENV_FILE"), ".env", ".en"]:
        if not fname:
            continue
        path = find_dotenv(filename=fname, usecwd=True)
        if path:
            load_dotenv(path, override=False)
            _loaded = True
            break
    if not _loaded:
        _here = os.path.dirname(__file__)
        _root = os.path.abspath(os.path.join(_here, "..", ".."))
        for fname in [".env", ".en"]:
            p = os.path.join(_root, fname)
            if os.path.exists(p):
                load_dotenv(p, override=False)
                _loaded = True
                break
    if not _loaded:
        print("[judgments] No .env/.en found (this is OK if env vars are set another way).")
except Exception as e:
    print(f"[judgments] dotenv load skipped: {e}")

judgments_bp = Blueprint("judgments", __name__, template_folder="../templates")

# ---------- helpers ----------------------------------------------------------

def _all_years():
    try:
        if (
            "catalog" in db.list_collection_names()
            and db.catalog.count_documents({"kind": "judgment", "level": "year"}) > 0
        ):
            yrs = [
                d.get("year")
                for d in db.catalog.find(
                    {"kind": "judgment", "level": "year"}, {"_id": 0, "year": 1}
                )
                if d.get("year") is not None
            ]
        else:
            yrs = [y for y in db.judgments.distinct("year") if y is not None]
        return sorted(set(yrs))
    except Exception as e:
        print(f"[judgments._all_years] error: {e}")
        return []

def _safe_year_for_clear(preferred_year, results):
    if isinstance(preferred_year, int):
        return preferred_year
    for r in results or []:
        if isinstance(r.get("year"), int):
            return r["year"]
    yrs = _all_years()
    return yrs[-1] if yrs else 1950

def _read_text_file(path: str) -> str | None:
    if not path or path.startswith(("http://", "https://")):
        return None
    try:
        with open(path, "rb") as f:
            data = f.read()
        for enc in ("utf-8", "cp1252", "latin-1"):
            try:
                return data.decode(enc)
            except UnicodeDecodeError:
                continue
        return data.decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"[judgments._read_text_file] failed to read {path}: {e}")
        return None

def _html_from_doc(j: dict) -> str | None:
    """
    Pull HTML in this order:
      1) NER/HTML fields (when available)
      2) 'content_html' (legacy) or 'content' (DB)
      3) file contents at 'path'
    """
    for k in (
        "content_ner_html", "ner_html",
        "content_html", "html", "raw_html", "body_html",
        "content"
    ):
        v = j.get(k)
        if isinstance(v, str) and v.strip():
            return v
    p = j.get("path") or j.get("file_path") or j.get("local_path")
    if p and os.path.exists(p):
        html = _read_text_file(p)
        if isinstance(html, str) and html.strip():
            return html
    return None

# ---------- Local safe url_for (for this blueprint's renders only) -----------

def _safe_url_for(endpoint: str, **values) -> str:
    """Return URL or empty string if endpoint missing (prevents BuildError in template)."""
    try:
        return _url_for(endpoint, **values)
    except BuildError:
        return ""

# ---------- OpenNyAI Engine (lazy singleton) + helpers -----------------------

_NER_ENGINE: OpenNyAIHtmlNER | None = None
_INFLIGHT_LOCKS: dict[int, threading.Lock] = {}
_LOCKS_MUTEX = threading.Lock()

def _get_doc_lock(doc_id: int) -> threading.Lock:
    with _LOCKS_MUTEX:
        lock = _INFLIGHT_LOCKS.get(doc_id)
        if lock is None:
            lock = threading.Lock()
            _INFLIGHT_LOCKS[doc_id] = lock
        return lock

def _get_ner_engine() -> OpenNyAIHtmlNER:
    global _NER_ENGINE
    if _NER_ENGINE is None:
        _NER_ENGINE = OpenNyAIHtmlNER(
            prefer_spacy_direct=True,   # try spaCy model first
            do_postprocess=False,       # avoid E030 if fallback to OpenNyAI
        )
        print(f"[NER] Initialized. mode={getattr(_NER_ENGINE, '_mode', None)}")
    return _NER_ENGINE

def _engine_mode() -> str:
    eng = _get_ner_engine()
    return getattr(eng, "_mode", None) or "disabled"

def _count_spans(html: str) -> int:
    if not isinstance(html, str):
        return 0
    # cheap but effective
    return html.count('class="ner') + html.count("class='ner")

def _prepare_sc_html_with_selector(full_html: str, selector: str | None) -> Tuple[str, str]:
    """
    Return (cleaned_html, used_selector_label).
    Tries explicit selector, then ".judgments", then <body>, then root.
    """
    soup = BeautifulSoup(full_html or "", "html.parser")
    container = None
    used = ""
    if selector:
        try:
            container = soup.select_one(selector)
            if container is not None:
                used = selector
        except Exception:
            container = None
    if container is None:
        container = soup.select_one(".judgments")
        if container is not None:
            used = ".judgments"
    if container is None:
        container = soup.body or soup
        used = "body" if soup.body else "root"

    # Drop noisy tags
    for t in container.select("script, style, noscript"):
        t.decompose()
    return str(container), used

# ---------- Years ------------------------------------------------------------

@judgments_bp.route("/")
def years():
    try:
        use_catalog = (
            "catalog" in db.list_collection_names()
            and db.catalog.count_documents({"kind": "judgment", "level": "year"}) > 0
        )
        if use_catalog:
            years = sorted(
                d["year"]
                for d in db.catalog.find(
                    {"kind": "judgment", "level": "year"}, {"_id": 0, "year": 1}
                )
                if "year" in d and d["year"] is not None
            )
        else:
            years = sorted(y for y in db.judgments.distinct("year") if y is not None)
        return render_template("sc_years.html", years=years or [])
    except Exception as e:
        print(f"[judgments.years] DB error: {e}")
        abort(500)

# ---------- List by year (filters + search) ---------------------------------

@judgments_bp.route("/<int:year>/")
def list_by_year(year: int):
    try:
        q = (request.args.get("q") or "").strip()
        month = (request.args.get("month") or "").strip()
        day = (request.args.get("day") or "").strip()

        mongo_query = {"year": year}
        if month.isdigit():
            mongo_query["month"] = int(month)
        if day.isdigit():
            mongo_query["day"] = int(day)
        if q:
            ors = []
            if q.isdigit():
                ors.append({"doc_id": int(q)})
            ors.append({"title": {"$regex": q, "$options": "i"}})
            ors.append({"full_title": {"$regex": q, "$options": "i"}})
            mongo_query["$or"] = ors

        use_catalog = (
            "catalog" in db.list_collection_names()
            and db.catalog.count_documents(
                {"kind": "judgment", "level": "doc", "year": year}
            ) > 0
        )

        results = []
        if use_catalog:
            docs = list(
                db.catalog.find(
                    {"kind": "judgment", "level": "doc", "year": year},
                    {"_id": 0, "doc_id": 1, "full_title": 1, "title": 1,
                     "year": 1, "month": 1, "day": 1},
                )
            )

            def _matches(d):
                if month.isdigit() and d.get("month") != int(month): return False
                if day.isdigit() and d.get("day") != int(day):       return False
                if not q: return True
                if q.isdigit() and d.get("doc_id") == int(q): return True
                text = (d.get("full_title") or d.get("title") or "")
                return q.lower() in text.lower()

            docs = [d for d in docs if _matches(d)]
            ids = [d.get("doc_id") for d in docs if d.get("doc_id") is not None]
            exist_ids = set(
                r["doc_id"] for r in db.judgments.find(
                    {"doc_id": {"$in": ids}}, {"_id": 0, "doc_id": 1}
                )
            )
            for d in docs:
                results.append({
                    "doc_id": d.get("doc_id"),
                    "title": d.get("full_title") or d.get("title"),
                    "full_title": d.get("full_title"),
                    "year": d.get("year"),
                    "month": d.get("month"),
                    "day": d.get("day"),
                    "exists": d.get("doc_id") in exist_ids,
                })
        else:
            fields = {"_id": 0, "doc_id": 1, "full_title": 1, "title": 1,
                      "year": 1, "month": 1, "day": 1}
            for it in db.judgments.find(mongo_query, fields):
                results.append({
                    "doc_id": it.get("doc_id"),
                    "title": it.get("full_title") or it.get("title"),
                    "full_title": it.get("full_title"),
                    "year": it.get("year"),
                    "month": it.get("month"),
                    "day": it.get("day"),
                    "exists": True,
                })

        results.sort(key=lambda x: (x.get("month") or 0,
                                    x.get("day") or 0,
                                    (x.get("title") or "").lower()))
        return render_template("sc_list.html", judgments=results, year=year, q=q)
    except Exception as e:
        print(f"[judgments.list_by_year] error: {e}")
        abort(500)

# ---------- View -------------------------------------------------------------

@judgments_bp.route("/view/<int:doc_id>")
def view(doc_id: int):
    """
    Render a single judgment.
    Templates expect 'content_html', so we source it from:
      j['content'] (your DB) OR j['content_html'] OR file at j['path'].
    """
    try:
        j = db.judgments.find_one({"doc_id": doc_id}, {"_id": 0})
        if not j:
            abort(404)

        # Title fallback
        if not j.get("title"):
            j["title"] = j.get("full_title")

        # Pull HTML from your 'content' field (or other fallbacks)
        html = _html_from_doc(j)

        # NOTE: pass a SAFE url_for into the template so {{ url_for('summary.view_from_html') }}
        # won't crash when summary isn't registered.
        return render_template("sc_view.html", judgment=j, content_html=html, url_for=_safe_url_for)
    except Exception as e:
        print(f"[judgments.view] error: {e}")
        abort(500)

# ---------- Optional NER viewer page ----------------------------------------

@judgments_bp.route("/view_html/<int:doc_id>")
def view_html(doc_id: int):
    """
    An HTML-first view with client-side toggle for RAW/NER.
    """
    j = db.judgments.find_one({"doc_id": doc_id}, {"_id": 0})
    if not j:
        abort(404)
    if not j.get("title"):
        j["title"] = j.get("full_title") or f"Judgment {doc_id}"
    raw = _html_from_doc(j) or ""
    # allow passing a different container selector via query (?container_selector=.mydiv)
    selector = request.args.get("container_selector")
    cleaned, used = _prepare_sc_html_with_selector(raw, selector)
    j["content"] = cleaned
    return render_template("view_sc_ner.html", judgment=j, used_selector=used, url_for=_safe_url_for)

# ---------- Search (reuses sc_list.html) ------------------------------------

@judgments_bp.route("/search")
def search():
    try:
        q = (request.args.get("q") or "").strip()
        month = (request.args.get("month") or "").strip()
        day = (request.args.get("day") or "").strip()
        year_param = request.args.get("year")
        preferred_year = int(year_param) if year_param and year_param.isdigit() else None

        query = {}
        if preferred_year is not None: query["year"] = preferred_year
        if month.isdigit():            query["month"] = int(month)
        if day.isdigit():              query["day"] = int(day)
        if q:
            ors = []
            if q.isdigit(): ors.append({"doc_id": int(q)})
            ors += [
                {"title": {"$regex": q, "$options": "i"}},
                {"full_title": {"$regex": q, "$options": "i"}},
            ]
            query["$or"] = ors

        fields = {"_id": 0, "doc_id": 1, "full_title": 1, "title": 1,
                  "year": 1, "month": 1, "day": 1}

        results = []
        for it in db.judgments.find(query, fields).limit(300):
            results.append({
                "doc_id": it.get("doc_id"),
                "title": it.get("full_title") or it.get("title"),
                "full_title": it.get("full_title"),
                "year": it.get("year"),
                "month": it.get("month"),
                "day": it.get("day"),
                "exists": True,
            })

        year_for_clear = _safe_year_for_clear(preferred_year, results)
        return render_template("sc_list.html", judgments=results, q=q, year=year_for_clear)
    except Exception as e:
        print(f"[judgments.search] error: {e}")
        abort(500)

# ---------- NER API (OpenNyAI) ----------------------------------------------

@judgments_bp.route("/api/ner/<int:doc_id>", endpoint="api_ner_html")
def api_ner_html(doc_id: int):
    """
    Returns annotated HTML as JSON:
      {"html": "<span class='ner ...'>...</span>...", "mode": "...", "span_count": N, "container_used": "..."}
    - Add ?debug=1 to return the annotated HTML directly (text/html) for quick inspection.
    - Add ?container_selector=.mydiv to override the container selection.
    """
    try:
        j = db.judgments.find_one(
            {"doc_id": doc_id},
            {"_id": 0, "content_ner_html": 1, "ner_html": 1,
             "content_html": 1, "html": 1, "raw_html": 1, "body_html": 1,
             "content": 1, "path": 1, "doc_id": 1}
        )
        if not j:
            return jsonify({"error": "not_found"}), 404

        force = (request.args.get("force") == "1")
        debug = (request.args.get("debug") == "1")
        container_selector = request.args.get("container_selector")

        # Serve cached immediately if present and not forcing
        if not force and not debug:
            pre = j.get("content_ner_html") or j.get("ner_html")
            if isinstance(pre, str) and pre.strip():
                return jsonify({"html": pre, "mode": "cache", "span_count": _count_spans(pre), "container_used": "cached"})

        # Gather raw HTML
        raw = (j.get("content_html") or j.get("html") or j.get("raw_html") or j.get("body_html") or j.get("content"))
        if not raw:
            p = j.get("path") or j.get("file_path") or j.get("local_path")
            if p and os.path.exists(p):
                raw = _read_text_file(p)
        if not isinstance(raw, str) or not raw.strip():
            return jsonify({"error": "no_html"}), 404

        cleaned, used = _prepare_sc_html_with_selector(raw, container_selector)

        # Stampede protection per doc
        lock = _get_doc_lock(doc_id)
        with lock:
            # double-check cache inside lock
            if not force and not debug:
                cached = db.judgments.find_one({"doc_id": doc_id}, {"_id": 0, "content_ner_html": 1})
                if cached and isinstance(cached.get("content_ner_html"), str) and cached["content_ner_html"].strip():
                    pre = cached["content_ner_html"]
                    return jsonify({"html": pre, "mode": "cache", "span_count": _count_spans(pre), "container_used": "cached"})

            engine = _get_ner_engine()
            mode = _engine_mode()

            if mode == "disabled":
                # make this loudly visible so you don't assume it worked
                return jsonify({"error": "ner_disabled", "detail": "spaCy model not found and OpenNyAI pipeline unavailable.", "mode": mode}), 503

            annotated = engine.annotate_html(cleaned) if cleaned else cleaned
            spans = _count_spans(annotated)

            # Debug view: render annotated HTML directly for a visual check
            if debug:
                return Response(annotated or "", mimetype="text/html")

            # Cache best-effort
            try:
                db.judgments.update_one({"doc_id": doc_id}, {"$set": {"content_ner_html": annotated}}, upsert=False)
            except Exception as uerr:
                print(f"[judgments.api_ner_html] cache update failed: {uerr}")

            return jsonify({"html": annotated, "mode": mode, "span_count": spans, "container_used": used})

    except Exception as e:
        print(f"[judgments.api_ner_html] error: {e}")
        return jsonify({"error": "server_error", "detail": str(e)}), 500

# app/judgments/routes.py
from flask import render_template, abort, request, url_for, redirect
from app import db  # shared Mongo client from app factory (app/__init__.py)
from . import judgments_bp, SC_COLLECTION
# ADD: import the helper you already created
from opennyai_html_ner import OpenNyAIHtmlNER

# ADD: lazy singleton (top-level in this module)
_NER_ENGINE = None

# app/judgments/routes.py  (ADD THIS)
from flask import jsonify
from opennyai_html_ner import OpenNyAIHtmlNER

_NER_ENGINE = None
# def _get_ner_engine():
#     global _NER_ENGINE
#     if _NER_ENGINE is None:
#         try:
#             _NER_ENGINE = OpenNyAIHtmlNER(use_gpu=False, model_name="en_legal_ner_trf")
#         except Exception as e:
#             print(f"[NER] Falling back to small model due to: {e}")
#             _NER_ENGINE = OpenNyAIHtmlNER(use_gpu=False, model_name="en_legal_ner_sm")
#     return _NER_ENGINE

_NER_ENGINE = None
def _get_ner_engine():
    global _NER_ENGINE
    if _NER_ENGINE is None:
        _NER_ENGINE = OpenNyAIHtmlNER(
            use_gpu=False,
            model_name="en_legal_ner_trf",
            do_sentence_level=True,
            do_postprocess=False,    # not used in spaCy-direct mode, but safe
            prefer_spacy_direct=True
        )
    return _NER_ENGINE



@judgments_bp.route('/api/ner/<doc_id>')
def api_ner_html(doc_id: str):
    try:
        doc = COLLECTION().find_one({"doc_id": str(doc_id)}, {"_id": 0})
        if not doc:
            abort(404)
        raw_html = doc.get("content_html") or ""
        annotated = _get_ner_engine().annotate_html(raw_html)
        return jsonify({"html": annotated})
    except Exception as e:
        print(f"[SC][NER-API] {e}")
        return jsonify({"error": str(e)}), 500


# def get_ner_engine() -> OpenNyAIHtmlNER:
#     global _NER_ENGINE
#     if _NER_ENGINE is not None:
#         return _NER_ENGINE
#     try:
#         # If your transformer stack is installed, use the big legal model
#         _NER_ENGINE = OpenNyAIHtmlNER(use_gpu=False, model_name="en_legal_ner_trf")
#     except Exception as e:
#         # If you had tokenizers/Rust/transformers conflicts, fall back to the small model
#         print(f"[NER] Falling back to en_legal_ner_sm due to: {e}")
#         _NER_ENGINE = OpenNyAIHtmlNER(use_gpu=False, model_name="en_legal_ner_sm")
#     return _NER_ENGINE


def COLLECTION():
    return getattr(db, SC_COLLECTION)

@judgments_bp.route('/')
def years():
    """
    Show all available years for Supreme Court judgments (desc order).
    """
    try:
        pipeline = [
            {"$match": {"year": {"$type": "int"}}},
            {"$group": {"_id": "$year", "count": {"$sum": 1}}},
            {"$sort": {"_id": -1}}  # newest first
        ]
        groups = list(COLLECTION().aggregate(pipeline))
        years = [g["_id"] for g in groups]
        return render_template("sc_years.html", years=years)
    except Exception as e:
        print(f"[SC] Error building years: {e}")
        abort(500)

@judgments_bp.route('/<int:year>')
def list_by_year(year: int):
    """
    List judgments for a given year with optional filters:
    - month (1..12)
    - day   (1..31)
    - q     (search by exact doc_id or title substring)
    """
    try:
        # Read filters from query string
        m = request.args.get('month', type=int)
        d = request.args.get('day', type=int)
        q = (request.args.get('q') or '').strip()

        # Base query
        base = {'year': year}
        if m is not None and 1 <= m <= 12:
            base['month'] = m
        if d is not None and 1 <= d <= 31:
            base['day'] = d

        # Add search if present
        query = base
        if q:
            query = {
                '$and': [
                    base,
                    {'$or': [
                        {'doc_id': q},
                        {'title': {'$regex': q, '$options': 'i'}}
                    ]}
                ]
            }

        projection = {
            '_id': 0, 'doc_id': 1, 'title': 1,
            'year': 1, 'month': 1, 'day': 1,
            'path': 1, 'content_html': 1
        }

        docs = list(COLLECTION().find(query, projection))

        # Normalize for template & compute 'exists' (content available)
        norm = []
        for ddoc in docs:
            doc_id = ddoc.get('doc_id')
            if doc_id is None:
                continue
            if not isinstance(doc_id, str):
                doc_id = str(doc_id)

            title = ddoc.get('title') or ""
            if not isinstance(title, str):
                title = str(title)

            exists = bool(ddoc.get('content_html')) or bool(ddoc.get('path'))

            norm.append({
                'doc_id': doc_id,
                'title': title,
                'year': ddoc.get('year'),
                'month': ddoc.get('month'),
                'day': ddoc.get('day'),
                'exists': exists,
            })

        # Safe sort: month, day, then title
        def s_int(x, default=0):
            try:
                return int(x) if x is not None else default
            except Exception:
                return default

        norm.sort(key=lambda x: (s_int(x.get('month')), s_int(x.get('day')), (x.get('title') or '').lower()))

        return render_template('sc_list.html', year=year, judgments=norm, q=q)
    except Exception as e:
        print(f"[SC] list_by_year({year}) failed: {e}", flush=True)
        abort(500)

# ADD: new annotated view
@judgments_bp.route('/view/<doc_id>/ner')
def view_with_ner(doc_id: str):
    """
    Show the same judgment, but with OpenNyAI NER spans injected into content_html.
    Leaves the original /view/<doc_id> untouched.
    """
    try:
        doc = COLLECTION().find_one({"doc_id": str(doc_id)}, {"_id": 0})
        if not doc:
            abort(404)

        raw_html = doc.get("content_html") or None
        annotated_html = _get_ner_engine().annotate_html(raw_html) if raw_html else None

        # Reuse your existing template; CSS you added styles <span class="ner ...">
        return render_template("sc_view.html", judgment=doc, content_html=annotated_html)
    except Exception as e:
        print(f"[SC][NER] Error viewing doc_id={doc_id}: {e}")
        abort(500)


@judgments_bp.route('/view/<doc_id>')
def view(doc_id: str):
    """
    Show a single judgment. If 'content_html' exists, render it.
    """
    try:
        doc = COLLECTION().find_one({"doc_id": str(doc_id)}, {"_id": 0})
        if not doc:
            abort(404)
        content_html = doc.get("content_html") or None
        return render_template("sc_view.html", judgment=doc, content_html=content_html)
    except Exception as e:
        print(f"[SC] Error viewing doc_id={doc_id}: {e}")
        abort(500)

@judgments_bp.route('/search')
def search():
    """
    Simple search by doc_id (exact) or title (substring).
    """
    q = (request.args.get("q") or "").strip()
    if not q:
        return redirect(url_for("judgments.years"))

    try:
        query = {"$or": [{"doc_id": q}, {"title": {"$regex": q, "$options": "i"}}]}
        results = list(
            COLLECTION().find(query, {"_id": 0, "doc_id": 1, "title": 1, "year": 1}).limit(100)
        )

        # If query looks like a doc_id and exactly one match, jump to it
        if q.isdigit() and len(results) == 1 and results[0].get("doc_id") == q:
            return redirect(url_for("judgments.view", doc_id=q))

        return render_template("sc_list.html", year=None, judgments=results, q=q)
    except Exception as e:
        print(f"[SC] Error searching q={q}: {e}")
        abort(500)

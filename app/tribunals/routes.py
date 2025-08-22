# app/tribunals/routes.py
from flask import Blueprint, render_template, abort, redirect, url_for, request
from urllib.parse import unquote
from app import db  # shared PyMongo database handle
from flask import render_template, redirect, url_for, abort
from bs4 import BeautifulSoup

tribunals_bp = Blueprint('tribunals', __name__, template_folder='../templates')


# ADD: import the helper you already created
from opennyai_html_ner import OpenNyAIHtmlNER

# ADD: lazy singleton (top-level in this module)
_NER_ENGINE = None

# app/judgments/routes.py  (ADD THIS)
from flask import jsonify
from opennyai_html_ner import OpenNyAIHtmlNER


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



@tribunals_bp.route('/api/ner/<int:doc_id>')
def api_ner_html(doc_id: int):
    try:
        doc = db.tribunals.find_one({"doc_id": doc_id}, {"_id": 0})
        if not doc:
            abort(404)
        cleaned_html, _, _ = prepare_tribunal_html_and_roles(doc.get("content", ""))
        annotated = _get_ner_engine().annotate_html(cleaned_html) if cleaned_html else ""
        return jsonify({"html": annotated})
    except Exception as e:
        print(f"[TRIBUNALS][NER-API] {e}")
        return jsonify({"error": str(e)}), 500




# ---- helper: clean + extract judgment + find roles ----
def prepare_tribunal_html_and_roles(full_html: str):
    soup = BeautifulSoup(full_html or "", "html.parser")

    # Pick the main container that holds the annotations
    container = soup.select_one(".judgments") or soup.body or soup

    # Remove noise tags globally within container
    for t in container.select("script, style, noscript"):
        t.decompose()

    # If the container has top/bottom chrome, trim it to the first/last annotated block
    annotated = container.select("[data-structure]")
    if annotated:
        # Trim everything BEFORE the first annotated block
        first = annotated[0]
        top_first = first
        while top_first.parent and top_first.parent is not container:
            top_first = top_first.parent
        for sib in list(top_first.previous_siblings):
            sib.extract()

        # Trim everything AFTER the last annotated block
        last = annotated[-1]
        top_last = last
        while top_last.parent and top_last.parent is not container:
            top_last = top_last.parent
        for sib in list(top_last.next_siblings):
            sib.extract()

    # Extra pass: drop obvious site-chrome blocks that don't contain annotations
    for sel in [
        ".header", "header", ".navbar", "#navbar", ".footer", "footer",
        ".sidebar", "#sidebar", ".adv", ".ads", ".ad", ".share", ".tools",
        ".translate", "#google_translate_element", ".logo", ".search"
    ]:
        for el in container.select(sel):
            if not el.select_one("[data-structure]"):
                el.decompose()

    # Build the final HTML (keep the wrapper to preserve local context)
    cleaned_html = str(container)

    # Discover roles in encounter order (case-preserving)
    role_order = []
    seen = set()
    for el in container.select("[data-structure]"):
        raw = (el.get("data-structure") or "").strip()
        k = raw.lower()
        if raw and k not in seen:
            seen.add(k)
            role_order.append(raw)

    # Dynamic palette (extend if you like)
    palette = [
        "rgba(96,165,250,.18)",   # blue
        "rgba(251,191,36,.20)",   # amber
        "rgba(167,139,250,.18)",  # violet
        "rgba(52,211,153,.18)",   # emerald
        "rgba(244,114,182,.18)",  # pink
        "rgba(248,113,113,.18)",  # red
        "rgba(56,189,248,.18)",   # sky
        "rgba(250,204,21,.18)",   # yellow
        "rgba(163,230,53,.18)",   # lime
        "rgba(251,113,133,.18)",  # rose
    ]
    role_colors = {r.lower(): palette[i % len(palette)] for i, r in enumerate(role_order)}

    return cleaned_html, role_order, role_colors



@tribunals_bp.route('/')
def show_categories():
    """
    Show all tribunal types (distinct category_name), grouped by law_type.
    Your documents have: law_type='tribunal', category='tribunals', category_name='<TribunalName>'.
    """
    try:
        # Group by law_type and collect distinct tribunal names (category_name)
        pipeline = [
            {"$group": {"_id": "$law_type", "categories": {"$addToSet": "$category_name"}}},
            {"$sort": {"_id": 1}}
        ]
        grouped = list(db.tribunals.aggregate(pipeline))

        # Sort categories alphabetically inside each group
        for g in grouped:
            g["categories"] = sorted(g.get("categories", []))

        # Use a tribunals-specific template (recommended). If you want to reuse the Acts template,
        # make sure it expects 'grouped_data' with {_id, categories}.
        return render_template('tribunals_categories.html', grouped_data=grouped)
    except Exception as e:
        print(f"Database error in show_categories: {e}")
        abort(500)


@tribunals_bp.route('/<category_name>/')
def show_years(category_name):
    """
    For a given tribunal (category_name), show all years available (ascending).
    """
    category_name = unquote(category_name)
    try:
        # IMPORTANT: filter by category_name (not 'category', which is the constant 'tribunals')
        years = sorted(db.tribunals.distinct("year", {"category_name": category_name}))
        if not years:
            abort(404)
        return render_template('tribunals_years.html', years=years, category_name=category_name)
    except Exception as e:
        print(f"Database error in show_years: {e}")
        abort(500)


@tribunals_bp.route('/<category_name>/<int:year>/')
def list_tribunals_by_year(category_name, year):
    """
    For a given tribunal (category_name) and year, list all docs (title + doc_id).
    """
    category_name = unquote(category_name)
    try:
        # Again: filter by category_name
        items = list(
            db.tribunals.find(
                {"category_name": category_name, "year": year},
                {"full_title": 1, "doc_id": 1, "_id": 0}
            ).sort("full_title", 1)
        )
        if not items:
            abort(404)
        # Template expects 'tribunals' list
        return render_template('tribunals_list.html', tribunals=items,
                               category_name=category_name, year=year)
    except Exception as e:
        print(f"Database error in list_tribunals_by_year: {e}")
        abort(500)

@tribunals_bp.route('/view/<int:doc_id>')
def view_tribunals(doc_id: int):
    try:
        # Optional child→parent redirect
        link_info = db.document_links.find_one({"doc_id": doc_id})
        if link_info and link_info.get("parent_doc_id"):
            return redirect(url_for(
                "tribunals.view_tribunals",
                doc_id=link_info["parent_doc_id"],
                highlight=doc_id
            ))

        doc = db.tribunals.find_one({"doc_id": doc_id}, {"_id": 0})
        if not doc:
            abort(404)

        # normalize fields so the template has everything it needs
        doc["doc_id"] = int(doc.get("doc_id") or doc_id)
        doc["title"]  = doc.get("full_title") or doc.get("title") or f"Doc {doc_id}"

        # Clean the stored tribunals HTML for first render (plain; NER comes from the button)
        cleaned_html, role_order, role_colors = prepare_tribunal_html_and_roles(doc.get("content", ""))
        doc["content"] = cleaned_html  # <-- template will do {{ tribunals.content|safe }}

        return render_template(
            "view_tribunals.html",
            tribunals=doc,  # <-- key: pass the name your template expects
            role_order=role_order,
            role_colors=role_colors,
            ner_api_url=url_for("tribunals.api_ner_html", doc_id=doc["doc_id"]),  # <-- tribunals API
        )
    except Exception as e:
        print(f"Database error in view_tribunals: {e}")
        abort(500)


# @tribunals_bp.route('/view/<int:doc_id>')
# def view_tribunals(doc_id):
#     """
#     Render a single tribunal document (HTML stored in 'content').
#     If a child link exists, redirect to the parent's page with ?highlight=<child_doc_id>.
#     """
#     try:
#         # Optional: handle child→parent linking via a separate collection
#         link_info = db.document_links.find_one({"doc_id": doc_id})
#         if link_info and link_info.get("parent_doc_id"):
#             return redirect(url_for(
#                 "tribunals.view_tribunals",
#                 doc_id=link_info["parent_doc_id"],
#                 highlight=doc_id
#             ))

#         doc = db.tribunals.find_one({"doc_id": doc_id}, {"_id": 0})
#         if not doc:
#             abort(404)


                

#         cleaned_html, role_order, role_colors = prepare_tribunal_html_and_roles(doc.get("content", ""))

#         # Optionally: persist cleaned HTML back to DB so we don't re-clean each request
#         # db.tribunals.update_one({"doc_id": doc_id}, {"$set": {"content": cleaned_html}})
#         raw_html = doc.get("content_html") or None
#         annotated_html = _get_ner_engine().annotate_html(raw_html) if raw_html else None

#         doc["content"] = annotated_html
#         return render_template(
#             "view_tribunals.html",
#             tribunals=doc,
#             role_order=role_order,
#             role_colors=role_colors,
#         )    

#         # Template should use {{ tribunals.content|safe }} (or whatever field names you prefer)
#         # return render_template("view_tribunals.html", tribunals=doc)
#     except Exception as e:
#         print(f"Database error in view_tribunals: {e}")
#         abort(500)

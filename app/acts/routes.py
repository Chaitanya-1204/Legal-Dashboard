# app/acts/routes.py
# Routes and logic for the 'Acts' section (uses `catalog` when available)

from flask import Blueprint, render_template, abort, current_app
from urllib.parse import unquote
from app import db  # shared Mongo connection from the app factory

acts_bp = Blueprint('acts', __name__, template_folder='../templates')

# --- simple grouping helper for categories (no DB change needed) ---
STATE_UT_NAMES = {
    # States
    "Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chhattisgarh","Goa","Gujarat",
    "Haryana","Himachal Pradesh","Jharkhand","Karnataka","Kerala","Madhya Pradesh",
    "Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Punjab",
    "Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura","Uttar Pradesh",
    "Uttarakhand","West Bengal",
    # UTs (old/new spellings)
    "Andaman and Nicobar Islands","Chandigarh","Dadra And Nagar Haveli",
    "Dadra and Nagar Haveli","Daman and Diu",
    "Dadra and Nagar Haveli and Daman and Diu",
    "Delhi","NCT Delhi","Jammu and Kashmir","Ladakh","Lakshadweep","Puducherry",
    # a few local bodies sometimes present in data
    "Greater Bengaluru City Corporation"
}

def _bucket_for_category(cat: str) -> str:
    """
    Heuristic grouping for the categories page when using `catalog`.
    Returns one of: 'Central Acts', 'State Acts', 'British India'
    """
    if not cat or not cat.strip():
        return "Central Acts"

    name = cat.strip()
    low = name.casefold()

    # --- British India / Historical buckets ---
    historical_exact = {
        "british india",
        "bombay presidency",
        "madras presidency",
        "bengal presidency",
        "central provinces and berar",
        "central provinces & berar",
        "chota nagpur division",
        "mysore state",
        "nagpur province",
        "punjab province",
        "united province",
        "vindhya province",
        "bhopal state",
    }
    if any(key in low for key in historical_exact) or "presidency" in low or "province" in low or "division" in low:
        return "British India"

    # --- State / UT buckets (modern + local bodies) ---
    if low.startswith("state of ") or low.startswith("ut ") or low.startswith("nct "):
        return "State Acts"

    state_ut_hints = {
        # States
        "andhra pradesh","arunachal pradesh","assam","bihar","chhattisgarh","goa","gujarat",
        "haryana","himachal pradesh","jammu & kashmir","jammu-kashmir","jammu and kashmir",
        "jharkhand","karnataka","kerala","madhya pradesh","maharashtra","manipur",
        "meghalaya","mizoram","nagaland","odisha","punjab","rajasthan","sikkim",
        "tamil nadu","tamilnadu","telangana","tripura","uttarakhand","uttar pradesh",
        "west bengal","madhya bharat",
        # UTs (old/new names included)
        "delhi","nct delhi","chandigarh","puducherry","lakshadweep","ladakh",
        "andaman and nicobar islands","daman and diu","dadra and nagar haveli",
        "dadra & nagar haveli","dadra and nagar haveli and daman and diu",
        # Local bodies / special buckets
        "greater bengaluru city corporation","bengaluru city corporation","city corporation",
        "municipal","municipality","panchayat","zilla parishad"
    }
    if any(h in low for h in state_ut_hints):
        return "State Acts"

    central_hints = {
        "union of india",
        "constitution",
        "constitutional amendment",
        "united nations convention",
        "un convention",
        "international treaty",
        "treaty - act",
        "parliament",
    }
    if any(h in low for h in central_hints):
        return "Central Acts"

    return "Central Acts"


@acts_bp.route('/')
def show_categories():
    """
    Categories page.
    Prefer tiny `catalog` (kind=act, level=category); fallback to previous aggregation on `acts`.
    """
    try:
        use_catalog = ("catalog" in db.list_collection_names()
                       and db.catalog.count_documents({"kind": "act", "level": "category"}) > 0)

        grouped_data = []

        if use_catalog:
            rows = list(db.catalog.find(
                {"kind": "act", "level": "category"},
                {"_id": 0, "act_category": 1}
            ))

            buckets = {
                "Central Acts": [],
                "State Acts": [],
                "British India": []
            }
            for r in rows:
                cat = r.get("act_category")
                buckets[_bucket_for_category(cat)].append(cat)

            order = ["Central Acts", "State Acts", "British India"]
            for label in order:
                cats = sorted(set(buckets[label]))
                if cats:
                    grouped_data.append({"_id": label, "categories": cats})

        else:
            pipeline = [
                {"$group": {"_id": "$law_type", "categories": {"$addToSet": "$category"}}},
                {"$addFields": {
                    "sortOrder": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": ["$_id", "Central Acts"]}, "then": 1},
                                {"case": {"$eq": ["$_id", "State Acts"]}, "then": 2},
                                {"case": {"$regexMatch": {"input": "$_id", "regex": "British India"}}, "then": 3},
                            ],
                            "default": 4
                        }
                    }
                }},
                {"$sort": {"sortOrder": 1, "_id": 1}},
            ]
            grouped_data = list(db.acts.aggregate(pipeline))
            for g in grouped_data:
                g["categories"].sort()

        return render_template('acts_categories.html', grouped_data=grouped_data)

    except Exception as e:
        current_app.logger.exception(f"[acts.show_categories] DB error: {e}")
        abort(500)


@acts_bp.route('/<category_name>/')
def show_years(category_name):
    """
    Years page for a given category.
    Prefer `catalog` (kind=act, level=year); fallback to distinct on `acts`.
    """
    category_name = unquote(category_name)
    try:
        use_catalog = ("catalog" in db.list_collection_names()
                       and db.catalog.count_documents({"kind": "act", "level": "year", "act_category": category_name}) > 0)

        if use_catalog:
            years = sorted([
                d["year"] for d in db.catalog.find(
                    {"kind": "act", "level": "year", "act_category": category_name},
                    {"_id": 0, "year": 1}
                )
                if "year" in d and d["year"] is not None
            ])
        else:
            years = sorted(db.acts.distinct("year", {"category": category_name}))

        if not years:
            abort(404)

        return render_template('acts_years.html', years=years, category_name=category_name)

    except Exception as e:
        current_app.logger.exception(f"[acts.show_years] DB error: {e}")
        abort(500)


@acts_bp.route('/<category_name>/<int:year>/')
def list_acts_by_year(category_name, year):
    """
    List page for a given (category, year).
    Prefer `catalog` (kind=act, level=doc); fallback to querying `acts`.
    """
    category_name = unquote(category_name)
    try:
        use_catalog = ("catalog" in db.list_collection_names()
                       and db.catalog.count_documents({
                           "kind": "act", "level": "doc",
                           "act_category": category_name, "year": year
                       }) > 0)

        if use_catalog:
            acts_list = list(db.catalog.find(
                {"kind": "act", "level": "doc", "act_category": category_name, "year": year},
                {"_id": 0, "doc_id": 1, "full_title": 1}
            ))
        else:
            acts_list = list(db.acts.find(
                {"category": category_name, "year": year},
                {"_id": 0, "doc_id": 1, "full_title": 1}
            ))

        if not acts_list:
            abort(404)

        acts_list.sort(key=lambda x: (x.get("full_title") or "").lower())

        return render_template('acts_list.html',
                               acts=acts_list,
                               category_name=category_name,
                               year=year)

    except Exception as e:
        current_app.logger.exception(f"[acts.list_acts_by_year] DB error: {e}")
        abort(500)


@acts_bp.route('/view/<int:doc_id>', strict_slashes=False)
def view_act(doc_id):
    """
    View page: show exactly this Act's own HTML.
    - No auto-redirects based on document_links (prevents 'Amended by ...' jumps)
    - Accepts doc_id stored as int or str
    - Allows trailing slash in the URL
    """
    try:
        ids = [doc_id, str(doc_id)]
        act = db.acts.find_one(
            {'doc_id': {'$in': ids}},
            {'_id': 0, 'doc_id': 1, 'title': 1, 'full_title': 1, 'year': 1,
             'category': 1, 'law_type': 1, 'content': 1, 'content_html': 1}
        )
        if not act:
            current_app.logger.warning(f"[acts.view_act] Not found: doc_id={doc_id} (checked int/str).")
            abort(404)

        # Normalize display fields
        act['title'] = act.get('title') or act.get('full_title') or f"Act {doc_id}"
        act['content'] = act.get('content') or act.get('content_html') or ''

        return render_template('view_act.html', act=act)

    except Exception as e:
        current_app.logger.exception(f"[acts.view_act] Error for doc_id={doc_id}: {e}")
        abort(500)

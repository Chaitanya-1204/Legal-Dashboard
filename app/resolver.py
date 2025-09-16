# app/resolver.py
from flask import Blueprint, redirect, url_for, abort
from app import db

resolver_bp = Blueprint("resolver", __name__)

def _to_int_or_none(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None

def _ids_query(doc_id):
    """Build an id-tolerant query (int & str) for cross-collection consistency."""
    return {"$in": [doc_id, str(doc_id)]}

def _find_act_parent(doc_id: int):
    """
    If doc_id is present in document_links(kind='act'), return its parent_doc_id (as int).
    Returns None if no mapping exists.
    """
    link = db.document_links.find_one(
        {"kind": "act", "doc_id": _ids_query(doc_id)},
        {"_id": 0, "parent_doc_id": 1}
    )
    return _to_int_or_none((link or {}).get("parent_doc_id"))

def _has_cycle(child_id: int, parent_id: int) -> bool:
    """
    Detect a simple A <-> B cycle in document_links(kind='act').
    If parent points back to child, treat as cycle.
    """
    return bool(
        db.document_links.find_one(
            {
                "kind": "act",
                "doc_id": _ids_query(parent_id),
                "parent_doc_id": _ids_query(child_id),
            },
            {"_id": 1}
        )
    )

@resolver_bp.route("/doc/<int:doc_id>")
def resolve_doc(doc_id: int):
    """
    Universal resolver with Section fast-path:
      - FAST PATH: if doc_id is a Section (present in section_index) -> open Act directly
      - Else Acts mapping:
          * if parent == child  -> open Act directly (no highlight)
          * if parent != child  -> open parent with ?highlight=child (cycle-safe)
      - Else direct collection hits (acts/judgments/tribunals/district)
      - Else (optional) fall back to catalog
    """
    # 0) FAST PATH — Section? open directly (no parent search, no highlight)
    if db.section_index.find_one({"doc_id": _ids_query(doc_id)}, {"_id": 1}):
        return redirect(url_for("acts.view_act", doc_id=doc_id))

    # 1) Prefer Act child→parent mapping (for subsections)
    parent_id = _find_act_parent(doc_id)
    if parent_id is not None:
        # Explicit rule: same id => open act directly (no highlight)
        if parent_id == doc_id:
            return redirect(url_for("acts.view_act", doc_id=doc_id))

        # Avoid ping-pong (A <-> B) by opening parent with no highlight
        if _has_cycle(doc_id, parent_id):
            return redirect(url_for("acts.view_act", doc_id=parent_id))

        # Normal child -> parent highlight flow
        return redirect(url_for("acts.view_act", doc_id=parent_id, highlight=doc_id))

    # 2) Top-level direct hits (id-tolerant)
    if db.acts.find_one({"doc_id": _ids_query(doc_id)}, {"_id": 1}):
        return redirect(url_for("acts.view_act", doc_id=doc_id))
    if db.judgments.find_one({"doc_id": _ids_query(doc_id)}, {"_id": 1}):
        return redirect(url_for("judgments.view", doc_id=doc_id))
    if db.tribunals.find_one({"doc_id": _ids_query(doc_id)}, {"_id": 1}):
        return redirect(url_for("tribunals.view_tribunals", doc_id=doc_id))
    if hasattr(db, "district_court") and db.district_court.find_one({"doc_id": _ids_query(doc_id)}, {"_id": 1}):
        return redirect(url_for("districtcourt.view", doc_id=doc_id))

    # 3) Optional catalog fallback (id-tolerant)
    cat = db.catalog.find_one({"level": "doc", "doc_id": _ids_query(doc_id)}, {"kind": 1})
    if cat:
        kind = (cat.get("kind") or "").lower()
        if kind == "act":
            return redirect(url_for("acts.view_act", doc_id=doc_id))
        if kind == "judgment":
            return redirect(url_for("judgments.view", doc_id=doc_id))
        if kind == "tribunal":
            return redirect(url_for("tribunals.view_tribunals", doc_id=doc_id))
        if kind == "district_court":
            return redirect(url_for("districtcourt.view", doc_id=doc_id))

    abort(404, description="Document id not found")

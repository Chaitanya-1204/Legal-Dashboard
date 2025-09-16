#!/usr/bin/env python3
"""
Step 4 ONLY: Build subsection → Act mappings into `document_links`.

For each Act:
  - parent = act.doc_id
  - children = all subsection IDs found in arrays OR in HTML as href="/doc/<id>/" (or /doc/<id>)
  - upsert: { kind: "act", doc_id: <child>, parent_doc_id: <parent> }

Notes
-----
- Idempotent: safe to re-run; uses upserts + unique index on (kind, doc_id).
- Highlighting is handled in frontend JS; we do NOT store any anchor info here.
- Tuned to your Akoma Ntoso HTML (anchors like <a href="/doc/199911628/">1.</a>).

Env overrides (optional)
------------------------
MONGO_URI        (default: mongodb://localhost:27017/)
MONGO_DB         (default: legal_dashboard_db)
DOC_LINKS_COLL   (default: document_links)
ACTS_COLL        (default: acts)
BATCH_SIZE       (default: 5000)
LINKS_DO_ACTS    (default: 1)  # set to 0 to noop
# Debug filters:
ONLY_ACT_DOC_ID  (no default)  # if set, process only this act doc_id (int)
LIMIT_ACTS       (no default)  # if set, limit number of acts scanned (int)
"""

import os
import re
from typing import Optional, Any, Set, List
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

# ---------- CONFIG ----------
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME          = os.getenv("MONGO_DB", "legal_dashboard_db")
DOC_LINKS_COLL   = os.getenv("DOC_LINKS_COLL", "document_links")
ACTS_COLL        = os.getenv("ACTS_COLL", "acts")
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "5000"))
DO_ACTS          = os.getenv("LINKS_DO_ACTS", "1").lower() in ("1","true","yes")
ONLY_ACT_DOC_ID  = os.getenv("ONLY_ACT_DOC_ID")
LIMIT_ACTS_ENV   = os.getenv("LIMIT_ACTS")
LIMIT_ACTS       = int(LIMIT_ACTS_ENV) if (LIMIT_ACTS_ENV and LIMIT_ACTS_ENV.isdigit()) else None
# ----------------------------

# Regex tuned to your HTML: matches /doc/123/ or /doc/123
DOC_HREF_RE   = re.compile(r'href="/doc/(\d+)/?"')
# Also support data attributes if present anywhere
DATA_ATTR_RE  = re.compile(r'data-doc-id=["\'](\d+)["\']')

# Candidate fields that might contain child IDs directly (arrays)
ARRAY_FIELDS  = [
    "subsections", "subsection_ids", "children", "child_doc_ids",
    "links", "doc_links", "sections"
]

# Candidate fields that might contain HTML/text (strings)
TEXT_FIELDS   = [
    "content_html", "content", "html", "body", "document", "text"
]


def to_int(x: Any) -> Optional[int]:
    try:
        if x is None or isinstance(x, bool):
            return None
        return int(str(x).strip())
    except Exception:
        return None


def connect():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    db.command("ping")
    return client, db


def ensure_doc_links_indexes(db):
    """Make sure document_links has the indexes we rely on."""
    col = db[DOC_LINKS_COLL]
    # Unique per (kind, doc_id) — we only write kind="act" rows here
    col.create_index([("kind", 1), ("doc_id", 1)], name="kind_doc_unique", unique=True)
    # Helpful for reverse lookups later
    col.create_index([("parent_doc_id", 1), ("kind", 1)], name="parent_lookup")


def bulk_upsert(col, ops: List[UpdateOne]) -> int:
    if not ops:
        return 0
    try:
        res = col.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0) + (res.matched_count or 0)
    except BulkWriteError as e:
        errs = (e.details or {}).get("writeErrors", [])
        print("⚠️  BulkWriteError (first 3):", errs[:3])
        return 0


def extract_child_ids(act_doc: dict) -> Set[int]:
    """
    Find subsection IDs inside an Act doc by scanning:
    1) Any array fields that might already contain IDs
    2) HTML/text for <a href="/doc/<id>"> (with or without trailing slash)
    3) data-doc-id="<id>" attributes if used anywhere
    """
    child_ids: Set[int] = set()

    # 1) Arrays of IDs, if present
    for f in ARRAY_FIELDS:
        vals = act_doc.get(f)
        if isinstance(vals, (list, tuple)):
            for v in vals:
                vi = to_int(v)
                if vi is not None:
                    child_ids.add(vi)

    # 2) First available HTML/text field
    html_text = None
    for f in TEXT_FIELDS:
        s = act_doc.get(f)
        if isinstance(s, str) and s:
            html_text = s
            break

    if html_text:
        # Matches /doc/123 or /doc/123/
        for m in DOC_HREF_RE.findall(html_text):
            vi = to_int(m)
            if vi is not None:
                child_ids.add(vi)

        # Optional: data-doc-id="123"
        for m in DATA_ATTR_RE.findall(html_text):
            vi = to_int(m)
            if vi is not None:
                child_ids.add(vi)

    return child_ids


def build_act_links(db) -> dict:
    """Map each detected subsection -> parent main act."""
    acts  = db[ACTS_COLL]
    links = db[DOC_LINKS_COLL]

    # Projection: only fields we actually scan
    proj_fields = {"_id": 0, "doc_id": 1}
    for f in ARRAY_FIELDS + TEXT_FIELDS:
        proj_fields[f] = 1

    # Optional debug filter: process only one Act by doc_id
    query = {}
    only = to_int(ONLY_ACT_DOC_ID)
    if only is not None:
        query = {"doc_id": only}

    total_acts = 0
    total_pairs = 0
    batch: List[UpdateOne] = []

    print("📚 Acts: mapping subsections → parent Act …")
    cur = acts.find(query, proj_fields).batch_size(BATCH_SIZE)

    for doc in cur:
        total_acts += 1
        parent = to_int(doc.get("doc_id"))
        if parent is None:
            continue

        children = extract_child_ids(doc)

        # Avoid self-mapping if the main doc_id appears inside its own content
        if parent in children:
            children.discard(parent)

        if not children:
            # nothing to map for this act
            if LIMIT_ACTS and total_acts >= LIMIT_ACTS:
                break
            continue

        for child in children:
            filt = {"kind": "act", "doc_id": child}
            setv = {"$set": {"parent_doc_id": parent}}
            batch.append(UpdateOne(filt, setv, upsert=True))
            if len(batch) >= BATCH_SIZE:
                total_pairs += bulk_upsert(links, batch); batch = []

        if LIMIT_ACTS and total_acts >= LIMIT_ACTS:
            break

    total_pairs += bulk_upsert(links, batch)
    print(f"✅ Acts: upserted {total_pairs} subsection→parent links from {total_acts} Act(s)")
    return {"acts_scanned": total_acts, "subsection_links_upserted": total_pairs}


def main():
    if not DO_ACTS:
        print("LINKS_DO_ACTS is disabled. Nothing to do.")
        return

    client, db = connect()
    try:
        ensure_doc_links_indexes(db)
        summary = build_act_links(db)
        total = db[DOC_LINKS_COLL].count_documents({"kind": "act"})
        print("\n📊 Summary:", summary)
        print({"document_links_act_rows": total})
        print("🎉 Step 4 complete.")
    finally:
        client.close()


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Step 5 ONLY: Add self links for Judgments & Tribunals into `document_links`.

For each document in:
  - `judgments`  → upsert { kind: "judgment", doc_id: X, parent_doc_id: X }
  - `tribunals`  → upsert { kind: "tribunal", doc_id: X, parent_doc_id: X }

Notes
-----
- Uses upserts + unique index on (kind, doc_id) → safe to re-run.
- Does not modify `catalog/catalogue` or the source collections.
- Casts doc_id to int; change `to_int` if your ids are strings.
- Highlighting remains on the frontend JS (not stored here).

Env overrides (optional)
------------------------
MONGO_URI           default: mongodb://localhost:27017/
MONGO_DB            default: legal_dashboard_db
DOC_LINKS_COLL      default: document_links
JUDGMENTS_COLL      default: judgments
TRIBUNALS_COLL      default: tribunals
BATCH_SIZE          default: 5000
LINKS_DO_JUDGMENTS  default: 1   (set 0 to skip)
LINKS_DO_TRIBUNALS  default: 1   (set 0 to skip)
LIMIT_DOCS          default: none (set to an int to process only N docs for a quick test)
"""

import os
from typing import Optional, Any, List
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

# ---------- CONFIG ----------
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME          = os.getenv("MONGO_DB", "legal_dashboard_db")

DOC_LINKS_COLL   = os.getenv("DOC_LINKS_COLL", "document_links")
JUDG_COLL        = os.getenv("JUDGMENTS_COLL", "judgments")
TRIB_COLL        = os.getenv("TRIBUNALS_COLL", "tribunals")

BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "5000"))
DO_JUDGMENTS     = os.getenv("LINKS_DO_JUDGMENTS", "1").lower() in ("1","true","yes")
DO_TRIBUNALS     = os.getenv("LINKS_DO_TRIBUNALS", "1").lower() in ("1","true","yes")

LIMIT_DOCS_ENV   = os.getenv("LIMIT_DOCS")
LIMIT_DOCS       = int(LIMIT_DOCS_ENV) if (LIMIT_DOCS_ENV and LIMIT_DOCS_ENV.isdigit()) else None
# ----------------------------

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
    col = db[DOC_LINKS_COLL]
    col.create_index([("kind", 1), ("doc_id", 1)], name="kind_doc_unique", unique=True)
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

def build_self_links(db, coll_name: str, kind: str) -> dict:
    src  = db[coll_name]
    dest = db[DOC_LINKS_COLL]

    total_docs = 0
    upserts = 0
    batch: List[UpdateOne] = []

    print(f"🧾 {kind}: creating self links from `{coll_name}` …")
    cur = src.find({}, {"_id": 0, "doc_id": 1}).batch_size(BATCH_SIZE)

    for doc in cur:
        total_docs += 1
        did = to_int(doc.get("doc_id"))
        if did is None:
            continue
        batch.append(UpdateOne(
            {"kind": kind, "doc_id": did},
            {"$set": {"parent_doc_id": did}},
            upsert=True
        ))
        if len(batch) >= BATCH_SIZE:
            upserts += bulk_upsert(dest, batch); batch = []
        if LIMIT_DOCS and total_docs >= LIMIT_DOCS:
            break

    upserts += bulk_upsert(dest, batch)
    print(f"✅ {kind}: {upserts} upserts (from {total_docs} docs)")
    return {"docs_scanned": total_docs, "self_links_upserted": upserts}

def main():
    client, db = connect()
    try:
        ensure_doc_links_indexes(db)

        summary = {}
        if DO_JUDGMENTS:
            summary["judgments"] = build_self_links(db, JUDG_COLL, "judgment")
        if DO_TRIBUNALS:
            summary["tribunals"] = build_self_links(db, TRIB_COLL, "tribunal")

        total_rows = db[DOC_LINKS_COLL].count_documents({"kind": {"$in": ["judgment", "tribunal"]}})
        print("\n📊 Summary:", summary)
        print({"document_links_self_rows": total_rows})
        print("🎉 Step 5 complete.")
    finally:
        client.close()

if __name__ == "__main__":
    main()

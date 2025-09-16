# scripts/build_section_index.py
"""
Build/refresh 'section_index' from the 'acts' collection by copying unique doc_id values.

Env vars:
  MONGO_URI       (default: mongodb://localhost:27017)
  MONGO_DB        (default: legal_dashboard_db)
  ACTS_COLLECTION (default: acts)

Usage:
  MONGO_DB=legal_dashboard_db ACTS_COLLECTION=acts python scripts/build_section_index.py
"""
import os
from pymongo import MongoClient, InsertOne

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "legal_dashboard_db")
ACTS_COLLECTION = os.getenv("ACTS_COLLECTION", "acts")

def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # pick 'acts' defensively (fallback to 'act' if needed)
    names = set(db.list_collection_names())
    acts_col_name = ACTS_COLLECTION if ACTS_COLLECTION in names else ("act" if "act" in names else "acts")
    acts = db[acts_col_name]

    print(f"[i] Using DB '{MONGO_DB}', collection '{acts_col_name}'")

    # 1) get all unique, non-null doc_ids from acts
    ids = list(acts.distinct("doc_id", {"doc_id": {"$exists": True, "$ne": None}}))
    print(f"[i] Found {len(ids)} unique doc_id from '{acts_col_name}'")

    # 2) rebuild section_index
    db.section_index.drop()
    if ids:
        db.section_index.bulk_write([InsertOne({"doc_id": _id}) for _id in ids])
    db.section_index.create_index("doc_id", unique=True)

    # 3) sanity print
    sample = list(db.section_index.find({}, {"_id": 0, "doc_id": 1}).limit(3))
    print(f"[✓] section_index ready with {db.section_index.estimated_document_count()} rows; sample: {sample}")

if __name__ == "__main__":
    main()

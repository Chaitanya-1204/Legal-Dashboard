#!/usr/bin/env python3
"""
B- ackfill 'catalog' from existing collections:
  - acts       (category, year, doc)
  tribunals  (category_name, year, doc)
  - judgments  (year, doc)  [Supreme Court]

No HTML is copied. Only tiny pointer rows are added to 'catalog'.

Document shapes inserted:

Acts
  {kind:"act", level:"category", act_category}
  {kind:"act", level:"year",     act_category, year}
  {kind:"act", level:"doc",      act_category, year, doc_id, full_title, coll:"acts"}

Tribunals
  {kind:"tribunal", level:"category", category_name}
  {kind:"tribunal", level:"year",     category_name, year}
  {kind:"tribunal", level:"doc",      category_name, year, doc_id, full_title, coll:"tribunals"}

Judgments (SC)
  {kind:"judgment", level:"year", year}
  {kind:"judgment", level:"doc",  year, doc_id, full_title, coll:"judgments"}

Notes:
- Uses batched bulk upserts for speed.
- Optional DROP/CLEAR flags for 'catalog' only.
"""

import os
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

# ---------- CONFIG ----------
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME     = os.getenv("MONGO_DB", "legal_dashboard_db")
CATALOG_COLL= os.getenv("CATALOG_COLL", "catalog")

DROP_CATALOG  = os.getenv("CAT_DROP",  "0").lower() in ("1","true","yes")
CLEAR_CATALOG = os.getenv("CAT_CLEAR", "0").lower() in ("1","true","yes")

# Toggle per-kind population if you want
DO_ACTS       = os.getenv("CAT_DO_ACTS",       "1").lower() in ("1","true","yes")
DO_TRIBUNALS  = os.getenv("CAT_DO_TRIBUNALS",  "1").lower() in ("1","true","yes")
DO_JUDGMENTS  = os.getenv("CAT_DO_JUDGMENTS",  "1").lower() in ("1","true","yes")

# Creating per-doc rows can be large; keep batches modest
BATCH_SIZE = int(os.getenv("CAT_BATCH_SIZE", "5000"))
# ---------------------------


def connect():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    client.admin.command("ping")
    return client, db


def drop_or_clear(db):
    col = db[CATALOG_COLL]
    if DROP_CATALOG:
        print(f"🗑️  Dropping {DB_NAME}.{CATALOG_COLL} …")
        col.drop()
    elif CLEAR_CATALOG:
        print(f"🧹 Clearing {DB_NAME}.{CATALOG_COLL} …")
        res = col.delete_many({})
        print(f"   removed {res.deleted_count} docs.")
    else:
        print("🚫 Drop/Clear: skipped")


def ensure_indexes(db):
    c = db[CATALOG_COLL]
    print("📚 Ensuring catalog indexes …")
    c.create_index([("kind", 1), ("level", 1)], name="k_level")
    c.create_index(
        [("level", 1), ("doc_id", 1)],
        name="level_doc_lookup",
        partialFilterExpression={"level": "doc"}
    )
    # Acts
    c.create_index([("kind",1), ("level",1), ("act_category",1)], name="act_cat")
    c.create_index([("kind",1), ("level",1), ("act_category",1), ("year",1)], name="act_cat_year")
    c.create_index([("kind",1), ("level",1), ("act_category",1), ("year",1), ("doc_id",1)], name="act_cat_year_doc")

    # Tribunals
    c.create_index([("kind",1), ("level",1), ("category_name",1)], name="trib_cat")
    c.create_index([("kind",1), ("level",1), ("category_name",1), ("year",1)], name="trib_cat_year")
    c.create_index([("kind",1), ("level",1), ("category_name",1), ("year",1), ("doc_id",1)], name="trib_cat_year_doc")

    # Judgments (SC)
    c.create_index([("kind",1), ("level",1), ("year",1)], name="judg_year")
    c.create_index([("kind",1), ("level",1), ("year",1), ("doc_id",1)], name="judg_year_doc")


def bulk_upsert(col, ops):
    if not ops:
        return 0
    try:
        res = col.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0) + (res.matched_count or 0)
    except BulkWriteError as e:
        # Continue but report
        print("⚠️  BulkWriteError:", e.details.get("writeErrors", [])[:3], "…")
        return 0


def populate_acts(db):
    catalog = db[CATALOG_COLL]
    acts = db["acts"]

    # 1) category rows
    print("🧭 Acts: categories …")
    ops = []
    for cat in acts.distinct("category"):
        if not cat:
            continue
        filt = {"kind":"act","level":"category","act_category":cat}
        ops.append(UpdateOne(filt, {"$set": filt}, upsert=True))
        if len(ops) >= BATCH_SIZE:
            bulk_upsert(catalog, ops); ops = []
    bulk_upsert(catalog, ops); ops = []

    # 2) (category, year) rows
    print("🧭 Acts: (category, year) …")
    pipeline = [
        {"$group": {"_id": {"category":"$category", "year":"$year"}}},
        {"$project": {"_id":0, "act_category":"$_id.category", "year":"$_id.year"}},
    ]
    for row in acts.aggregate(pipeline, allowDiskUse=True):
        cat = row.get("act_category"); yr = row.get("year")
        if cat is None or yr is None:
            continue
        filt = {"kind":"act","level":"year","act_category":cat,"year":int(yr)}
        ops.append(UpdateOne(filt, {"$set": filt}, upsert=True))
        if len(ops) >= BATCH_SIZE:
            bulk_upsert(catalog, ops); ops = []
    bulk_upsert(catalog, ops); ops = []

    # 3) doc rows (list page)
    print("🧭 Acts: doc rows …")
    cur = acts.find(
        {},
        {"_id":0, "doc_id":1, "full_title":1, "category":1, "year":1}
    ).batch_size(BATCH_SIZE)
    for doc in cur:
        did = doc.get("doc_id"); cat = doc.get("category"); yr = doc.get("year")
        title = doc.get("full_title")
        if did is None or cat is None or yr is None:
            continue
        filt = {"kind":"act","level":"doc","act_category":cat,"year":int(yr),"doc_id":int(did)}
        setv = {"$set":{"full_title":title, "coll":"acts"}}
        ops.append(UpdateOne(filt, setv, upsert=True))
        if len(ops) >= BATCH_SIZE:
            bulk_upsert(catalog, ops); ops = []
    bulk_upsert(catalog, ops)


def populate_tribunals(db):
    catalog = db[CATALOG_COLL]
    trib = db["tribunals"]

    # 1) categories
    print("🏛️ Tribunals: categories …")
    ops = []
    for cat in trib.distinct("category_name"):
        if not cat:
            continue
        filt = {"kind":"tribunal","level":"category","category_name":cat}
        ops.append(UpdateOne(filt, {"$set": filt}, upsert=True))
        if len(ops) >= BATCH_SIZE:
            bulk_upsert(catalog, ops); ops = []
    bulk_upsert(catalog, ops); ops = []

    # 2) (category, year)
    print("🏛️ Tribunals: (category, year) …")
    pipeline = [
        {"$group": {"_id": {"category_name":"$category_name", "year":"$year"}}},
        {"$project": {"_id":0, "category_name":"$_id.category_name", "year":"$_id.year"}},
    ]
    for row in trib.aggregate(pipeline, allowDiskUse=True):
        cat = row.get("category_name"); yr = row.get("year")
        if cat is None or yr is None:
            continue
        filt = {"kind":"tribunal","level":"year","category_name":cat,"year":int(yr)}
        ops.append(UpdateOne(filt, {"$set": filt}, upsert=True))
        if len(ops) >= BATCH_SIZE:
            bulk_upsert(catalog, ops); ops = []
    bulk_upsert(catalog, ops); ops = []

    # 3) doc rows
    print("🏛️ Tribunals: doc rows …")
    cur = trib.find(
        {},
        {"_id":0, "doc_id":1, "full_title":1, "category_name":1, "year":1}
    ).batch_size(BATCH_SIZE)
    for doc in cur:
        did = doc.get("doc_id"); cat = doc.get("category_name"); yr = doc.get("year")
        title = doc.get("full_title")
        if did is None or cat is None or yr is None:
            continue
        filt = {"kind":"tribunal","level":"doc","category_name":cat,"year":int(yr),"doc_id":int(did)}
        setv = {"$set":{"full_title":title, "coll":"tribunals"}}
        ops.append(UpdateOne(filt, setv, upsert=True))
        if len(ops) >= BATCH_SIZE:
            bulk_upsert(catalog, ops); ops = []
    bulk_upsert(catalog, ops)


def populate_judgments(db):
    catalog = db[CATALOG_COLL]
    judg = db["judgments"]

    # 1) year rows
    print("⚖️  Judgments: years …")
    ops = []
    for yr in judg.distinct("year"):
        if yr is None:
            continue
        filt = {"kind":"judgment","level":"year","year":int(yr)}
        ops.append(UpdateOne(filt, {"$set": filt}, upsert=True))
        if len(ops) >= BATCH_SIZE:
            bulk_upsert(catalog, ops); ops = []
    bulk_upsert(catalog, ops); ops = []

    # 2) doc rows
    print("⚖️  Judgments: doc rows …")
    cur = judg.find(
        {},
        {"_id":0, "doc_id":1, "full_title":1, "year":1}
    ).batch_size(BATCH_SIZE)
    for doc in cur:
        did = doc.get("doc_id"); yr = doc.get("year"); title = doc.get("full_title")
        if did is None or yr is None:
            continue
        filt = {"kind":"judgment","level":"doc","year":int(yr),"doc_id":int(did)}
        setv = {"$set":{"full_title":title, "coll":"judgments"}}
        ops.append(UpdateOne(filt, setv, upsert=True))
        if len(ops) >= BATCH_SIZE:
            bulk_upsert(catalog, ops); ops = []
    bulk_upsert(catalog, ops)


def main():
    client, db = connect()
    try:
        drop_or_clear(db)
        ensure_indexes(db)

        if DO_ACTS:
            populate_acts(db)
        if DO_TRIBUNALS:
            populate_tribunals(db)
        if DO_JUDGMENTS:
            populate_judgments(db)

        total = db[CATALOG_COLL].count_documents({})
        print({"catalog_total_docs": total})
        print("✅ Done.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

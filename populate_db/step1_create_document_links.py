#!/usr/bin/env python3
import os
from pymongo import MongoClient

MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME        = os.getenv("MONGO_DB", "legal_dashboard_db")
DOC_LINKS_COLL = os.getenv("DOC_LINKS_COLL", "document_links")

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    db.command("ping")

    # create collection if missing
    if DOC_LINKS_COLL not in db.list_collection_names():
        db.create_collection(DOC_LINKS_COLL)

    col = db[DOC_LINKS_COLL]

    # add the ONE important unique index
    col.create_index([("kind", 1), ("doc_id", 1)], name="kind_doc_unique", unique=True)

    # show indexes
    print("indexes:", [ix["name"] for ix in col.list_indexes()])
    client.close()
    print("✅ step 1 done: document_links created + unique index added.")

if __name__ == "__main__":
    main()

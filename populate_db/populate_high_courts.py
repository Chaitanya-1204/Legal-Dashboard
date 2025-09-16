#!/usr/bin/env python3
"""
Ingest High Court HTML → MongoDB

- Reads all *.html / *.htm from: /DATACHAI/Data/sample_high_court_html/processed_html
- Filename pattern example: HPHC010000012001_1_2011-06-22.html
  - doc_id      = "HPHC010000012001_1_2011-06-22" (string)
  - full_title  = same as filename stem
  - title       = same as filename stem
  - category    = "high_court"
  - law_type    = "judgment"
  - year        = 2011 (parsed if YYYY-MM-DD present)
  - word_count  = computed from stripped HTML
  - content     = raw HTML

Mongo:
- DB  : legal_dashboard_db
- Coll: high_courts
"""

import re
from pathlib import Path
from typing import Optional, Tuple

from pymongo import MongoClient, ASCENDING

# ===== CONFIG =====
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME   = "legal_dashboard_db"
COLL_NAME = "high_courts"

HTML_DIR  = "/DATACHAI/Data/sample_high_court_html/processed_html"
FALLBACK_ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
# ==================


# ---------- helpers ----------
def read_text_file(p: Path) -> Tuple[Optional[str], Optional[str]]:
    for enc in FALLBACK_ENCODINGS:
        try:
            return p.read_text(encoding=enc, errors="replace"), enc
        except UnicodeDecodeError:
            continue
        except Exception:
            break
    return None, None

def html_to_text(html: str) -> str:
    # strip script/style, tags, and compress whitespace
    html = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def word_count_from_html(html: str) -> int:
    return len(re.findall(r"\b\w+\b", html_to_text(html)))

def parse_year_from_stem(stem: str) -> Optional[int]:
    """
    Try to find a YYYY-MM-DD at end of the stem and return YYYY as int.
    Example: HPHC010000012001_1_2011-06-22 -> 2011
    """
    m = re.search(r"(\d{4})-\d{2}-\d{2}$", stem)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None
# ------------------------------


def ensure_indexes(col):
    # Unique doc_id since we upsert by it (string key)
    col.create_index([("doc_id", ASCENDING)], unique=True, name="doc_id_unique")
    # Useful filter
    col.create_index([("year", ASCENDING)], name="year_idx")
    col.create_index([("category", ASCENDING)], name="category_idx")
    col.create_index([("law_type", ASCENDING)], name="law_type_idx")


def connect_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    db.command("ping")
    return client, db[COLL_NAME]


def ingest_high_courts():
    root = Path(HTML_DIR)
    if not root.exists():
        print(f"❌ Path not found: {root}")
        return

    client, col = connect_collection()
    try:
        ensure_indexes(col)

        matched = 0
        skipped = 0

        files = list(root.glob("*.html")) + list(root.glob("*.htm"))
        if not files:
            print(f"⚠️  No .html/.htm files under: {root}")
            return

        for path in files:
            stem = path.stem  # e.g. "HPHC010000012001_1_2011-06-22"
            html, enc = read_text_file(path)
            if not html:
                skipped += 1
                continue

            wc = word_count_from_html(html)
            yr = parse_year_from_stem(stem)

            document_to_insert = {
                "doc_id": stem,                # string doc_id (filename stem)
                "full_title": stem,            # as requested
                "title": stem,                 # same as full_title for now
                "category": "high_court",      # as requested
                "law_type": "judgment",        # as requested
                "year": yr,                    # None if not parsed
                "word_count": wc,
                "content": html
            }

            col.update_one(
                {"doc_id": stem},
                {"$set": document_to_insert},
                upsert=True
            )
            matched += 1

        total = col.estimated_document_count()
        print(f"✅ High Court ingest: upserted {matched}, skipped {skipped}.")
        print({"total_docs_in_high_courts": total})

    finally:
        client.close()
        print("🔌 MongoDB connection closed.")


if __name__ == "__main__":
    ingest_high_courts()

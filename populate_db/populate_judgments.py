#!/usr/bin/env python3
"""
Supreme Court Judgments -> MongoDB (same schema/keys as `tribunals`)

CSV:
  /DATACHAI/Data/Judments/Supreme_Court/supreme_court_logs_enriched.csv
HTML root (year-wise folders, files named <doc_id>.html|.htm):
  /DATACHAI/Data/Judments/Supreme_Court/<YEAR>/<doc_id>.html

MongoDB (legal_dashboard_db.judgments) document:
{
  doc_id: int,
  full_title: str,            # from CSV
  category: "judgments",      # constant
  category_name: "Supreme_Court",
  year: int,
  law_type: "judgment",
  word_count: int,
  content: str                # full HTML
}
"""

import os
import re
import csv
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from html import unescape

from pymongo import MongoClient, ASCENDING

# ===== CONFIG =====
CSV_PATH   = "/DATACHAI/Data/Judments/Supreme_Court/supreme_court_logs_enriched.csv"
DOC_ROOT   = "/DATACHAI/Data/Judments/Supreme_Court"

MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "legal_dashboard_db"
COLL_NAME  = "judgments"

DROP_COLLECTION  = True    # drop collection & indexes, rebuild from scratch
CLEAR_COLLECTION = False   # alternative: clear docs, keep indexes

MAX_HTML_BYTES = 15_000_000
FALLBACK_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
CATEGORY_NAME = "Supreme_Court"
# ==================


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
    html = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    txt = unescape(html)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt



def word_count_from_html(html: str) -> int:
    return len(re.findall(r"\b\w+\b", html_to_text(html)))

def load_csv_rows(csv_path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for enc in FALLBACK_ENCODINGS:
        try:
            with open(csv_path, newline="", encoding=enc, errors="replace") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows.append({
                        "year":   (r.get("year")   or r.get("Year")   or "").strip(),
                        "doc_id": (r.get("doc_id") or r.get("Doc_id") or r.get("id") or "").strip(),
                        "title":  (r.get("title")  or r.get("Title")  or r.get("full_title") or "").strip(),
                    })
            break
        except UnicodeDecodeError:
            continue
    return [r for r in rows if r["year"] and r["doc_id"] and r["title"]]


def connect_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLL_NAME]
    client.admin.command("ping")
    return client, col


def drop_or_clear(col):
    if DROP_COLLECTION:
        print(f"🗑️  Dropping {DB_NAME}.{COLL_NAME} …")
        col.drop()
    elif CLEAR_COLLECTION:
        print(f"🧹 Clearing {DB_NAME}.{COLL_NAME} …")
        res = col.delete_many({})
        print(f"   removed {res.deleted_count} docs.")
    else:
        print("🚫 Drop/Clear: skipped")


def ensure_indexes(col):
    print("📚 Ensuring indexes …")
    col.create_index([("doc_id", ASCENDING)], unique=True, name="doc_id_unique")
    col.create_index([("year", ASCENDING)], name="year_idx")


def ingest():
    csv_path = Path(CSV_PATH).resolve()
    if not csv_path.exists():
        print(f"❌ CSV not found: {csv_path}")
        return
    root = Path(DOC_ROOT).resolve()
    if not root.exists():
        print(f"❌ DOC_ROOT not found: {root}")
        return

    client, col = connect_collection()
    try:
        drop_or_clear(col)
        ensure_indexes(col)

        rows = load_csv_rows(csv_path)
        if not rows:
            print("⚠️  CSV empty or missing required headers (year, doc_id, title).")
            return

        matched = unmatched = 0

        for row in rows:
            try:
                year_int = int(row["year"])
                doc_id_int = int(row["doc_id"])
            except Exception:
                unmatched += 1
                continue

            year_dir = root / str(year_int)
            if not (year_dir.exists() and year_dir.is_dir()):
                unmatched += 1
                continue

            # Look for <doc_id>.html or <doc_id>.htm
            p_html = year_dir / f"{doc_id_int}.html"
            p_htm  = year_dir / f"{doc_id_int}.htm"
            p = p_html if p_html.exists() else (p_htm if p_htm.exists() else None)
            if not p:
                unmatched += 1
                continue

            html, enc = read_text_file(p)
            if not html:
                unmatched += 1
                continue

            html_bytes = len(html.encode("utf-8", errors="replace"))
            if html_bytes > MAX_HTML_BYTES:
                print(f"   🚫 too big, skipping: {p} ({html_bytes} bytes)")
                unmatched += 1
                continue

            document_to_insert = {
                "doc_id": doc_id_int,
                "full_title": row["title"],
                "category": "judgments",
                "category_name": CATEGORY_NAME,
                "year": year_int,
                "law_type": "judgment",
                "word_count": word_count_from_html(html),
                "content": html
            }

            col.update_one(
                {"doc_id": document_to_insert["doc_id"]},
                {"$set": document_to_insert},
                upsert=True
            )
            matched += 1

        print(f"✅ {CATEGORY_NAME}: matched={matched}, unmatched={unmatched}")
        total = col.count_documents({})
        print({"total_docs_in_collection": total})
    finally:
        client.close()
        print("✅ MongoDB connection closed.")


if __name__ == "__main__":
    ingest()

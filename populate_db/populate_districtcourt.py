#!/usr/bin/env python3
"""
District/High Courts -> MongoDB (title == filename)

Folder layout:
  /DATACHAI/Data/Judments/District_court/
    ├── <COURT_NAME>/
    │     ├── <YEAR>/**/*.html|.htm
    │     └── *.csv  (columns: year,doc_id,title)
    └── ...

MongoDB (legal_dashboard_db.district_court) document:
{
  doc_id: int,
  full_title: str,                  # from CSV
  category: "district_court",       # constant
  category_name: "<COURT_NAME>",    # folder name (e.g., "delhi high court")
  year: int,
  law_type: "judgments",            # << as requested
  word_count: int,
  content: str                      # full HTML
}
"""

import os
import re
import csv
from pathlib import Path
from typing import Optional, Tuple, Dict, List
from html import unescape

from pymongo import MongoClient, ASCENDING, DESCENDING

# ===== CONFIG =====
DOC_ROOT   = "/DATACHAI/Data/Judments/District_court"
MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "legal_dashboard_db"
COLL_NAME  = "district_court"        # collection name

DROP_COLLECTION  = False             # True to drop collection before ingest
CLEAR_COLLECTION = False             # True to delete all docs before ingest

MAX_HTML_BYTES = 15_000_000
FALLBACK_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
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


def norm_key(s: str) -> str:
    """Normalize so CSV title and filename stem compare equal."""
    s = (s or "").strip().lower()
    s = unescape(s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[\u2018\u2019\u201C\u201D]", "'", s)   # curly → straight
    s = re.sub(r"[^a-z0-9 ]+", " ", s)                 # keep alnum + spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s


def html_to_text(html: str) -> str:
    """Minimal HTML→text for word count."""
    html = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    txt = unescape(html)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def word_count_from_html(html: str) -> int:
    return len(re.findall(r"\b\w+\b", html_to_text(html)))


def pick_csv_in(court_dir: Path) -> Optional[Path]:
    """Pick a CSV in the court folder."""
    cands = list(court_dir.glob("*.csv"))
    if not cands:
        return None
    pri = [p for p in cands if "download" in p.name.lower() or "log" in p.name.lower()]
    return (pri or cands)[0]


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
                        "title":  (r.get("title")  or r.get("Title")  or "").strip(),
                    })
            break
        except UnicodeDecodeError:
            continue
    return [r for r in rows if r["year"] and r["doc_id"] and r["title"]]


def build_year_index(year_dir: Path) -> Dict[str, Path]:
    """
    Map normalized filename stem -> Path for all .html/.htm in the year folder.
    Prefer .html over .htm if both exist.
    """
    idx: Dict[str, Path] = {}
    for p in year_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in (".html", ".htm"):
            continue
        key = norm_key(p.stem)
        if key not in idx or (p.suffix.lower() == ".html" and idx[key].suffix.lower() == ".htm"):
            idx[key] = p
    return idx


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
    # Unique doc per (court folder, doc_id)
    col.create_index([("category_name", ASCENDING), ("doc_id", ASCENDING)],
                     unique=True, name="court_doc_unique")
    # Year filter index
    col.create_index([("year", ASCENDING)], name="year_idx")
    # Optional: quick filter by court+year
    col.create_index([("category_name", ASCENDING), ("year", DESCENDING)],
                     name="court_year_idx")


def ingest():
    root = Path(DOC_ROOT).resolve()
    if not root.exists():
        print(f"❌ DOC_ROOT not found: {root}")
        return

    client, col = connect_collection()
    try:
        drop_or_clear(col)
        ensure_indexes(col)

        courts = sorted([d for d in root.iterdir() if d.is_dir()])
        for court_dir in courts:
            category_name = court_dir.name  # e.g., "delhi high court"
            csv_path = pick_csv_in(court_dir)
            if not csv_path:
                print(f"⚠️  {category_name}: no CSV found, skipping.")
                continue

            rows = load_csv_rows(csv_path)
            if not rows:
                print(f"⚠️  {category_name}: CSV empty or missing required headers, skipping.")
                continue

            # group rows by year
            by_year: Dict[str, List[Dict[str, str]]] = {}
            for r in rows:
                by_year.setdefault(r["year"], []).append(r)

            matched = unmatched = 0

            for year_str, year_rows in sorted(by_year.items()):
                year_dir = court_dir / year_str
                if not year_dir.exists():
                    print(f"  ⚠️ {category_name}/{year_str}: folder missing (rows={len(year_rows)})")
                    unmatched += len(year_rows)
                    continue

                idx = build_year_index(year_dir)

                for row in year_rows:
                    key = norm_key(row["title"])
                    p = idx.get(key)
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

                    try:
                        doc_id_int = int(row["doc_id"])
                        year_int = int(year_str)
                    except Exception:
                        unmatched += 1
                        continue

                    # ---- store in requested shape ----
                    document_to_insert = {
                        "doc_id": doc_id_int,
                        "full_title": row["title"],
                        "category": "district_court",      # constant
                        "category_name": category_name,     # court folder name
                        "year": year_int,
                        "law_type": "judgments",            # << updated here
                        "word_count": word_count_from_html(html),
                        "content": html,
                    }

                    # upsert on (category_name, doc_id)
                    col.update_one(
                        {"category_name": category_name, "doc_id": doc_id_int},
                        {"$set": document_to_insert},
                        upsert=True
                    )
                    matched += 1

            print(f"✅ {category_name}: matched={matched}, unmatched={unmatched}")

        total = col.count_documents({})
        print({"total_docs_in_collection": total})
    finally:
        client.close()
        print("✅ MongoDB connection closed.")


if __name__ == "__main__":
    DOC_ROOT  = os.getenv("TRIB_DOC_ROOT", DOC_ROOT)
    MONGO_URI = os.getenv("MONGO_URI", MONGO_URI)
    DB_NAME   = os.getenv("MONGO_DB", DB_NAME)
    COLL_NAME = os.getenv("TRIB_COLL_NAME", COLL_NAME)  # env override if needed
    ingest()

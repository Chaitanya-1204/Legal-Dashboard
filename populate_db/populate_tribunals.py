#!/usr/bin/env python3
"""
Simple incremental ingester for Tribunals -> MongoDB

Matching logic (per CSV row) — EITHER ONE:
1) Try by doc_id: match a file whose FILENAME STEM is exactly the doc_id (digits only).
2) Else by title: match a file whose normalized FILENAME STEM equals normalized CSV title.

Everything happens inside the CSV row's YEAR folder only.

Default is INSERT-ONLY (won't touch existing doc_ids). Use --update-existing to allow updates.
"""

import os
import re
import csv
import argparse
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List, Set
from html import unescape

from pymongo import MongoClient, ASCENDING

# ===== CONFIG (can be overridden by env or CLI) =====
DOC_ROOT   = "/DATACHAI/Data/Judments/Tribunals"
MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "legal_dashboard_db"
COLL_NAME  = "tribunals"

MAX_HTML_BYTES = 15_000_000
FALLBACK_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
# ====================================================


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
    s = (s or "").strip().lower()
    s = unescape(s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[\u2018\u2019\u201C\u201D]", "'", s)  # curly → straight quotes
    s = re.sub(r"[^a-z0-9 ]+", " ", s)                 # keep alnum + spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s


def html_to_text(html: str) -> str:
    html = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    txt = unescape(html)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def word_count_from_html(html: str) -> int:
    return len(re.findall(r"\b\w+\b", html_to_text(html)))


def pick_csv_in(tribunal_dir: Path) -> Optional[Path]:
    cands = list(tribunal_dir.glob("*.csv"))
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


def _prefers_html(new: Path, cur: Path) -> bool:
    return new.suffix.lower() == ".html" and cur.suffix.lower() == ".htm"


def build_year_index(year_dir: Path) -> Dict[str, Dict[str, Path]]:
    """
    Build two indexes for a year folder:
    - by_id: filename stem is exactly digits (doc_id) -> Path
    - by_title: normalized filename stem -> Path
    Prefer .html over .htm when both exist.
    """
    by_id: Dict[str, Path] = {}
    by_title: Dict[str, Path] = {}

    for p in year_dir.rglob("*"):
        if not p.is_file() or p.suffix.lower() not in (".html", ".htm"):
            continue

        stem = p.stem

        # exact doc_id filenames (digits only)
        if re.fullmatch(r"\d+", stem or ""):
            if stem not in by_id or _prefers_html(p, by_id[stem]):
                by_id[stem] = p

        # title mapping
        tkey = norm_key(stem)
        if tkey not in by_title or _prefers_html(p, by_title[tkey]):
            by_title[tkey] = p

    return {"by_id": by_id, "by_title": by_title}


def connect_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLL_NAME]
    client.admin.command("ping")
    return client, col


def ensure_indexes(col):
    col.create_index([("doc_id", ASCENDING)], unique=True, name="doc_id_unique")
    col.create_index([("year", ASCENDING)], name="year_idx")
    col.create_index([("category_name", ASCENDING), ("year", ASCENDING)], name="tribunal_year_idx")


def parse_args():
    ap = argparse.ArgumentParser(description="Simple incremental Tribunals -> MongoDB (doc_id OR title match)")
    ap.add_argument("--doc-root", default=os.getenv("TRIB_DOC_ROOT", DOC_ROOT))
    ap.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", MONGO_URI))
    ap.add_argument("--db-name", default=os.getenv("MONGO_DB", DB_NAME))
    ap.add_argument("--coll-name", default=os.getenv("TRIB_COLL_NAME", COLL_NAME))

    ap.add_argument("--only-trib", default=os.getenv("TRIB_ONLY", ""), help="Comma-separated tribunal folder names to ingest")
    ap.add_argument("--only-year", default=os.getenv("YEAR_ONLY", ""), help="Comma-separated years to ingest (e.g., 2021,2022)")
    ap.add_argument("--update-existing", action="store_true", help="Allow updating existing doc_ids (default: insert-only)")
    return ap.parse_args()


def ingest():
    args = parse_args()

    global DOC_ROOT, MONGO_URI, DB_NAME, COLL_NAME
    DOC_ROOT, MONGO_URI, DB_NAME, COLL_NAME = args.doc_root, args.mongo_uri, args.db_name, args.coll_name

    trib_only: Set[str] = set([s.strip() for s in (args.only_trib or "").split(",") if s.strip()])
    year_only: Set[str] = set([s.strip() for s in (args.only_year or "").split(",") if s.strip()])

    root = Path(DOC_ROOT).resolve()
    if not root.exists():
        print(f"❌ DOC_ROOT not found: {root}")
        return

    client, col = connect_collection()
    try:
        ensure_indexes(col)

        all_tribs = sorted([d for d in root.iterdir() if d.is_dir()])
        tribunals = [d for d in all_tribs if not trib_only or d.name in trib_only]

        for trib_dir in tribunals:
            category_name = trib_dir.name
            csv_path = pick_csv_in(trib_dir)
            if not csv_path:
                print(f"⚠️  {category_name}: no CSV found, skipping.")
                continue

            rows = load_csv_rows(csv_path)
            if not rows:
                print(f"⚠️  {category_name}: CSV empty or missing required headers, skipping.")
                continue

            # optional filter by year
            if year_only:
                rows = [r for r in rows if r["year"] in year_only]
                if not rows:
                    print(f"ℹ️  {category_name}: no rows for selected years, skipping.")
                    continue

            # fetch existing ids only if we're insert-only
            existing_ids: Set[int] = set()
            if not args.update_existing:
                ids = []
                for r in rows:
                    try:
                        ids.append(int(r["doc_id"]))
                    except Exception:
                        pass
                if ids:
                    cur = col.find({"doc_id": {"$in": ids}}, {"doc_id": 1})
                    existing_ids = {d["doc_id"] for d in cur}

            inserted = updated = skipped_existing = unmatched = 0

            # group by year
            by_year: Dict[str, List[Dict[str, str]]] = {}
            for r in rows:
                by_year.setdefault(r["year"], []).append(r)

            for year_str, year_rows in sorted(by_year.items()):
                year_dir = trib_dir / year_str
                if not (year_dir.exists() and year_dir.is_dir()):
                    print(f"  ⚠️ {category_name}/{year_str}: folder missing (rows={len(year_rows)})")
                    unmatched += len(year_rows)
                    continue

                idx = build_year_index(year_dir)
                by_id = idx["by_id"]
                by_title = idx["by_title"]

                for row in year_rows:
                    # parse ids
                    try:
                        doc_id_int = int(row["doc_id"])
                        year_int = int(year_str)
                    except Exception:
                        unmatched += 1
                        continue

                    # Skip existing in insert-only mode
                    if not args.update_existing and doc_id_int in existing_ids:
                        skipped_existing += 1
                        continue

                    # EITHER doc_id OR title
                    p = by_id.get(str(doc_id_int))
                    if not p:
                        p = by_title.get(norm_key(row["title"]))

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

                    document = {
                        "doc_id": doc_id_int,
                        "full_title": row["title"],
                        "category": "tribunals",
                        "category_name": category_name,
                        "year": year_int,
                        "law_type": "tribunal",
                        "word_count": word_count_from_html(html),
                        "content": html
                    }

                    if args.update_existing:
                        res = col.update_one({"doc_id": doc_id_int}, {"$set": document}, upsert=True)
                        if res.matched_count == 0 and res.upserted_id is not None:
                            inserted += 1
                        else:
                            updated += 1
                    else:
                        res = col.update_one({"doc_id": doc_id_int}, {"$setOnInsert": document}, upsert=True)
                        if res.upserted_id is not None:
                            inserted += 1
                        else:
                            skipped_existing += 1

            print(f"✅ {category_name}: inserted={inserted}, updated={updated}, skipped_existing={skipped_existing}, unmatched={unmatched}")

        total = col.count_documents({})
        print({"total_docs_in_collection": total})
    finally:
        client.close()
        print("✅ MongoDB connection closed.")


if __name__ == "__main__":
    ingest()

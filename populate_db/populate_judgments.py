#!/usr/bin/env python3
"""
Populate MongoDB: legal_dashboard_db.judgments (simple & readable)

Flow:
  1) (optional) DROP or CLEAR the 'judgments' collection
  2) Import rows from CSV (upsert by doc_id, normalize headers)
  3) Backfill content_html from local HTML files
"""

import os
import csv
import re
from pathlib import Path
from typing import Dict, Any, Optional
from pymongo import MongoClient, ASCENDING, TEXT

# ========= CONFIG (edit these) =========
CSV_PATH   = "/DATACHAI/Data/Judments/Supreme_Court/supreme_court_logs_enriched.csv"
DOC_ROOT   = "/DATACHAI/Data/Judments/Supreme_Court"

MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "legal_dashboard_db"
COLL_NAME  = "judgments"

# choose ONE of these behaviors:
DROP_COLLECTION  = True    # drop the collection (removes docs + indexes)
CLEAR_COLLECTION = False   # OR: delete documents but keep indexes
# ======================================

FALLBACK_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
MAX_BYTES = 14_000_000  # stay under Mongo's ~16MB

# ---------- small helpers ----------

def snake(s: str) -> str:
    """Convert header names to snake_case."""
    s = s.strip().replace(" ", "_")
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower()

def to_int_or_none(v: Optional[str]):
    if v is None: return None
    v = v.strip()
    if v == "": return None
    try:
        return int(v)
    except:
        return v

def normalize_row(row: Dict[str, str]) -> Dict[str, Any]:
    """Normalize CSV field names + cast common ints."""
    out: Dict[str, Any] = {}
    for k, v in row.items():
        nk = snake(k)
        out[nk] = v.strip() if isinstance(v, str) else v
    if "year" in out:  out["year"]  = to_int_or_none(out["year"])
    if "month" in out: out["month"] = to_int_or_none(out["month"])
    if "day" in out:   out["day"]   = to_int_or_none(out["day"])
    return out

def read_text_file(p: Path):
    """Read a file with encoding fallbacks; return (text, encoding) or (None, None)."""
    for enc in FALLBACK_ENCODINGS:
        try:
            return p.read_text(encoding=enc, errors="replace"), enc
        except UnicodeDecodeError:
            continue
        except Exception:
            break
    return None, None

def build_html_index(root: Path) -> Dict[str, Path]:
    """
    One-time index: {doc_id -> full path of .html/.htm}
    Prefer .html over .htm when both exist.
    """
    print(f"🔎 Indexing HTML under: {root}")
    idx: Dict[str, Path] = {}
    scanned = 0
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        suf = p.suffix.lower()
        if suf not in (".html", ".htm"):
            continue
        stem = p.stem  # we assume filename like 12345.html where 12345 == doc_id
        prev = idx.get(stem)
        if prev is None or (suf == ".html" and prev.suffix.lower() == ".htm"):
            idx[stem] = p.resolve()
        scanned += 1
        if scanned % 5000 == 0:
            print(f"  ...indexed {scanned} files")
    print(f"✅ HTML index ready: {len(idx)} doc_ids mapped")
    return idx

# ---------- main steps ----------

def connect_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLL_NAME]
    # quick ping
    client.admin.command("ping")
    return client, col

def drop_or_clear(col):
    if DROP_COLLECTION:
        print(f"🗑️  Dropping {DB_NAME}.{COLL_NAME} ...")
        col.drop()
        print("   dropped.")
    elif CLEAR_COLLECTION:
        print(f"🧹 Clearing documents in {DB_NAME}.{COLL_NAME} ...")
        res = col.delete_many({})
        print(f"   removed {res.deleted_count} docs.")
    else:
        print("🚫 Drop/Clear: skipped (edit flags at top if you want a clean start)")

def ensure_indexes(col):
    print("📚 Ensuring indexes ...")
    col.create_index([("doc_id", ASCENDING)], unique=True, name="doc_id_unique")
    col.create_index([("year", ASCENDING)], name="year_idx")
    col.create_index([("title", TEXT)], name="title_text_idx")

def import_csv(col):
    print(f"📥 Importing CSV: {CSV_PATH}")
    imported = 0
    missing_docid = 0

    for enc in FALLBACK_ENCODINGS:
        try:
            with open(CSV_PATH, newline="", encoding=enc, errors="replace") as f:
                reader = csv.DictReader(f)
                for raw in reader:
                    row = normalize_row(raw)
                    doc_id = row.get("doc_id")
                    title = row.get("title") or row.get("case_title") or row.get("full_title")

                    if not doc_id:
                        missing_docid += 1
                        continue

                    # clean empty strings to None
                    for k in list(row.keys()):
                        if isinstance(row[k], str) and row[k].strip() == "":
                            row[k] = None
                    if title:
                        row["title"] = title

                    col.update_one({"doc_id": str(doc_id)}, {"$set": row}, upsert=True)
                    imported += 1
            print(f"   ✅ OK with encoding={enc}")
            break
        except UnicodeDecodeError:
            continue

    print(f"   → upserted: {imported} rows (skipped without doc_id: {missing_docid})")

def backfill_html(col):
    print(f"🧩 Backfilling HTML from: {DOC_ROOT}")
    root = Path(DOC_ROOT).resolve()
    if not root.exists():
        print(f"   ❌ doc-root not found: {root}")
        return

    html_idx = build_html_index(root)

    total = col.count_documents({})
    missing_before = col.count_documents({"content_html": {"$exists": False}})
    print(f"   before: missing={missing_before} / total={total}")

    processed = updated = skipped = 0

    cursor = col.find(
        {"content_html": {"$exists": False}},
        {"_id": 0, "doc_id": 1, "path": 1, "html_path": 1}
    )

    for doc in cursor:
        processed += 1
        did = str(doc.get("doc_id") or "")
        if not did:
            skipped += 1
            continue

        html = None
        enc_used = None
        src = None

        # 1) try explicit relative/absolute path from CSV (path/html_path)
        pval = doc.get("path") or doc.get("html_path")
        if pval:
            p = Path(pval)
            if not p.is_absolute():
                p = root / p
            if p.exists() and p.is_file() and p.suffix.lower() in (".html", ".htm"):
                html, enc_used = read_text_file(p)
                if html:
                    src = {"kind": "file", "path": str(p)}

        # 2) fallback: index lookup by doc_id -> {doc_id}.html/htm
        if html is None:
            p = html_idx.get(did)
            if p and p.exists():
                html, enc_used = read_text_file(p)
                if html:
                    src = {"kind": "file", "path": str(p)}

        if html is None:
            skipped += 1
            continue

        # keep under Mongo size limit
        if len(html.encode("utf-8", errors="replace")) > MAX_BYTES:
            skipped += 1
            continue

        col.update_one(
            {"doc_id": did},
            {"$set": {
                "content_html": html,
                "content_meta": {"source": src, "encoding": enc_used}
            }},
            upsert=False
        )
        updated += 1

        if processed % 2000 == 0:
            print(f"   progress: processed={processed}, updated={updated}, skipped={skipped}")

    missing_after = col.count_documents({"content_html": {"$exists": False}})
    print(f"   done: processed={processed}, updated={updated}, skipped={skipped}")
    print(f"   after : missing={missing_after} / total={total}")

def main():
    print("=== Populate judgments (simple) ===")
    client, col = connect_collection()

    try:
        drop_or_clear(col)
        ensure_indexes(col)
        import_csv(col)
        backfill_html(col)

        total = col.count_documents({})
        have  = col.count_documents({"content_html": {"$exists": True}})
        miss  = col.count_documents({"content_html": {"$exists": False}})
        print({"total": total, "with_content_html": have, "missing": miss})
    finally:
        client.close()
        print("✅ MongoDB connection closed.")

if __name__ == "__main__":
    main()

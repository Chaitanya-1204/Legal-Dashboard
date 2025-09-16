# app/high_courts/routes.py
from __future__ import annotations

from flask import Blueprint, render_template, abort, request, jsonify, url_for
from werkzeug.routing import BuildError
from app import db
import os, re, html as _html, random
from collections import OrderedDict

high_courts_bp = Blueprint("high_courts", __name__, template_folder="../templates")

# -------- minimal helpers: return the *raw* stored HTML (or text) ------------
def _best_html(d: dict) -> str | None:
    """
    Return the first non-empty string among common HTML/text fields.
    NO parsing/cleaning here – let the template do everything.
    """
    candidate_keys = (
        "content_html", "html", "raw_html", "body_html", "content",
        "document_html", "source_html", "page_html",
        "content_text", "text", "body_text", "plain", "raw"
    )
    for k in candidate_keys:
        v = d.get(k)
        if isinstance(v, (str, bytes)):
            if isinstance(v, bytes):
                try:
                    v = v.decode("utf-8", errors="ignore")
                except Exception:
                    continue
            if v.strip():
                return v
    # optional: file path fallback if you keep local files
    for k in ("path", "file_path", "local_path"):
        p = d.get(k)
        if isinstance(p, str) and os.path.exists(p):
            try:
                with open(p, "rb") as f:
                    b = f.read()
                for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
                    try:
                        s = b.decode(enc)
                        if s.strip():
                            return s
                    except UnicodeDecodeError:
                        continue
                return b.decode("utf-8", errors="ignore")
            except Exception:
                pass
    return None


# ------------------------------ list -----------------------------------------
@high_courts_bp.route("/")
def list_all():
    try:
        q = (request.args.get("q") or "").strip()
        year = request.args.get("year")
        mongo = {}
        if year and year.isdigit():
            mongo["year"] = int(year)
        if q:
            mongo["$or"] = [
                {"doc_id": {"$regex": q, "$options": "i"}},
                {"title": {"$regex": q, "$options": "i"}},
                {"full_title": {"$regex": q, "$options": "i"}},
            ]

        fields = {"_id": 0, "doc_id": 1, "title": 1, "full_title": 1, "year": 1}
        rows = list(
            db.high_courts.find(mongo, fields)
            .limit(500)
            .sort([("year", 1), ("doc_id", 1)])
        )
        return render_template(
            "hc_list.html",
            judgments=rows,
            year=(int(year) if year and year.isdigit() else None),
        )
    except Exception as e:
        print(f"[high_courts.list_all] error: {e}")
        abort(500)


# ------------------------------ view -----------------------------------------
@high_courts_bp.route("/view/<string:doc_id>")
def view(doc_id: str):
    """
    Render a single High Court judgment page.
    We *only* fetch and pass through the raw HTML/text. The template does all UI work.
    """
    try:
        d = db.high_courts.find_one({"doc_id": doc_id}, {"_id": 0})
        if not d:
            abort(404)

        if not d.get("title"):
            d["title"] = d.get("full_title") or d.get("doc_id")

        raw_html = _best_html(d) or ""

        # SAFE resolver base (avoid BuildError if resolver blueprint not registered)
        try:
            resolver_url0 = url_for("resolver.resolve_doc", doc_id=0)
        except BuildError:
            resolver_url0 = ""  # template JS will fallback to '/doc/0' if empty

        return render_template(
            "hc_view.html",
            judgment=d,
            raw_html=raw_html,
            # Links for the three buttons that open analyses in a separate tab:
            ner_url=url_for("high_courts.analyze_page", doc_id=doc_id, kind="ner"),
            sum_url=url_for("high_courts.analyze_page", doc_id=doc_id, kind="summary"),
            rr_url=url_for("high_courts.analyze_page", doc_id=doc_id, kind="rr"),
            resolver_url0=resolver_url0,
        )
    except Exception as e:
        print(f"[high_courts.view] error: {e}")
        abort(500)


# ------------------------------ OpenNyAI runner -------------------------------
def _run_opennyai(kind: str, text: str) -> dict:
    try:
        from opennyai import Pipeline
        from opennyai.utils import Data
    except Exception as e:
        return {"error": f"OpenNyAI not available: {type(e).__name__}: {e}"}

    data = Data([text])

    if kind == "ner":
        pipe = Pipeline(components=["NER"], use_gpu=False)
        return pipe(data)[0]
    if kind == "summary":
        pipe = Pipeline(components=["Summarizer"], use_gpu=False)
        return pipe(data)[0]
    if kind == "rr":
        pipe = Pipeline(components=["Rhetorical_Role"], use_gpu=False)
        return pipe(data)[0]
    return {"error": "unknown kind"}


# ------------------------------ text utilities -------------------------------
def _strip_tags_and_junk(html: str) -> str:
    """Remove script/style/noscript/template blocks first, then strip tags."""
    if not html:
        return ""
    html = re.sub(r"(?is)<(script|style|noscript|template)[^>]*>.*?</\1>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    html = re.sub(r"\s+", " ", html).strip()
    return html

def _sentences(text: str) -> list[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[.?!])\s+", text)
    return [p.strip() for p in parts if p.strip()]

def _chunk_sentences(parts: list[str], max_per_box: int = 6) -> list[str]:
    if not parts:
        return []
    out, buf = [], []
    for s in parts:
        buf.append(s)
        if len(buf) >= max_per_box:
            out.append(" ".join(buf))
            buf = []
    if buf:
        out.append(" ".join(buf))
    return out

def _extract_summary_boxes(result: dict, full_text: str) -> list[str]:
    """
    Build 'box' paragraphs for Summary:
    - If there's a plain summary string, use it and chunk by sentences.
    - Else, if there are 'annotations' with in_summary==True, join those texts and chunk.
    """
    for key in ("summary", "summary_text", "Summarizer", "Summary", "summarizer"):
        v = result.get(key)
        if isinstance(v, str) and v.strip():
            sents = _sentences(v.strip())
            return _chunk_sentences(sents, max_per_box=6)

    anns = result.get("annotations")
    if isinstance(anns, list) and anns:
        try:
            anns = sorted(anns, key=lambda a: int(a.get("start", 0)))
        except Exception:
            pass
        segs = []
        for a in anns:
            if a.get("in_summary") is True and isinstance(a.get("text"), str):
                t = a["text"].strip()
                if t:
                    segs.append(t)
        if segs:
            joined = " ".join(segs)
            sents = _sentences(joined)
            return _chunk_sentences(sents, max_per_box=6)

    sents = _sentences(full_text)[:36]
    return _chunk_sentences(sents, max_per_box=6)


def _extract_rr_sections(result: dict, full_text: str) -> list[dict]:
    """
    Normalize rhetorical-role output into:
      [{"role": <label>, "text": <joined text>, "confidence": <avg or None>}, ...]
    Supports shapes:
      - {"annotations": [{"labels": ["FACTS"], "text": "...", "score": 0.92}, ...]}
      - {"rhetorical_roles": [{"role": "...", "text": "...", "confidence": 0.9}, ...]}
      - {"Rhetorical_Role": [{"label": "...", "span": "...", "score": 0.9}, ...]}
    """
    if not isinstance(result, dict):
        return []

    # Case A: direct list already normalized
    for key in ("rhetorical_roles", "roles"):
        v = result.get(key)
        if isinstance(v, list) and v and isinstance(v[0], dict) and ("role" in v[0] or "label" in v[0]):
            out = []
            for it in v:
                role = it.get("role") or it.get("label") or "Section"
                txt  = it.get("text") or it.get("span") or ""
                conf = it.get("confidence") or it.get("score")
                try:
                    conf = float(conf) if conf is not None else None
                except Exception:
                    conf = None
                if txt.strip():
                    out.append({"role": str(role), "text": txt.strip(), "confidence": conf})
            return out

    # Case B: common OpenNyAI shape under "Rhetorical_Role"
    v = result.get("Rhetorical_Role")
    if isinstance(v, list) and v and isinstance(v[0], dict):
        out = []
        for it in v:
            role = it.get("role") or it.get("label") or "Section"
            txt  = it.get("text") or it.get("span") or ""
            conf = it.get("confidence") or it.get("score")
            try:
                conf = float(conf) if conf is not None else None
            except Exception:
                conf = None
            if txt.strip():
                out.append({"role": str(role), "text": txt.strip(), "confidence": conf})
        return out

    # Case C: flat "annotations" list with labels + text
    anns = result.get("annotations")
    if isinstance(anns, list) and anns:
        buckets: "OrderedDict[str, list[str]]" = OrderedDict()
        confs:   "OrderedDict[str, list[float]]" = OrderedDict()

        for a in anns:
            if not isinstance(a, dict):
                continue
            lbl = None
            if isinstance(a.get("labels"), list) and a["labels"]:
                lbl = a["labels"][0]
            lbl = lbl or a.get("label") or a.get("summary_section") or "Section"
            txt = (a.get("text") or "").strip()
            if not txt:
                try:
                    s = int(a.get("start", -1)); e = int(a.get("end", -1))
                    if 0 <= s < e <= len(full_text):
                        txt = full_text[s:e].strip()
                except Exception:
                    pass
            if not txt:
                continue

            score = a.get("score") or a.get("confidence")
            try:
                score = float(score) if score is not None else None
            except Exception:
                score = None

            buckets.setdefault(str(lbl), []).append(txt)
            if score is not None:
                confs.setdefault(str(lbl), []).append(score)

        out = []
        for role, pieces in buckets.items():
            avg_conf = None
            if role in confs and confs[role]:
                avg_conf = sum(confs[role]) / len(confs[role])
            out.append({"role": role, "text": "\n\n".join(pieces), "confidence": avg_conf})
        return out

    return []


# ------------------------------ NER with spaCy fallback ----------------------
def _ner_with_fallback(text: str) -> dict:
    """
    Try OpenNyAI NER first. If that fails or returns nothing,
    fall back to spaCy (en_core_web_trf if available, else en_core_web_sm).
    Returns a dict shaped like {"engine": "...", "entities": [...]}
    """
    # 1) OpenNyAI attempt
    try:
        res = _run_opennyai("ner", text)
        if isinstance(res, dict):
            ents = res.get("entities") or res.get("NER") or res.get("labels")
            if isinstance(ents, list) and ents:
                return {"engine": "opennyai", "entities": ents}
    except Exception:
        pass  # fall through to spaCy


    print("opennyai failec");    

    # 2) spaCy fallback
    try:
        import spacy
        try:
            nlp = spacy.load("en_core_web_trf")
        except Exception:
            nlp = spacy.load("en_core_web_sm")
        doc = nlp(text)
        ents = [{"start": e.start_char, "end": e.end_char, "label": e.label_, "text": e.text} for e in doc.ents]
        return {"engine": "spacy", "entities": ents}
    except Exception as e:
        return {"error": f"NER unavailable (OpenNyAI + spaCy failed): {type(e).__name__}: {e}"}


# ------------------------------ analyze page ---------------------------------
@high_courts_bp.route("/view/<string:doc_id>/analyze/<string:kind>")
def analyze_page(doc_id: str, kind: str):
    """
    Page that renders NER / Summary / Rhetorical Roles for the given doc.
    NER: highlighted HTML + table (with spaCy fallback)
    Summary: boxed paragraphs
    RR: grouped sections by role
    """
    try:
        d = db.high_courts.find_one({"doc_id": doc_id}, {"_id": 0})
        if not d:
            abort(404)

        raw_html = _best_html(d) or ""
        text = _strip_tags_and_junk(raw_html)

        annotated_html = None
        ner_entities = None
        summary_boxes = None
        rr_sections = None
        raw_result = None

        k = kind.lower()
        if k == "ner":
            ner_out = _ner_with_fallback(text)
            raw_result = ner_out  # for ?debug=1
            if ner_out.get("error"):
                annotated_html = None
                ner_entities = None
            else:
                entities = ner_out.get("entities") or []
                norm = []
                for e in entities:
                    start = e.get("start") or e.get("begin") or e.get("start_char")
                    end   = e.get("end")   or e.get("stop")  or e.get("end_char")
                    label = e.get("label") or e.get("type")  or e.get("tag") or e.get("entity")
                    txt   = e.get("text")  or e.get("word")  or e.get("span")
                    if txt is None and start is not None and end is not None:
                        txt = text[int(start):int(end)]
                    if start is None or end is None:
                        if txt:
                            idx = text.lower().find(str(txt).lower())
                            if idx >= 0:
                                start, end = idx, idx + len(txt)
                    if start is None or end is None:
                        continue
                    norm.append({
                        "start": int(start),
                        "end": int(end),
                        "label": str(label or ""),
                        "text":  txt or text[int(start):int(end)],
                    })

                if norm:
                    norm.sort(key=lambda e: (e["start"], e["end"]))
                    pos, out = 0, []
                    colors = {}
                    def style_for(label: str) -> str:
                        if label in colors:
                            return colors[label]
                        h = random.randint(0, 360)
                        s = f"background:hsl({h} 100% 92%);border:1px solid hsl({h} 45% 70%);padding:0 2px;border-radius:6px"
                        colors[label] = s
                        return s

                    for ent in norm:
                        s, epos, lab = ent["start"], ent["end"], ent["label"]
                        if s < pos:  # skip overlaps
                            continue
                        out.append(_html.escape(text[pos:s]))
                        span_txt = _html.escape(text[s:epos])
                        out.append(
                            f'<span title="{_html.escape(lab)}" style="{style_for(lab)}">'
                            f'{span_txt}<sup style="font-size:10px;margin-left:2px">{_html.escape(lab)}</sup>'
                            f'</span>'
                        )
                        pos = epos
                    out.append(_html.escape(text[pos:]))
                    annotated_html = "".join(out)
                    ner_entities = norm

        elif k == "summary":
            res = _run_opennyai("summary", text)
            raw_result = res
            summary_boxes = _extract_summary_boxes(res, text)

        elif k == "rr":
            res = _run_opennyai("rr", text)
            raw_result = res
            rr_sections = _extract_rr_sections(res, text)

        debug = (request.args.get("debug") == "1")

        return render_template(
            "hc_analyze.html",
            doc_id=doc_id,
            kind=kind.upper(),
            result=raw_result or {},
            debug=debug,
            annotated_html=annotated_html,
            ner_entities=ner_entities,
            summary_boxes=summary_boxes,
            rr_sections=rr_sections,
            back_url=url_for("high_courts.view", doc_id=doc_id),
        )
    except Exception as e:
        print(f"[high_courts.analyze_page] error: {e}")
        return jsonify({"error": str(e)}), 500

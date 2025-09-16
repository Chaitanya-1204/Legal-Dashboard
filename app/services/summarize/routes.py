from flask import request, jsonify, render_template
from . import summary_bp
from .summarizer import summarize_text

# JSON API: POST /summary/api/gemini
summary_bp.post("/api/gemini")
def api_gemini():
    data = request.get_json(force=True) or {}
    html = data.get("html")
    text = data.get("text")

    # HARD-SET a valid Gemini model, ignore input for now
    model = "gemini-2.5-flash"
    print("[route] api_gemini model (forced):", model)

    target_tokens = int(data.get("target_tokens", 200))
    min_tokens = int(data.get("min_tokens", 70))

    payload = html if html is not None else (text or "")
    is_html = html is not None

    result = summarize_text(
        text_or_html=payload,
        model_name=model,
        target_tokens=target_tokens,
        min_tokens=min_tokens,
        is_html=is_html,
    )
    return jsonify({"summary": result["summary"], "meta": result["meta"]})



@summary_bp.post("/view_from_html")
def view_from_html():
    # DEBUG: print everything the form sent
    print("[route] incoming form:", dict(request.form))

    html = request.form.get("html") or ""
    # HARD-SET a valid Gemini model, ignore input for now
    model = "gemini-2.5-flash"
    print("[route] view_from_html model (forced):", model)

    target_tokens = int(request.form.get("target_tokens", 200))
    min_tokens = int(request.form.get("min_tokens", 70))
    doc_type = request.form.get("doc_type") or "judgments"
    doc_id = request.form.get("doc_id") or "NA"

    result = summarize_text(
        text_or_html=html,
        model_name=model,
        target_tokens=target_tokens,
        min_tokens=min_tokens,
        is_html=True,
    )
    return render_template(
        "summarize/summary_view.html",
        summary=result["summary"],
        meta=result["meta"],
        doc_type=doc_type,
        doc_id=doc_id,
        model=model,
    )

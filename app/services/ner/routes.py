# app/services/ner/routes.py
from __future__ import annotations
from flask import Blueprint, request, jsonify
from .gemini import GeminiNER, get_labels, clean_container_html, DEFAULT_MAX_CHARS_PER_CALL

ner_bp = Blueprint("ner", __name__)

_ENGINE: GeminiNER | None = None

def _get_engine() -> GeminiNER:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = GeminiNER()  # raises if GOOGLE_API_KEY missing
    return _ENGINE

@ner_bp.route("/ping", methods=["GET"])
def ping():
    try:
        eng = _get_engine()
        return jsonify({"status": "ok", "model": eng.model}), 200
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 503

@ner_bp.route("/extract", methods=["POST"])
def extract():
    try:
        data = request.get_json(force=True) or {}
        text = (data.get("text") or "").strip()
        labels = data.get("labels") or get_labels()
        max_chars = int(data.get("max_chars_per_call") or DEFAULT_MAX_CHARS_PER_CALL)
        if not text:
            return jsonify({"error": "missing 'text'"}), 400
        spans = _get_engine().extract_spans(text, labels=labels, max_chars=max_chars)
        return jsonify({"spans": [{"start": s, "end": e, "label": L} for s, e, L in spans]}), 200
    except Exception as e:
        return jsonify({"error": "server_error", "detail": str(e)}), 500

@ner_bp.route("/annotate", methods=["POST"])
def annotate():
    try:
        data = request.get_json(force=True) or {}
        html = data.get("html") or ""
        labels = data.get("labels") or get_labels()
        max_chars = int(data.get("max_chars_per_call") or DEFAULT_MAX_CHARS_PER_CALL)
        if not html.strip():
            return jsonify({"error": "missing 'html'"}), 400

        # Optional: narrow to main content container before annotation
        container_selector = (data.get("container_selector") or "").strip() or ".judgments"
        cleaned = clean_container_html(html, container_selector=container_selector)

        annotated = _get_engine().annotate_html(cleaned, labels=labels, max_chars=max_chars)
        return jsonify({"html": annotated}), 200
    except Exception as e:
        return jsonify({"error": "server_error", "detail": str(e)}), 500

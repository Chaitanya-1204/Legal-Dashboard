# app/services/summarize/summarizer.py
from typing import List, Dict, Any
import os

from bs4 import BeautifulSoup
from google import genai
from google.genai.errors import ClientError

# -------------------------------
# Lazy Gemini client
# -------------------------------
_client = None
def _client_gemini():
    global _client
    if _client is None:
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError("Missing API key. Set GEMINI_API_KEY or GOOGLE_API_KEY in .env")
        _client = genai.Client(api_key=api_key)
    return _client


# -------------------------------
# HTML → clean text
# -------------------------------
DROP_TAGS = ["script", "style", "noscript", "iframe", "header", "footer"]
UNWRAP_TAGS = ["font", "center"]
DROP_SELECTORS = [
    ".ads", ".share", ".share-buttons", "[id^='ad-']",
    ".ad_doc", ".ad-doc", ".doc_citations", ".doc-citations",
    ".breadcrumbs", ".nav", ".topbar"
]

def html_to_clean_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for t in soup(DROP_TAGS):
        t.decompose()
    for sel in DROP_SELECTORS:
        for el in soup.select(sel):
            el.decompose()
    for t in soup(UNWRAP_TAGS):
        t.unwrap()
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines()]
    return "\n".join([ln for ln in lines if ln])


# -------------------------------
# Chunking helpers
# -------------------------------
CHUNK_IF_ABOVE_CHARS = 120_000   # single call if <= this
CHARS_PER_CHUNK      = 80_000    # used only when we must chunk
HARD_CLAMP_CHARS     = 600_000   # absolute safety clamp

def _split_text(t: str, chunk_size: int = CHARS_PER_CHUNK) -> List[str]:
    t = (t or "").strip()
    if not t:
        return []
    if len(t) <= chunk_size:
        return [t]
    chunks, start, n = [], 0, len(t)
    while start < n:
        end = min(n, start + chunk_size)
        cut = t.rfind("\n\n", start, end)
        if cut == -1 or cut <= start + int(0.5 * chunk_size):
            cut = end
        chunks.append(t[start:cut].strip())
        start = cut
    return [c for c in chunks if c]


# -------------------------------
# Prompts (HTML output)
# -------------------------------
SYSTEM = (
    "You are a legal summarizer. Summarize Indian legal documents (judgments/tribunal orders) "
    "neutrally for a dashboard. Return ONLY a minimal, valid HTML snippet (no <html> or <body>), "
    "structured exactly as sections inside <article class='ld-summary'>."
)

HTML_BLOCK_GUIDE = """
Structure your answer EXACTLY like this (omit a section if truly not present; never write placeholders):

<article class="ld-summary">
  <section data-key="parties">
    <h3>Parties / Forum</h3>
    <p>…</p>
  </section>
  <section data-key="bench">
    <h3>Bench / Coram</h3>
    <p>…</p>
  </section>
  <section data-key="issues">
    <h3>Issues / Questions</h3>
    <ul><li>…</li></ul>
  </section>
  <section data-key="facts">
    <h3>Key Facts</h3>
    <ul><li>…</li></ul>
  </section>
  <section data-key="arguments">
    <h3>Arguments</h3>
    <ul>
      <li><strong>Appellants:</strong> …</li>
      <li><strong>Respondent:</strong> …</li>
    </ul>
  </section>
  <section data-key="holding">
    <h3>Holding / Reasoning</h3>
    <ul><li>…</li></ul>
  </section>
  <section data-key="disposition">
    <h3>Final Order / Disposition</h3>
    <p>…</p>
  </section>
  <section data-key="citations">
    <h3>Important Citations</h3>
    <ul><li>…</li></ul>
  </section>
</article>

Rules:
- Use only: <article>, <section>, <h3>, <p>, <ul>, <li>, <strong>, <em>, <span>, <br>.
- Do NOT include Markdown, JSON, code fences, scripts, or styles.
- Be concise; about {target_tokens} tokens total.
"""

SINGLE_PROMPT = """{system}

{guide}

CONTENT START
{content}
CONTENT END
"""

CHUNK_PROMPT = """{system}

You are summarizing an excerpt of a longer document. Produce the SAME HTML structure,
but keep this excerpt concise (~{target_tokens} tokens for THIS part).

{guide}

EXCERPT START
{chunk}
EXCERPT END
"""

FUSE_PROMPT = """{system}

You are given {n} partial HTML summaries (same structure as specified). Merge them into ONE
clean final HTML summary with those sections (omit duplicates and missing info).
Target ~{target_tokens} tokens.

PARTIAL SUMMARIES START
{partials}
PARTIAL SUMMARIES END
"""


# -------------------------------
# Low-level generation with fallbacks
# -------------------------------
def _gen(model: str, prompt: str) -> str:
    client = _client_gemini()

    # ordered unique candidates
    raw_candidates = []
    if model:
        raw_candidates.append(model)
        if not model.startswith("models/"):
            raw_candidates.append(f"models/{model}")
    for fb in ["gemini-1.5-flash-8b", "gemini-1.5-flash"]:
        raw_candidates.append(fb)
        raw_candidates.append(f"models/{fb}")

    seen, candidates = set(), []
    for m in raw_candidates:
        if m not in seen:
            seen.add(m)
            candidates.append(m)

    last_err = None
    for m in candidates:
        try:
            print(f"[gen] trying model: {m}")
            resp = client.models.generate_content(
                model=m,
                contents=prompt,
                config={"thinking_config": {"thinking_budget": 0}},
            )
            return (resp.text or "").strip()
        except ClientError as e:
            if getattr(e, "status_code", None) == 404:
                last_err = e
                continue
            raise
    if last_err:
        raise last_err
    raise RuntimeError("Model resolution failed unexpectedly")


# -------------------------------
# Minimal sanitize/tidy for returned HTML
# -------------------------------
ALLOWED_TAGS = {"article","section","h3","p","ul","li","strong","em","span","br"}
def _tidy_summary_html(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "html.parser")

    # remove scripts/styles if any slipped in
    for bad in soup(["script","style"]):
        bad.decompose()

    # drop tags not in allowlist (keep their text)
    for el in soup.find_all(True):
        if el.name not in ALLOWED_TAGS:
            el.unwrap()

    # ensure the wrapper exists
    art = soup.find("article", {"class": "ld-summary"})
    if not art:
        # wrap whole thing
        wrapper = soup.new_tag("article", **{"class": "ld-summary"})
        wrapper.append(soup)
        soup = wrapper

    # return outer HTML
    return str(soup)


# -------------------------------
# Public API
# -------------------------------
def summarize_text(
    *,
    text_or_html: str,
    model_name: str = "gemini-1.5-flash",
    target_tokens: int = 200,
    min_tokens: int = 70,
    is_html: bool = True
) -> Dict[str, Any]:
    """
    Returns: {'summary': HTML_SNIPPET, 'meta': {...}}
    - Single model call when cleaned text is small.
    - Chunk+Fuse only when necessary.
    - Output is HTML (pretty by template styles).
    """
    plain = html_to_clean_text(text_or_html) if is_html else (text_or_html or "").strip()
    if not plain:
        return {"summary": "", "meta": {"model": model_name, "chunks": 0, "chars": 0}}

    if len(plain) > HARD_CLAMP_CHARS:
        plain = plain[:HARD_CLAMP_CHARS]

    # Single-shot path
    if len(plain) <= CHUNK_IF_ABOVE_CHARS:
        prompt = SINGLE_PROMPT.format(system=SYSTEM, guide=HTML_BLOCK_GUIDE.format(target_tokens=target_tokens), content=plain)
        try:
            html = _gen(model_name, prompt)
            return {
                "summary": _tidy_summary_html(html),
                "meta": {"model": model_name, "chunks": 1, "chars": len(plain)},
            }
        except ClientError as e:
            msg = getattr(e, "message", str(e)) or "Model error"
            return {
                "summary": f"<article class='ld-summary'><section><h3>Error</h3><p>Summarization failed: {msg}</p></section></article>",
                "meta": {"model": model_name, "chunks": 1, "chars": len(plain), "error": "client_error"},
            }

    # Chunk + fuse path
    chunks = _split_text(plain, CHARS_PER_CHUNK)
    partials: List[str] = []
    try:
        for ch in chunks:
            prompt = CHUNK_PROMPT.format(
                system=SYSTEM,
                guide=HTML_BLOCK_GUIDE.format(target_tokens=target_tokens),
                target_tokens=target_tokens,
                chunk=ch,
            )
            partials.append(_tidy_summary_html(_gen(model_name, prompt)))

        if len(partials) == 1:
            final_html = partials[0]
        else:
            joined = "\n\n---\n\n".join(partials)
            fuse = FUSE_PROMPT.format(
                system=SYSTEM,
                n=len(partials),
                target_tokens=max(target_tokens, min_tokens),
                partials=joined,
            )
            final_html = _tidy_summary_html(_gen(model_name, fuse))

        return {
            "summary": final_html,
            "meta": {"model": model_name, "chunks": len(chunks), "chars": len(plain)},
        }
    except ClientError as e:
        msg = getattr(e, "message", str(e)) or "Model error"
        return {
            "summary": f"<article class='ld-summary'><section><h3>Error</h3><p>Summarization failed: {msg}</p></section></article>",
            "meta": {"model": model_name, "chunks": len(chunks), "chars": len(plain), "error": "client_error"},
        }

# app/services/ner/gemini.py
from __future__ import annotations

import os
import time
import json
import math
import threading
import hashlib
import random
from typing import List, Tuple, Optional, Iterable

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

# ===== Constants / Config =====
DEFAULT_MODEL = os.getenv("NER_MODEL_NAME", "gemini-1.5-flash")
DEFAULT_MAX_CHARS_PER_CALL = int(os.getenv("NER_MAX_CHARS_PER_CALL", "6000"))
GEMINI_QPS = float(os.getenv("GEMINI_QPS", "0.75"))  # ~1 call / 1.33s default
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{DEFAULT_MODEL}:generateContent"
NER_DEBUG = os.getenv("NER_DEBUG", "0") == "1"  # set to "1" to print debug lines

_SKIP_TAGS = {"script", "style", "noscript", "code", "pre", "svg", "canvas", "iframe"}


# ===== Errors =====
class RateLimitError(Exception):
    def __init__(self, retry_after: Optional[int] = None, msg: str = "rate limited"):
        super().__init__(msg)
        self.retry_after = retry_after


# ===== Simple in-memory TTL cache (avoid re-calling provider) =====
class _TTLCache:
    def __init__(self, maxsize=128, ttl=3600):
        self._maxsize = maxsize
        self._ttl = ttl
        self._store = {}
        self._lock = threading.Lock()

    def _prune(self):
        now = time.time()
        dead = [k for k, (v, t) in self._store.items() if now - t > self._ttl]
        for k in dead:
            self._store.pop(k, None)
        # size control
        if len(self._store) > self._maxsize:
            for k in list(self._store.keys())[: len(self._store) - self._maxsize]:
                self._store.pop(k, None)

    def get(self, k):
        with self._lock:
            v = self._store.get(k)
            if not v:
                return None
            val, ts = v
            if time.time() - ts > self._ttl:
                self._store.pop(k, None)
                return None
            return val

    def set(self, k, val):
        with self._lock:
            self._store[k] = (val, time.time())
            self._prune()


_CACHE = _TTLCache(maxsize=256, ttl=int(os.getenv("NER_CACHE_TTL", "7200")))


# ===== Light QPS limiter (per process) =====
class _RateLimiter:
    def __init__(self, qps: float):
        self.min_interval = 1.0 / max(qps, 0.01)
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.time()


_LIMITER = _RateLimiter(GEMINI_QPS)


def get_labels() -> list[str]:
    """
    Override via env:
      NER_LABELS="PERSON,ORG,STATUTE,CASE,CITATION,COURT,DATE,LOCATION"
    """
    raw = os.getenv("NER_LABELS")
    if raw:
        return [s.strip() for s in raw.split(",") if s.strip()]
    # sensible defaults for legal docs
    return ["PERSON", "ORG", "COURT", "CASE", "CITATION", "STATUTE", "DATE", "LOCATION"]


def clean_container_html(raw_html: str, container_selector: str = ".judgments") -> str:
    soup = BeautifulSoup(raw_html or "", "html.parser")
    container = soup.select_one(container_selector) or soup.body or soup
    for t in container.select(",".join(_SKIP_TAGS)):
        t.decompose()
    return str(container)


class GeminiNER:
    """
    Minimal, dependency-light Gemini client for NER spans over large HTML.
    - Single batched pass across all visible text nodes (chunked if needed)
    - Local QPS limit + retry with jittered backoff for 429
    - In-memory TTL cache for identical inputs
    - Optional debug introspection via last_meta()
    """

    def __init__(self, api_key: str, model: str = DEFAULT_MODEL):
        if not api_key:
            raise RuntimeError("GOOGLE_API_KEY (or compatible) is required")
        self.api_key = api_key
        self.model = model
        self.debug = NER_DEBUG
        self._last_meta: dict = {}

    # ---------- Debug/introspection ----------
    def last_meta(self) -> dict:
        """Return details of the last provider attempt (status, latency, excerpt...)."""
        return dict(self._last_meta)

    # ---------- HTML helpers ----------
    def _should_skip(self, tag: Tag) -> bool:
        return tag.name in _SKIP_TAGS

    # ---------- Chunker ----------
    def _chunks(self, text: str, max_chars: int) -> List[str]:
        text = text or ""
        if len(text) <= max_chars:
            return [text]
        parts, cur = [], 0
        while cur < len(text):
            end = min(len(text), cur + max_chars)
            slice_ = text[cur:end]
            # try to break on sentence-ish boundary
            cut = slice_.rfind(". ")
            if cut == -1 or cut < int(0.5 * len(slice_)):
                cut = len(slice_)
            parts.append(slice_[:cut])
            cur += cut
        return parts

    # ---------- Provider call with retry/backoff ----------
    def _call_gemini_json(self, prompt: str, text: str, max_retries=3) -> list:
        """
        Calls Gemini; expects a JSON array of spans:
        [{"start": int, "end": int, "label": str}, ...]
        """
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": f"{prompt}\n\n<INPUT>\n{text}\n</INPUT>"}],
                }
            ],
            "generationConfig": {"responseMimeType": "application/json"},
        }
        params = {"key": self.api_key}

        backoff = 1.5
        self._last_meta = {}

        for attempt in range(1, max_retries + 1):
            _LIMITER.wait()
            t0 = time.perf_counter()
            resp = requests.post(
                GEMINI_URL,
                headers=headers,
                params=params,
                data=json.dumps(payload),
                timeout=60,
            )
            elapsed = time.perf_counter() - t0

            # record debug info (last attempt wins)
            self._last_meta = {
                "model": self.model,
                "url": GEMINI_URL,
                "text_len": len(text),
                "attempt": attempt,
                "status": resp.status_code,
                "elapsed_sec": round(elapsed, 3),
                "retry_after": resp.headers.get("Retry-After"),
                "response_excerpt": (resp.text or "")[:300],
            }

            if resp.status_code == 200:
                try:
                    data = resp.json()
                    # API returns candidates[0].content.parts[0].text containing JSON
                    cand = (data.get("candidates") or [{}])[0]
                    part = (cand.get("content") or {}).get("parts") or []
                    text_json = part[0].get("text") if part else "[]"
                    return json.loads(text_json or "[]")
                except Exception:
                    # treat as no spans if parsing fails
                    return []

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "0") or "0")
                if self.debug:
                    print(
                        f"[GeminiNER] 429 (attempt {attempt}), retry_after={retry_after}, "
                        f"elapsed={elapsed:.2f}s"
                    )
                if attempt == max_retries:
                    raise RateLimitError(
                        retry_after=retry_after or int(math.ceil(backoff))
                    )
                # jittered backoff
                sleep_s = (retry_after or backoff) * (1.2 + 0.4 * random.random())
                time.sleep(sleep_s)
                backoff *= 2
                continue

            # other HTTP errors → try again (or raise on last)
            if self.debug and resp.status_code >= 400:
                print(
                    f"[GeminiNER] HTTP {resp.status_code} (attempt {attempt}); "
                    f"body: {(resp.text or '')[:400]}"
                )
            if attempt == max_retries:
                resp.raise_for_status()
            time.sleep(0.75 * attempt)

        return []

    def _prompt(self, labels: list[str]) -> str:
        labs = ", ".join(labels)
        return (
            "Extract entity spans from the input text. Return JSON array only, "
            'each item: {"start":int, "end":int, "label":string}. '
            f"Labels allowed: {labs}. Use character offsets over the input."
        )

    # ---------- Core: extract spans over (possibly huge) text ----------
    def extract_spans(
        self, text: str, labels: list[str], max_chars: int
    ) -> List[Tuple[int, int, str]]:
        if not text or not text.strip():
            return []

        cache_key = hashlib.sha1(
            f"{self.model}|{','.join(labels)}|{len(text)}|{hash(text)}|{max_chars}".encode()
        ).hexdigest()
        cached = _CACHE.get(cache_key)
        if cached is not None:
            return cached

        prompt = self._prompt(labels)
        spans: List[Tuple[int, int, str]] = []
        offset = 0

        for chunk in self._chunks(text, max_chars=max_chars):
            try:
                arr = self._call_gemini_json(prompt, chunk)
            except RateLimitError:
                # let caller handle 429 for cross-process/route backoff
                raise
            except requests.HTTPError:
                arr = []

            # normalize to tuples
            for it in arr or []:
                try:
                    s = int(it.get("start", 0))
                    e = int(it.get("end", 0))
                    lab = str(it.get("label", "")).upper().strip()
                except Exception:
                    continue
                if lab and e > s and s >= 0 and e <= len(chunk):
                    spans.append((s + offset, e + offset, lab))

            offset += len(chunk)

        # sort + non-overlap merge
        spans.sort(key=lambda x: (x[0], x[1]))
        merged: List[Tuple[int, int, str]] = []
        last_end = -1
        for s, e, L in spans:
            if s >= last_end:
                merged.append((s, e, L))
                last_end = e

        _CACHE.set(cache_key, merged)
        return merged

    # ---------- Batched HTML annotation (one model pass) ----------
    def annotate_html(
        self,
        html: str,
        labels: Optional[list[str]] = None,
        max_chars: int = DEFAULT_MAX_CHARS_PER_CALL,
    ) -> str:
        labels = labels or get_labels()
        if not html:
            return html

        soup = BeautifulSoup(html, "html.parser")
        text_nodes: List[NavigableString] = []
        texts: List[str] = []
        offsets: List[Tuple[int, int]] = []

        # delimiter between concatenated nodes; use a token unlikely to appear
        delim = "\n¶\n"
        dlen = len(delim)

        cursor = 0
        for tn in list(soup.find_all(string=True)):
            parent = tn.parent
            if not isinstance(parent, Tag) or self._should_skip(parent):
                continue
            # skip if we're already inside an annotated ner span
            if "ner" in (parent.get("class") or []):
                continue
            s = str(tn)
            if len(s.strip()) < 2:
                continue
            text_nodes.append(tn)
            texts.append(s)
            start = cursor
            end = start + len(s)
            offsets.append((start, end))
            cursor = end + dlen

        if not text_nodes:
            return str(soup)

        big_text = delim.join(texts)
        spans = self.extract_spans(big_text, labels=labels, max_chars=max_chars)

        # project global spans back to each node
        for idx, tn in enumerate(text_nodes):
            nstart, nend = offsets[idx]
            ntext = str(tn)

            node_spans: List[Tuple[int, int, str]] = []
            for s, e, L in spans:
                if e <= nstart or s >= nend:
                    continue
                ls = max(0, s - nstart)
                le = min(len(ntext), e - nstart)
                if ls < le:
                    node_spans.append((ls, le, L))

            if not node_spans:
                continue

            node_spans.sort(key=lambda x: (x[0], x[1]))
            merged: List[Tuple[int, int, str]] = []
            last_end = -1
            for s2, e2, L2 in node_spans:
                if s2 >= last_end:
                    merged.append((s2, e2, L2))
                    last_end = e2

            frags: List[NavigableString | Tag] = []
            cur = 0
            for s2, e2, L2 in merged:
                if s2 > cur:
                    frags.append(NavigableString(ntext[cur:s2]))
                span_tag = soup.new_tag("span")
                span_tag["class"] = ["ner", f"ner-{L2}"]
                span_tag["data-entity"] = L2
                span_tag.string = ntext[s2:e2]
                frags.append(span_tag)
                cur = e2
            if cur < len(ntext):
                frags.append(NavigableString(ntext[cur:]))

            tn.replace_with(*frags)

        return str(soup)

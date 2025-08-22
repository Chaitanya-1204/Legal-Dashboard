# # opennyai_html_ner.py
# from __future__ import annotations
# from typing import Iterable, List, Tuple
# from bs4 import BeautifulSoup, NavigableString, Tag

# from opennyai import Pipeline
# from opennyai.utils import Data

# # Tags we should not touch
# _SKIP = {"script", "style", "noscript", "code", "pre", "svg", "canvas", "iframe"}

# class OpenNyAIHtmlNER:
#     """
#     Wraps OpenNyAI NER pipeline and injects spans into HTML text nodes.
#     Uses official params from docs: sentence-level + postprocess enabled.
#     """
#     def __init__(
#         self,
#         use_gpu: bool = False,
#         model_name: str = "en_legal_ner_trf",
#         ner_mini_batch_size: int = 40000,
#         verbose: bool = False,
#     ):
#         self.pipeline = Pipeline(
#             components=["NER"],
#             use_gpu=use_gpu,
#             verbose=verbose,
#             ner_model_name=model_name,
#             ner_mini_batch_size=ner_mini_batch_size,
#             ner_do_sentence_level=True,
#             ner_do_postprocess=True,
#             ner_statute_shortforms_path="",
#         )

#     def _ents_for_text(self, text: str) -> List[Tuple[int, int, str]]:
#         """
#         Run NER on one string and return (start, end, label) spans.
#         OpenNyAI returns a spaCy Doc in pipeline._ner_model_output.
#         """
#         if not text or not text.strip():
#             return []

#         _ = self.pipeline(Data([text]))  # triggers NER
#         doc = self.pipeline._ner_model_output[0]  # spaCy Doc
#         # Ensure sorted, non-overlapping spans
#         ents = sorted([(e.start_char, e.end_char, e.label_) for e in doc.ents], key=lambda x: (x[0], x[1]))
#         # Remove overlaps just in case
#         merged = []
#         last_end = -1
#         for s, e, L in ents:
#             if s >= last_end:
#                 merged.append((s, e, L))
#                 last_end = e
#         return merged

#     def annotate_html(self, html: str, skip_tags: Iterable[str] = _SKIP) -> str:
#         """
#         Parse HTML, run NER on visible text nodes, wrap with:
#           <span class="ner ner-<LABEL>" data-entity="<LABEL>">...</span>
#         """
#         soup = BeautifulSoup(html, "html.parser")

#         def should_skip(tag: Tag) -> bool:
#             return tag.name in skip_tags

#         for text_node in list(soup.find_all(string=True)):
#             parent = text_node.parent
#             if not isinstance(parent, Tag) or should_skip(parent):
#                 continue

#             text = str(text_node)
#             if len(text.strip()) < 2:
#                 continue

#             spans = self._ents_for_text(text)
#             if not spans:
#                 continue

#             # Rebuild fragments with wrappers
#             frags: List = []
#             cur = 0
#             for start, end, label in spans:
#                 if start > cur:
#                     frags.append(NavigableString(text[cur:start]))
#                 span = soup.new_tag("span")
#                 span["class"] = ["ner", f"ner-{label}"]
#                 span["data-entity"] = label
#                 span.string = text[start:end]
#                 frags.append(span)
#                 cur = end
#             if cur < len(text):
#                 frags.append(NavigableString(text[cur:]))

#             text_node.replace_with(*frags)

#         return str(soup)


# # Convenience helpers

# def annotate_one_html(html: str, **ner_kwargs) -> str:
#     """
#     One-shot annotate a single HTML string.
#     Example: annotate_one_html(raw_html, use_gpu=False, model_name="en_legal_ner_trf")
#     """
#     engine = OpenNyAIHtmlNER(**ner_kwargs)
#     return engine.annotate_html(html)


# def annotate_many_files(paths: List[str], output_dir: str, **ner_kwargs) -> List[str]:
#     """
#     Read multiple HTML files, write annotated versions to output_dir.
#     Returns list of output file paths.
#     """
#     import os
#     os.makedirs(output_dir, exist_ok=True)
#     engine = OpenNyAIHtmlNER(**ner_kwargs)
#     out_paths: List[str] = []
#     for p in paths:
#         with open(p, "r", encoding="utf-8") as f:
#             html = f.read()
#         annotated = engine.annotate_html(html)
#         out_p = os.path.join(output_dir, os.path.basename(p))
#         with open(out_p, "w", encoding="utf-8") as f:
#             f.write(annotated)
#         out_paths.append(out_p)
#     return out_paths


# # opennyai_html_ner.py
# from __future__ import annotations
# from typing import Iterable, List, Tuple
# from bs4 import BeautifulSoup, NavigableString, Tag

# _SKIP = {"script", "style", "noscript", "code", "pre", "svg", "canvas", "iframe"}
# opennyai_html_ner.py (replace the class with this)
from __future__ import annotations
from typing import Iterable, List, Tuple
from bs4 import BeautifulSoup, NavigableString, Tag

_SKIP = {"script", "style", "noscript", "code", "pre", "svg", "canvas", "iframe"}

class OpenNyAIHtmlNER:
    """
    Prefer direct spaCy load of the legal transformer model to avoid OpenNyAI's
    internal postprocess E030. Falls back to OpenNyAI pipeline if needed.
    """

    def __init__(
        self,
        use_gpu: bool = False,
        model_name: str = "en_legal_ner_trf",
        ner_mini_batch_size: int = 40000,
        verbose: bool = False,
        do_sentence_level: bool = True,
        do_postprocess: bool = False,   # default OFF to avoid E030 if we ever fall back
        prefer_spacy_direct: bool = True,
    ):
        self._Data = None
        self.pipeline = None     # OpenNyAI pipeline (fallback)
        self._nlp = None         # spaCy model (preferred)
        self._mode = None        # "spacy" or "opennyai"
        self._init_pipeline(use_gpu, model_name, ner_mini_batch_size, verbose,
                            do_sentence_level, do_postprocess, prefer_spacy_direct)

    def _ensure_sentencizer(self, nlp):
        try:
            pn = set(nlp.pipe_names)
            if not ({"parser", "senter", "sentencizer"} & pn):
                nlp.add_pipe("sentencizer")
                print("[NER] Added 'sentencizer' to spaCy pipeline.")
        except Exception as e:
            print(f"[NER] Could not add sentencizer: {e}")

    def _init_pipeline(self, use_gpu, model_name, ner_mini_batch_size, verbose,
                       do_sentence_level, do_postprocess, prefer_spacy_direct):
        # 1) Prefer spaCy direct load of the legal model
        if prefer_spacy_direct:
            try:
                import spacy
                self._nlp = spacy.load(model_name)  # e.g., "en_legal_ner_trf"
                self._ensure_sentencizer(self._nlp)
                self._mode = "spacy"
                print(f"[NER] Using spaCy model directly: {model_name}")
                return
            except Exception as e:
                print(f"[NER] spaCy direct load failed, will try OpenNyAI: {e}")

        # 2) Fallback: OpenNyAI pipeline, with postprocess OFF to avoid E030
        try:
            from opennyai import Pipeline
            from opennyai.utils import Data
            self._Data = Data
            self.pipeline = Pipeline(
                components=["NER"],
                use_gpu=use_gpu,
                verbose=verbose,
                ner_model_name=model_name,
                ner_mini_batch_size=ner_mini_batch_size,
                ner_do_sentence_level=do_sentence_level,
                ner_do_postprocess=False,          # force OFF
                ner_statute_shortforms_path="",
            )
            # Try to find internal nlp and ensure sentencizer anyway
            for attr in ("_ner_nlp", "_nlp", "nlp"):
                nlp = getattr(self.pipeline, attr, None)
                if nlp is not None:
                    self._ensure_sentencizer(nlp)
                    break
            self._mode = "opennyai"
            print(f"[NER] Using OpenNyAI Pipeline (postprocess OFF): {model_name}")
        except Exception as e:
            # Last resort: disable
            self.pipeline = None
            self._nlp = None
            self._mode = None
            print(f"[NER] Failed to initialize any model. NER disabled. Cause: {e}")

    def _ents_for_text(self, text: str) -> List[Tuple[int, int, str]]:
        if not text or not text.strip():
            return []
        if self._mode == "spacy" and self._nlp:
            doc = self._nlp(text)
        elif self._mode == "opennyai" and self.pipeline:
            _ = self.pipeline(self._Data([text]))
            doc = self.pipeline._ner_model_output[0]  # spaCy Doc
        else:
            return []
        ents = sorted([(e.start_char, e.end_char, e.label_) for e in doc.ents],
                      key=lambda x: (x[0], x[1]))
        merged, last_end = [], -1
        for s, e, L in ents:
            if s >= last_end:
                merged.append((s, e, L))
                last_end = e
        return merged

    def annotate_html(self, html: str, skip_tags: Iterable[str] = _SKIP) -> str:
        if not html or (self._mode is None):
            return html
        soup = BeautifulSoup(html, "html.parser")

        def should_skip(tag: Tag) -> bool:
            return tag.name in skip_tags

        for text_node in list(soup.find_all(string=True)):
            parent = text_node.parent
            if not isinstance(parent, Tag) or should_skip(parent):
                continue
            text = str(text_node)
            if len(text.strip()) < 2:
                continue

            spans = self._ents_for_text(text)
            if not spans:
                continue

            frags: List = []
            cur = 0
            for start, end, label in spans:
                if start > cur:
                    frags.append(NavigableString(text[cur:start]))
                span = soup.new_tag("span")
                span["class"] = ["ner", f"ner-{label}"]
                span["data-entity"] = label
                span.string = text[start:end]
                frags.append(span)
                cur = end
            if cur < len(text):
                frags.append(NavigableString(text[cur:]))

            text_node.replace_with(*frags)

        return str(soup)

# convenience helpers unchanged…
def annotate_one_html(html: str, **ner_kwargs) -> str:
    engine = OpenNyAIHtmlNER(**ner_kwargs)
    return engine.annotate_html(html)

def annotate_many_files(paths: List[str], output_dir: str, **ner_kwargs) -> List[str]:
    import os
    os.makedirs(output_dir, exist_ok=True)
    engine = OpenNyAIHtmlNER(**ner_kwargs)
    out_paths: List[str] = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            html = f.read()
        annotated = engine.annotate_html(html)
        out_p = os.path.join(output_dir, os.path.basename(p))
        with open(out_p, "w", encoding="utf-8") as f:
            f.write(annotated)
        out_paths.append(out_p)
    return out_paths

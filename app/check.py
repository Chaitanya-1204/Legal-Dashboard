#!/usr/bin/env python3
# check_versions.py — print versions and paths of key libs

import importlib
import sys

PKGS = [
    ("numpy", "numpy"),
    ("scipy", "scipy"),
    ("scikit-learn", "sklearn"),
    ("spaCy", "spacy"),
    ("thinc", "thinc"),
    ("transformers", "transformers"),
    ("tokenizers", "tokenizers"),
    ("torch", "torch"),
    ("pydantic", "pydantic"),
    ("opennyai", "opennyai"),
    ("google-genai", "google.genai"),  # will say "not installed" if absent
]

print(f"Python: {sys.version.split()[0]}  @ {sys.executable}\n")
print("{:<15} {:<14} {}".format("Package", "Version", "Location"))
print("-" * 80)

for label, modname in PKGS:
    try:
        m = importlib.import_module(modname)
        ver = getattr(m, "__version__", "unknown")
        path = getattr(m, "__file__", "<built-in>")
        print(f"{label:<15} {ver:<14} {path}")
    except Exception as e:
        print(f"{label:<15} {'not installed':<14} ({e.__class__.__name__}: {e})")

# Simple heads-up if NumPy is 2.x (common ABI mismatch trigger)
try:
    import numpy as _np
    major = int(_np.__version__.split(".")[0])
    if major >= 2:
        print("\nNOTE: NumPy 2.x detected; older SciPy/spaCy/thinc wheels built for 1.x may fail.")
except Exception:
    pass

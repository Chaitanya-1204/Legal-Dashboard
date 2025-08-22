# app/judgments/__init__.py
from flask import Blueprint
import os

# Allow overriding collection name via env var if needed
SC_COLLECTION = os.getenv("SC_COLLECTION", "judgments")

# Mirror acts blueprint style: templates live in ../templates
judgments_bp = Blueprint(
    'judgments',
    __name__,
    template_folder='../templates'
)

# Import routes to attach them to the blueprint
from . import routes  # noqa: E402,F401

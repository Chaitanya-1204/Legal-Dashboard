from flask import Blueprint

# Blueprint lives under /summary when registered
summary_bp = Blueprint("summary", __name__, template_folder="templates")

# Import routes so endpoints register on blueprint
from . import routes  # noqa: E402,F401

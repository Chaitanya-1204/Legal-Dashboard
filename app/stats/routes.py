# app/stats/routes.py
from flask import Blueprint, render_template

# Create a Blueprint for the statistics dashboard
stats_bp = Blueprint('stats', __name__, template_folder='../templates')

@stats_bp.route('/stats')
def show_stats():
    """
    Renders the statistics dashboard page.
    """
    return render_template('stats.html')

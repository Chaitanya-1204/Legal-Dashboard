# app/acts/routes.py
# This file contains all the routes and logic for the 'Acts' section.

from flask import Blueprint, render_template, abort
from app import db # Import the shared db connection from the app factory
from urllib.parse import unquote

# Create a Blueprint.
acts_bp = Blueprint('acts', __name__, template_folder='../templates')

@acts_bp.route('/')
def show_categories():
    """
    Displays act categories grouped by their law_type (e.g., Central Acts, State Acts).
    """
    try:
        # This is a MongoDB Aggregation Pipeline. It's a powerful way to process data.
        pipeline = [
            {
                # Stage 1: Group all documents by the 'law_type' field.
                "$group": {
                    "_id": "$law_type",
                    # For each group, create a list of unique 'category' names.
                    "categories": {"$addToSet": "$category"} 
                }
            },
            {
                # Stage 2: Sort the groups by their name (e.g., Central, State).
                "$sort": {"_id": 1}
            }
        ]
        # Execute the pipeline and convert the result to a list.
        grouped_categories = list(db.acts.aggregate(pipeline))
        
        # The result is a list of dictionaries, like:
        # [{'id': 'Central Acts', 'categories': [...]}, {'id': 'State Acts', 'categories': [...]}]

        # As a final touch, sort the categories alphabetically within each group.
        for group in grouped_categories:
            group['categories'].sort()

        return render_template('acts_categories.html', grouped_data=grouped_categories)
    except Exception as e:
        print(f"Database error in show_categories: {e}")
        abort(500)

@acts_bp.route('/<category_name>/')
def show_years(category_name):
    """
    For a given category, displays a list of all unique years available.
    """
    # URL quoting can sometimes replace spaces with %20, etc. Unquote cleans it up.
    category_name = unquote(category_name)
    try:
        # Find all unique years for documents that match the given category.
        # Years are now sorted in ascending order (removed reverse=True).
        years = sorted(db.acts.distinct("year", {"category": category_name}))
        if not years:
            # If the category name doesn't exist, it's a 404 error.
            abort(404)
        return render_template('acts_years.html', years=years, category_name=category_name)
    except Exception as e:
        print(f"Database error in show_years: {e}")
        abort(500)

@acts_bp.route('/<category_name>/<int:year>/')
def list_acts_by_year(category_name, year):
    """
    For a given category and year, lists all the acts.
    """
    category_name = unquote(category_name)
    try:
        acts = list(db.acts.find(
            {"category": category_name, "year": year},
            {'title': 1, 'doc_id': 1, '_id': 0}
        ))
        if not acts:
            abort(404)
        return render_template('acts_list.html', acts=acts, category_name=category_name, year=year)
    except Exception as e:
        print(f"Database error in list_acts_by_year: {e}")
        abort(500)


@acts_bp.route('/view/<int:doc_id>')
def view_act(doc_id):
    """
    Renders a page displaying the full content of a single act.
    """
    act = db.acts.find_one({'doc_id': doc_id}, {'_id': 0})
    if not act:
        abort(404)
    return render_template('view_act.html', act=act)

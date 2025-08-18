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
    Displays act categories grouped by their law_type, with a custom sort order.
    """
    try:
        # This MongoDB Aggregation Pipeline processes data to achieve the desired grouping and sorting.
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
                # Stage 2: Add a temporary field to define a custom sort order.
                # Lower numbers will appear first.
                "$addFields": {
                    "sortOrder": {
                        "$switch": {
                            "branches": [
                                # Assign 1 to 'Central Acts' to make it appear first.
                                {"case": {"$eq": ["$_id", "Central Acts"]}, "then": 1},
                                # Assign 2 to 'State Acts' to make it appear second.
                                {"case": {"$eq": ["$_id", "State Acts"]}, "then": 2},
                                # Assign 3 to any law type containing "British" for third position.
                                {"case": {"$regexMatch": {"input": "$_id", "regex": "British India"}}, "then": 3}
                            ],
                            # All other law types will get a default order of 4.
                            "default": 4 
                        }
                    }
                }
            },
            {
                # Stage 3: Sort the groups first by our custom 'sortOrder' field,
                # and then alphabetically by name for any ties.
                "$sort": {
                    "sortOrder": 1,
                    "_id": 1
                }
            }
        ]
        # Execute the pipeline and convert the result to a list.
        grouped_categories = list(db.acts.aggregate(pipeline))
        
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
            {'full_title': 1, 'doc_id': 1, '_id': 0}
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

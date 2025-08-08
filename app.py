from flask import Flask, render_template
import datetime
import pandas as pd
import os
import ast
from bs4 import BeautifulSoup
import re

# Initialize the Flask application
app = Flask(__name__)

# --- Data for the App ---

# Define the root path for your data files
DATA_ROOT = "/DATACHAI/Data/DATA/laws"

# Construct the path to the CSV file
csv_file_path = os.path.join('csv_files', 'categories_and_folders.csv')
doc_links_csv_path = os.path.join('csv_files', 'doc_links.csv')


# Read the CSV file into a DataFrame
try:
    categories_df = pd.read_csv(csv_file_path)
    doc_links_df = pd.read_csv(doc_links_csv_path)
    
    # Define the desired order for the law types
    law_type_order = ['Central Acts', 'State Acts', 'British India']
    
    # Convert the 'law_type' column to a special 'Categorical' type.
    categories_df['law_type'] = pd.Categorical(categories_df['law_type'], categories=law_type_order, ordered=True)
    
    # Sort the DataFrame first by our custom law_type order, 
    # and then alphabetically by the category_name within each type.
    categories_df = categories_df.sort_values(by=['law_type', 'category_name'])

except FileNotFoundError as e:
    print(f"Error: {e}")
    categories_df = pd.DataFrame(columns=['category_name', 'folder_name', 'years_available', 'law_type'])
    doc_links_df = pd.DataFrame(columns=['doc_id', 'parent_doc_id', 'doc_type', 'path'])


# --- Routes ---

@app.route('/')
def index():
    """Renders the main dashboard page."""
    return render_template('index.html', active_page='home')

@app.route('/stats')
def stats():
    """Renders the placeholder page for Statistics."""
    return render_template('placeholder.html', title="Statistics", active_page='stats')


# --- Acts Routes ---
@app.route('/acts')
def acts():
    """Renders the main selection page for Acts, categorized."""
    acts_by_law_type = categories_df.groupby('law_type', sort=False)
    return render_template('acts.html', acts_by_law_type=acts_by_law_type, active_page='home')

@app.route('/acts/<law_name_folder>')
def act_years(law_name_folder):
    """Renders the year selection page for a specific act."""
    law_details = categories_df.loc[categories_df['folder_name'] == law_name_folder].iloc[0]
    years_str = law_details['years_available']
    law_type = law_details['law_type']
    
    try:
        years = ast.literal_eval(years_str)
    except (ValueError, SyntaxError):
        years = []

    return render_template('act_years.html', law_type=law_type, law_name=law_details['category_name'], law_name_folder=law_name_folder, years=years, active_page='home')

@app.route('/acts/<law_name_folder>/<int:year>')
def act_year_details(law_name_folder, year):
    """Lists all the HTML files for a given act and year."""
    law_details = categories_df.loc[categories_df['folder_name'] == law_name_folder].iloc[0]
    year_path = os.path.join(DATA_ROOT, law_name_folder, str(year))
    
    try:
        files = [f for f in os.listdir(year_path) if f.endswith('.html')]
    except FileNotFoundError:
        files = []

    return render_template('act_files.html', law_name=law_details['category_name'], year=year, law_name_folder=law_name_folder, files=files)

@app.route('/acts/<law_name_folder>/<int:year>/<filename>')
def act_file_content(law_name_folder, year, filename):
    """Displays the content of a specific act's HTML file."""
    full_path = os.path.join(DATA_ROOT, law_name_folder, str(year), filename)
    
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Remove square brackets from the content
        content = content.replace('[', '').replace(']', '')
    except FileNotFoundError:
        content = "File not found."
    except Exception as e:
        content = f"An error occurred: {e}"

    # We can extract a title from the filename
    title = os.path.basename(filename)
    return render_template('act_content.html', title=title, content=content)

# --- MODIFIED ROUTE for handling internal doc links ---
@app.route('/doc/<int:doc_id>/')
def show_doc_section(doc_id):
    """
    Finds a specific doc ID, extracts the parent section of that link,
    and displays it with a contextual title.
    """
    try:
        # Find the row in the doc_links DataFrame that matches the doc_id
        link_info = doc_links_df[doc_links_df['doc_id'] == doc_id].iloc[0]
        file_path = link_info['path']
        
        # Get the main act's title from the categories dataframe
        try:
            parent_folder_name = os.path.basename(os.path.dirname(os.path.dirname(file_path)))
            act_title = categories_df[categories_df['folder_name'] == parent_folder_name]['category_name'].iloc[0]
        except Exception:
            act_title = "the Act" # Fallback title

        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, 'html.parser')
        anchor_tag = soup.find('a', href=re.compile(f'^/doc/{doc_id}/?$'))
        
        content_to_display = ""
        page_type = "Section"
        full_title = f"Details for Doc ID {doc_id}"

        if anchor_tag:
            parent_section = anchor_tag.find_parent('section')
            
            if parent_section:
                section_id = parent_section.get('id', '') # e.g., 'section_6.b'
                id_parts = []
                if section_id.startswith('section_'):
                    id_parts = section_id.replace('section_', '').split('.')

                # Construct title based on ID parts
                if len(id_parts) > 0:
                    main_num = id_parts[0]
                    sub_num = f"({id_parts[1]})" if len(id_parts) > 1 else ""
                    
                    if "Constitution" in act_title:
                        page_type = "Constitution Article" if not sub_num else "Constitution Sub-article"
                        full_title = f"Article {main_num}{sub_num} in {act_title}"
                    else:
                        page_type = "Act Section" if not sub_num else "Act Sub-section"
                        full_title = f"Section {main_num}{sub_num} in {act_title}"
                
                # Get the content to display
                content_to_display = str(parent_section).replace('[', '').replace(']', '')
            else:
                # Fallback if no parent section is found
                parent_content = anchor_tag.parent
                content_to_display = str(parent_content).replace('[', '').replace(']', '')
        else:
            content_to_display = f"<p>Error: Could not find the link for doc id {doc_id} in the file.</p>"

    except IndexError:
        content_to_display = f"<p>Error: No information found for doc id {doc_id}.</p>"
    except FileNotFoundError:
        content_to_display = f"<p>Error: The file specified for doc id {doc_id} could not be found.</p>"
    except Exception as e:
        content_to_display = f"<p>An unexpected error occurred: {e}</p>"

    # Use a new template for this view
    return render_template('section_display.html', page_type=page_type, full_title=full_title, content=content_to_display)


# List of High Courts
high_courts_list = [
    "Allahabad High Court", "Bombay High Court", "Calcutta High Court",
    "Gauhati High Court", "High Court for State of Telangana", "High Court of Andhra Pradesh",
    "High Court of Chhattisgarh", "High Court of Delhi", "High Court of Gujarat",
    "High Court of Himachal Pradesh", "High Court of Jammu and Kashmir", "High Court of Jharkhand",
    "High Court of Karnataka", "High Court of Kerala", "High Court of Madhya Pradesh",
    "High Court of Manipur", "High Court of Meghalaya", "High Court of Orissa",
    "High Court of Punjab and Haryana", "High Court of Rajasthan", "High Court of Sikkim",
    "High Court of Tripura", "High Court of Uttarakhand", "Madras High Court", "Patna High Court"
]

# List of Tribunals
tribunals_list = [
    "Appellate Tribunal For Electricity", "Authority Tribunal", "Central Administrative Tribunal",
    "Customs, Excise and Gold Tribunal", "Central Electricity Regulatory Commission", "Central Information Commission",
    "Company Law Board", "Consumer Disputes Redressal", "Copyright Board",
    "Debt Recovery Appellate Tribunal", "National Green Tribunal", "Competition Commission of India",
    "Intellectual Property Appellate Board", "Income Tax Appellate Tribunal", "Monopolies and Restrictive Trade Practices Commission",
    "Securities Appellate Tribunal", "State Taxation Tribunal", "Telecom Disputes Settlement Tribunal",
    "Trademark Tribunal", "Custom, Excise & Service Tax Tribunal", "National Company Law Appellate Tribunal"
]

# List of District Courts
district_courts_list = [
    "Bangalore District Court", "Delhi District Court"
]


# --- Supreme Court Routes ---
@app.route('/supreme')
def supreme_court():
    current_year = datetime.datetime.now().year
    end_year = max(current_year, 2025)
    years = range(1950, end_year + 1)
    return render_template('supreme_court.html', years=years, active_page='home')

@app.route('/supreme/<int:year>')
def supreme_court_year(year):
    return render_template('placeholder.html', title=f"Supreme Court Judgments - {year}")

# --- High Court Routes ---
@app.route('/highcourt')
def high_court():
    return render_template('high_court.html', courts=high_courts_list, active_page='home')

@app.route('/highcourt/<court_name>')
def high_court_judgments(court_name):
    current_year = datetime.datetime.now().year
    end_year = max(current_year, 2025)
    years = range(1950, end_year + 1)
    return render_template('high_court_years.html', court_name=court_name, years=years, active_page='home')

@app.route('/highcourt/<court_name>/<int:year>')
def high_court_year_judgments(court_name, year):
    return render_template('placeholder.html', title=f"{court_name} - {year} Judgments")

# --- Tribunals Routes ---
@app.route('/tribunals')
def tribunals():
    return render_template('tribunals.html', tribunals=tribunals_list, active_page='home')

@app.route('/tribunals/<tribunal_name>')
def tribunal_judgments(tribunal_name):
    current_year = datetime.datetime.now().year
    end_year = max(current_year, 2025)
    years = range(1950, end_year + 1)
    return render_template('tribunal_years.html', tribunal_name=tribunal_name, years=years, active_page='home')

@app.route('/tribunals/<tribunal_name>/<int:year>')
def tribunal_year_judgments(tribunal_name, year):
    return render_template('placeholder.html', title=f"{tribunal_name} - {year} Judgments")

# --- District Court Routes ---
@app.route('/district-courts')
def district_courts():
    """Renders the selection page for District Courts."""
    return render_template('district_courts.html', courts=district_courts_list, active_page='home')

@app.route('/district-courts/<court_name>')
def district_court_judgments(court_name):
    """Renders the year selection page for a specific District Court."""
    current_year = datetime.datetime.now().year
    end_year = max(current_year, 2025)
    years = range(1950, end_year + 1)
    return render_template('district_court_years.html', court_name=court_name, years=years, active_page='home')

@app.route('/district-courts/<court_name>/<int:year>')
def district_court_year_judgments(court_name, year):
    """Renders a placeholder for a specific District Court's judgments for a specific year."""
    return render_template('placeholder.html', title=f"{court_name} - {year} Judgments")


# --- Other Main Routes ---
@app.route('/blogs')
def blogs():
    return render_template('placeholder.html', title="Blogs")

@app.route('/books')
def books():
    return render_template('placeholder.html', title="Books")


if __name__ == '__main__':
    app.run(debug=True)

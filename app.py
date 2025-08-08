from flask import Flask, render_template
import datetime

# Initialize the Flask application
app = Flask(__name__)

# --- Data for the App ---

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

# Data for Acts, categorized
acts_data = {
    "Central Laws": [
        "Union of India - Act", "International Treaty - Act", "Constitution and Amendments", "United Nations Conventions"
    ],
    "State Laws": [
        "State of Andhra Pradesh - Act", "State of Arunachal Pradesh - Act", "State of Assam - Act", "State of Bihar - Act",
        "UT Chandigarh - Act", "State of Goa - Act", "NCT Delhi - Act", "State of Gujarat - Act", "State of Haryana - Act",
        "State of Himachal Pradesh - Act", "State of Jammu-Kashmir - Act", "State of Jharkhand - Act", "State of Karnataka - Act",
        "State of Kerala - Act", "State of Madhya Bharat - Act", "State of Madhya Pradesh - Act", "State of Maharashtra - Act",
        "State of Manipur - Act", "State of Meghalaya - Act", "State of Mizoram - Act", "State of Nagaland - Act",
        "State of Odisha - Act", "State of Puducherry - Act", "State of Punjab - Act", "State of Rajasthan - Act",
        "State of Sikkim - Act", "State of Tamilnadu- Act", "State of Telangana - Act", "State of Tripura - Act",
        "State of Uttarakhand - Act", "State of Uttar Pradesh - Act", "State of West Bengal - Act", "Lakshadweep - Act",
        "Andaman and Nicobar Islands - Act", "Greater Bengaluru City Corporation - Act", "UT Ladakh - Act",
        "Daman and Diu - Act", "Dadra And Nagar Haveli - Act"
    ],
    "British India (Historical)": [
        "British India - Act", "Bhopal State - Act", "Bombay Presidency - Act", "Madras Presidency - Act",
        "Central Provinces And Berar - Act", "Bengal Presidency - Act", "Chota Nagpur Division - Act", "Mysore State - Act",
        "Nagpur Province - Act", "Punjab Province - Act", "United Province - Act", "Vindhya Province - Act"
    ]
}

# List of District Courts
district_courts_list = [
    "Bangalore District Court", "Delhi District Court"
]


# --- Routes ---

@app.route('/')
def index():
    """Renders the main dashboard page."""
    return render_template('index.html', active_page='home')

@app.route('/stats')
def stats():
    """Renders the placeholder page for Statistics."""
    return render_template('placeholder.html', title="Statistics", active_page='stats')

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

# --- Acts Routes ---
@app.route('/acts')
def acts():
    """Renders the main selection page for Acts, categorized."""
    return render_template('acts.html', acts_data=acts_data, active_page='home')

@app.route('/acts/<law_type>/<law_name>')
def act_years(law_type, law_name):
    """Renders the year selection page for a specific act."""
    current_year = datetime.datetime.now().year
    end_year = max(current_year, 2025)
    years = range(1950, end_year + 1)
    return render_template('act_years.html', law_type=law_type, law_name=law_name, years=years, active_page='home')

@app.route('/acts/<law_type>/<law_name>/<int:year>')
def act_year_details(law_type, law_name, year):
    """Renders a placeholder for a specific act and year."""
    return render_template('placeholder.html', title=f"{law_name} ({law_type}) - {year}")

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

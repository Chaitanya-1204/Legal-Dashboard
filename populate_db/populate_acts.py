# main_populate_script.py
import os
import csv
import re
from pymongo import MongoClient
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm

# --- CONFIGURATION ---
# Update these variables to match your setup
METADATA_CSV_PATH = '/DATACHAI/Final_Data/laws/metadata.csv'
LAW_TYPE_CSV_PATH = '/DATACHAI/Final_Data/laws/categories_and_folders.csv'
MAIN_DOCUMENTS_FOLDER = '/DATACHAI/Final_Data/laws'
LOG_FILE_NAME = 'processing_issues.log'
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "legal_dashboard_db"
COLLECTION_NAME = "acts"

# --- HELPER FUNCTIONS ---

def load_law_type_mapping(filepath):
    """Reads the law type CSV and returns a dictionary for easy lookup."""
    mapping = {}
    print(f"📘 Loading law type mapping from '{filepath}'...")
    try:
        with open(filepath, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                mapping[row['category_name']] = row['law_type']
        print(f"   -> Successfully loaded {len(mapping)} category-to-law-type mappings.")
        return mapping
    except FileNotFoundError:
        print(f"❌ Critical Error: Law type mapping file not found at '{filepath}'.")
        return None
    except Exception as e:
        print(f"❌ Error reading law type mapping file: {e}")
        return None

def setup_log_file():
    """Creates and prepares the log file for the current run."""
    try:
        with open(LOG_FILE_NAME, 'w', encoding='utf-8') as log_file:
            log_file.write(f"--- Data Population Log ---\n")
            log_file.write(f"Run started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file.write("-" * 30 + "\n\n")
        print(f"📝 Log file '{LOG_FILE_NAME}' created for processing issues.")
    except Exception as e:
        print(f"❌ Could not create log file: {e}")

def log_issue(issue_type, detail):
    """Appends an issue to the log file."""
    try:
        with open(LOG_FILE_NAME, 'a', encoding='utf-8') as log_file:
            log_file.write(f"[{issue_type.upper()}]: {detail}\n")
    except Exception as e:
        print(f"   ❌ Error writing to log file: {e}")

def get_all_html_files(root_folder):
    """Walks through a directory and returns a set of all .html filenames."""
    html_files = set()
    print(f"🔍 Scanning '{root_folder}' for all .html files...")
    for dirpath, _, filenames in os.walk(root_folder):
        for f in filenames:
            if f.endswith('.html'):
                html_files.add(f)
    print(f"   -> Found {len(html_files)} total .html files on disk.")
    return html_files

def calculate_word_count(html_string):
    """Removes HTML tags and returns the word count of the plain text."""
    plain_text = re.sub('<[^<]+?>', ' ', html_string)
    words = plain_text.split()
    return len(words)


def clean_html_content(html_content):
    """
    Cleans HTML using BeautifulSoup:
    - Removes <span class="akn-remark">
    - Removes <span> wrapping <a class="akn-ref">
    - Removes <span class="akn-p">References</span>
    - Removes <span class="akn-p">[* * *]</span> and similar
    - Cleans text like "*** [Mizoram;]" → "Mizoram;"
    - Removes bracket-only <span class="akn-p"> like '[', ']', ']', ']}}'
    - Strips trailing hyphens from all text nodes
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # 1. Remove <span class="akn-remark">
    for span in soup.find_all('span', class_='akn-remark'):
        span.decompose()

    # 2. Remove <span> containing <a class="akn-ref">
    for a in soup.find_all('a', class_='akn-ref'):
        if a.parent and a.parent.name == 'span':
            a.parent.decompose()

    # 3. Remove <span class="akn-p">References</span>
    for span in soup.find_all('span', string='References'):
        if 'akn-p' in (span.get('class') or []):
            span.decompose()

    # 4. Remove <span class="akn-p"> containing only brackets/braces/asterisks (e.g., [* * *], ], }})
    junk_pattern = re.compile(r'^[\*\[\]\{\}\s]+$')
    for span in soup.find_all('span', class_='akn-p'):
        if junk_pattern.match(span.get_text(strip=True)):
            span.decompose()

    # 5. Fix spans like: "*** [Mizoram;]" → "Mizoram;"
    cleanup_star_bracket_pattern = re.compile(r'^\*+\s*\[([^\]]+)\]$')
    for span in soup.find_all('span', class_='akn-p'):
        text = span.get_text(strip=True)
        match = cleanup_star_bracket_pattern.match(text)
        if match:
            cleaned = match.group(1).strip()
            span.string = cleaned

    # 6. Remove trailing hyphens from all string text
    for text_node in soup.find_all(string=True):
        if text_node.strip().endswith('-'):
            cleaned = re.sub(r'[\u2010-\u2015\-]+$', '', text_node.strip())
            text_node.replace_with(cleaned)

    return str(soup)




# --- MAIN SCRIPT LOGIC ---

def populate_acts_collection():
    """
    Connects to MongoDB, cleans HTML content, populates the database,
    and verifies file coverage.
    """
    print("--- Starting MongoDB Population Script ---")
    setup_log_file()

    law_type_map = load_law_type_mapping(LAW_TYPE_CSV_PATH)
    if law_type_map is None:
        print("Aborting script due to missing law type mapping.")
        return

    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        acts_collection = db[COLLECTION_NAME]
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB.")
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        return

    try:
        delete_result = acts_collection.delete_many({})
        print(f"🧹 Cleared existing data. {delete_result.deleted_count} documents removed from '{COLLECTION_NAME}'.")
    except Exception as e:
        print(f"❌ Error clearing collection: {e}")
        client.close()
        return

    print(f"📂 Reading metadata from '{METADATA_CSV_PATH}'...")
    successful_inserts = 0
    failed_files = 0
    inserted_filenames = set()

    try:
        with open(METADATA_CSV_PATH, mode='r', encoding='utf-8') as csvfile:
            csv_rows = list(csv.DictReader(csvfile))

            for row in tqdm(csv_rows, desc="Populating Database", unit="docs"):
                try:
                    doc_id_str = row['doc_id']
                    year_str = row['year']
                    full_title = row['full_title']
                    category = row['category']
                    category_folder = row['category_folder']
                    filename = row['filename']
                    law_type = law_type_map.get(category, "Uncategorized")

                    if law_type == "Uncategorized":
                        log_issue("Missing Law Type", f"Category '{category}' not found in law_types.csv.")

                    html_file_path = os.path.join(MAIN_DOCUMENTS_FOLDER, category_folder, year_str, filename)

                    try:
                        with open(html_file_path, 'r', encoding='utf-8') as html_file:
                            html_content = html_file.read()
                    except FileNotFoundError:
                        log_issue("File Not Found", html_file_path)
                        failed_files += 1
                        continue
                    except Exception as e:
                        log_issue("File Read Error", f"Could not read {html_file_path}: {e}")
                        failed_files += 1
                        continue

                    cleaned_html_content = clean_html_content(html_content)
                    word_count = calculate_word_count(cleaned_html_content)

                    document_to_insert = {
                        'doc_id': int(doc_id_str),
                        'full_title': full_title,
                        'category': category,
                        'year': int(year_str),
                        'law_type': law_type,
                        'word_count': word_count,
                        'content': cleaned_html_content
                    }

                    acts_collection.insert_one(document_to_insert)
                    inserted_filenames.add(filename)
                    successful_inserts += 1

                except KeyError as e:
                    log_issue("CSV Column Error", f"Missing column in metadata.csv row: {e}")
                    continue

    except FileNotFoundError:
        print(f"❌ Critical Error: Metadata file not found at '{METADATA_CSV_PATH}'.")
    except Exception as e:
        print(f"❌ An unexpected error occurred during CSV processing: {e}")

    print("\n--- Verifying File Coverage ---")
    all_disk_files = get_all_html_files(MAIN_DOCUMENTS_FOLDER)
    unpopulated_files = all_disk_files - inserted_filenames

    if not unpopulated_files:
        print("✅ All HTML files found in the directory were successfully populated.")
    else:
        print(f"⚠️ Warning: {len(unpopulated_files)} HTML file(s) found on disk were NOT populated.")
        print("   (This is likely because they are missing from the metadata.csv file)")
        for f in sorted(list(unpopulated_files)):
             log_issue("Not in CSV", f)
        print(f"   Details logged to '{LOG_FILE_NAME}'.")

    client.close()
    print("\n--- Script Finished ---")
    print(f"📊 Summary:")
    print(f"   - Documents successfully inserted: {successful_inserts}")
    print(f"   - Files in CSV but not found on disk: {failed_files}")
    print(f"   - Files on disk but not in CSV: {len(unpopulated_files)}")
    if failed_files > 0 or len(unpopulated_files) > 0:
        print(f"   - Check '{LOG_FILE_NAME}' for a detailed list of all issues.")
    print("✅ MongoDB connection closed.")

if __name__ == "__main__":
    populate_acts_collection()
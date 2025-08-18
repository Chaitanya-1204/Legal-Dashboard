# main_populate_script.py

import os
import csv
from pymongo import MongoClient
from datetime import datetime

# --- CONFIGURATION ---
# Update these variables to match your setup

# Path to your metadata CSV file
METADATA_CSV_PATH = '/DATACHAI/Final_Data/laws/metadata.csv'

# Name of the main folder containing your HTML documents
# This folder should contain subdirectories for each 'category_folder'
MAIN_DOCUMENTS_FOLDER = '/DATACHAI/Final_Data/laws'

# Log file for missing documents
LOG_FILE_NAME = 'processing_issues.log'

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "legal_dashboard_db"
COLLECTION_NAME = "acts"


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


def populate_acts_collection():
    """
    Connects to MongoDB, clears the 'acts' collection, populates it with data,
    and then verifies that all HTML files in the source directory were processed.
    """
    print("--- Starting MongoDB Population Script ---")
    setup_log_file() # Prepare the log file for this session

    # --- 1. ESTABLISH MONGODB CONNECTION ---
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        acts_collection = db[COLLECTION_NAME]
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB.")
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        return

    # --- 2. CLEAR EXISTING COLLECTION DATA ---
    try:
        delete_result = acts_collection.delete_many({})
        print(f"🧹 Cleared existing data. {delete_result.deleted_count} documents removed from '{COLLECTION_NAME}'.")
    except Exception as e:
        print(f"❌ Error clearing collection: {e}")
        client.close()
        return

    # --- 3. READ METADATA AND POPULATE ---
    print(f"📂 Reading metadata from '{METADATA_CSV_PATH}'...")
    
    successful_inserts = 0
    failed_files = 0
    inserted_filenames = set() # Keep track of files processed from CSV

    try:
        with open(METADATA_CSV_PATH, mode='r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            
            for row in csv_reader:
                try:
                    doc_id = row['doc_id']
                    full_title = row['full_title']
                    category = row['category']
                    year = row['year']
                    category_folder = row['category_folder']
                    filename = row['filename']

                    html_file_path = os.path.join(MAIN_DOCUMENTS_FOLDER, category_folder, year, filename)

                    try:
                        with open(html_file_path, 'r', encoding='utf-8') as html_file:
                            html_content = html_file.read()
                    except FileNotFoundError:
                        print(f"   ⚠️  Warning: HTML file not found for CSV entry. Path: {html_file_path}")
                        log_issue("File Not Found", html_file_path)
                        failed_files += 1
                        continue
                    except Exception as e:
                        print(f"   ❌ Error reading file {html_file_path}: {e}")
                        failed_files += 1
                        continue

                    document_to_insert = {
                        'doc_id': doc_id,
                        'full_title': full_title,
                        'category': category,
                        'year': int(year),
                        'content': html_content
                    }

                    acts_collection.insert_one(document_to_insert)
                    inserted_filenames.add(filename) # Add to our set of processed files
                    successful_inserts += 1
                    print(f"   -> Successfully processed and inserted doc_id: {doc_id}")

                except KeyError as e:
                    print(f"   ❌ Error: Missing expected column in CSV: {e}. Please check CSV headers.")
                    continue

    except FileNotFoundError:
        print(f"❌ Critical Error: Metadata file not found at '{METADATA_CSV_PATH}'.")
    except Exception as e:
        print(f"❌ An unexpected error occurred during CSV processing: {e}")
    
    # --- 4. VERIFY FILE COVERAGE ---
    print("\n--- Verifying File Coverage ---")
    all_disk_files = get_all_html_files(MAIN_DOCUMENTS_FOLDER)
    unpopulated_files = all_disk_files - inserted_filenames

    if not unpopulated_files:
        print("✅ All HTML files found in the directory were successfully populated into the database.")
    else:
        print(f"⚠️ Warning: {len(unpopulated_files)} HTML file(s) found on disk were NOT populated.")
        print("   This is likely because they are missing from the metadata.csv file.")
        for f in sorted(list(unpopulated_files)):
             log_issue("Not in CSV", f)
        print(f"   Details on these unpopulated files have been logged to '{LOG_FILE_NAME}'.")


    # --- 5. CLOSE CONNECTION AND REPORT SUMMARY ---
    client.close()
    print("\n--- Script Finished ---")
    print(f"📊 Summary:")
    print(f"   - Documents successfully inserted: {successful_inserts}")
    print(f"   - Files in CSV but not found on disk: {failed_files}")
    print(f"   - Files on disk but not in CSV: {len(unpopulated_files)}")
    if failed_files > 0 or len(unpopulated_files) > 0:
        print(f"   - Check '{LOG_FILE_NAME}' for a detailed list of all issues.")
    print("✅ MongoDB connection closed.")


# --- RUN THE SCRIPT ---
if __name__ == "__main__":
    populate_acts_collection()

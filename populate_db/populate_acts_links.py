# populate_db/populate_act_links.py

import os
import re
from pymongo import MongoClient
from bs4 import BeautifulSoup
from datetime import datetime

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "legal_dashboard_db"
SOURCE_COLLECTION = "acts"
TARGET_COLLECTION = "document_links"
LOG_FILE_NAME = 'link_processing_log.log'

def setup_log_file():
    """Creates and prepares the log file for the current run."""
    try:
        with open(LOG_FILE_NAME, 'w', encoding='utf-8') as log_file:
            log_file.write(f"--- Link Processing Log ---\n")
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

def extract_and_populate_links():
    """
    Connects to MongoDB, finds all inter-act links, and populates a new collection.
    """
    print("--- Starting Link Extraction Script ---")
    setup_log_file()

    # --- 1. ESTABLISH MONGODB CONNECTION ---
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        source_collection = db[SOURCE_COLLECTION]
        target_collection = db[TARGET_COLLECTION]
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB.")
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        return

    # --- 2. CLEAR EXISTING TARGET COLLECTION DATA ---
    try:
        delete_result = target_collection.delete_many({})
        print(f"🧹 Cleared existing data. {delete_result.deleted_count} documents removed from '{TARGET_COLLECTION}'.")
    except Exception as e:
        print(f"❌ Error clearing collection: {e}")
        client.close()
        return

    # --- 3. PROCESS EACH DOCUMENT IN THE SOURCE COLLECTION ---
    total_links_found = 0
    docs_processed = 0
    
    # Use a cursor to iterate through all documents in the 'acts' collection
    for act_document in source_collection.find({}, {'doc_id': 1, 'content': 1}):
        parent_doc_id = act_document.get('doc_id')
        html_content = act_document.get('content')

        if not parent_doc_id or not html_content:
            log_issue("Missing Data", f"Skipping document due to missing 'doc_id' or 'content'.")
            continue

        # Use BeautifulSoup to parse the HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all anchor tags with an 'href' attribute
        links = soup.find_all('a', href=True)
        
        links_in_doc = 0
        for link in links:
            href = link['href']
            # Use regex to find links that match the pattern '/doc/some_number'
            match = re.search(r'/doc/(\d+)', href)
            
            if match:
                # The extracted doc_id from the link
                linked_doc_id = int(match.group(1))
                
                # Prepare the document for the new collection
                link_document = {
                    'doc_id': linked_doc_id,
                    'parent_doc_id': parent_doc_id,
                    'type': SOURCE_COLLECTION # As requested, the type is 'acts'
                }
                
                # Insert the new document into the 'act_links' collection
                target_collection.insert_one(link_document)
                total_links_found += 1
                links_in_doc += 1

        if links_in_doc > 0:
            print(f"   -> Processed doc_id: {parent_doc_id}, found {links_in_doc} links.")
        
        docs_processed += 1

    # --- 4. CLOSE CONNECTION AND REPORT SUMMARY ---
    client.close()
    print("\n--- Script Finished ---")
    print(f"📊 Summary:")
    print(f"   - Total documents processed: {docs_processed}")
    print(f"   - Total inter-act links found and inserted: {total_links_found}")
    print("✅ MongoDB connection closed.")

# --- RUN THE SCRIPT ---
if __name__ == "__main__":
    extract_and_populate_links()

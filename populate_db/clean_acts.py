import re
from pymongo import MongoClient
from bs4 import BeautifulSoup
from tqdm import tqdm

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "legal_dashboard_db"
COLLECTION_NAME = "acts"

def clean_akn_p_content(html_content):
    """
    Cleans <span class="akn-p"> elements in HTML by:
    1. Removing leading hyphens
    2. Removing [***] / [ * * * ] completely (if only * and spaces inside)
    3. Removing all levels of brackets around meaningful content (e.g., [[hello]] → hello)
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Pattern to remove [***] / [ * * * ] etc.
    star_only_pattern = re.compile(r'\[\s*[\*\s]+\s*\]')

    # Pattern to remove ALL nested or single brackets but keep text (e.g., [[hello]] -> hello)
    nested_brackets_pattern = re.compile(r'\[+([^\[\]\*]+?)\]+')

    for span in soup.find_all('span', class_='akn-p'):
        text = span.get_text()

        # 1. Remove leading hyphen and optional whitespace
        text = re.sub(r'^\s*-\s*', '', text)

        # 2. Remove star-only bracketed content
        text = star_only_pattern.sub('', text)

        # 3. Replace nested brackets while preserving content
        while nested_brackets_pattern.search(text):
            text = nested_brackets_pattern.sub(r'\1', text)

        span.string = text

    return str(soup)

def update_documents():
    print("🔄 Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    total_docs = collection.count_documents({})
    print(f"📦 Found {total_docs} documents in '{COLLECTION_NAME}'.")

    updated_count = 0
    cursor = collection.find({}, {"_id": 1, "content": 1})

    for doc in tqdm(cursor, desc="Cleaning <span class='akn-p'>", unit="doc"):
        old_content = doc.get("content", "")
        cleaned_content = clean_akn_p_content(old_content)

        if cleaned_content != old_content:
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"content": cleaned_content}}
            )
            updated_count += 1

    print(f"\n✅ Done. Updated {updated_count} documents with cleaned content.")
    client.close()

if __name__ == "__main__":
    update_documents()

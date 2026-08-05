import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import os

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

def init_firestore():
    from google.cloud import firestore
    from google.oauth2 import service_account
    import google.auth

    if os.path.exists(CRED_PATH):
        print(f"Using service account file: {CRED_PATH}")
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        print("Service account file not found. Using Application Default Credentials (ADC)...")
        credentials, project = google.auth.default()
        return firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

def cleanup_old_data():
    db = init_firestore()
    if not db:
        return

    # Calculate cutoff date (7 days ago)
    cutoff_date = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    print(f"Deleting price_history data older than {cutoff_date}...")

    # Query for docs where date < cutoff_date
    # Note: This relies on 'date' being stored as "YYYY-MM-DD" string, which sorts correctly.
    docs = db.collection('price_history').where('date', '<', cutoff_date).stream()
    
    batch = db.batch()
    count = 0
    deleted_count = 0
    
    for doc in docs:
        batch.delete(doc.reference)
        count += 1
        deleted_count += 1
        
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
            print(f"Deleted {deleted_count} price_history docs...")
            
    if count > 0:
        batch.commit()
        
    print(f"Cleanup finished. Total {deleted_count} price_history documents deleted.")

    # 2. Cleanup access_logs (older than 7 days)
    print(f"Deleting access_logs older than {cutoff_date}...")
    access_docs = db.collection('access_logs').where('date', '<', cutoff_date).stream()
    
    batch = db.batch()
    count = 0
    deleted_access = 0
    for doc in access_docs:
        batch.delete(doc.reference)
        count += 1
        deleted_access += 1
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
            
    if count > 0:
        batch.commit()
    print(f"Cleanup finished. Total {deleted_access} access_logs deleted.")

    # 3. Cleanup crawl_stats (older than 7 days)
    print(f"Deleting crawl_stats older than {cutoff_date}...")
    crawl_docs = db.collection('crawl_stats').where('date', '<', cutoff_date).stream()
    
    batch = db.batch()
    count = 0
    deleted_crawl = 0
    for doc in crawl_docs:
        batch.delete(doc.reference)
        count += 1
        deleted_crawl += 1
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
            
    if count > 0:
        batch.commit()
    print(f"Cleanup finished. Total {deleted_crawl} crawl_stats deleted.")

    # 4. Cleanup daily_stats (older than 10 days, max 5000 docs per run)
    cutoff_10days = (datetime.date.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    print(f"Deleting daily_stats older than {cutoff_10days} (max 5000 docs per run)...")
    daily_stats_docs = db.collection('daily_stats').where('date', '<', cutoff_10days).limit(5000).stream()

    batch = db.batch()
    count = 0
    deleted_daily = 0
    for doc in daily_stats_docs:
        batch.delete(doc.reference)
        count += 1
        deleted_daily += 1
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
            print(f"Deleted {deleted_daily} daily_stats docs...")

    if count > 0:
        batch.commit()
    print(f"Cleanup finished. Total {deleted_daily} daily_stats deleted.")
        
    print(f"Cleanup complete. Total documents deleted: {deleted_count + deleted_access + deleted_crawl + deleted_daily}")

if __name__ == "__main__":
    cleanup_old_data()

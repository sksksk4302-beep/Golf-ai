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
            print(f"Deleted {deleted_count} docs...")
            
    if count > 0:
        batch.commit()
        print(f"Deleted {deleted_count} docs...")
        
    print(f"Cleanup complete. Total documents deleted: {deleted_count}")

if __name__ == "__main__":
    cleanup_old_data()

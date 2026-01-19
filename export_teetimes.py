
import os
import csv
import datetime
from google.cloud import firestore as google_firestore
import google.auth

PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

def init_firestore():
    if os.path.exists(CRED_PATH):
        from google.oauth2 import service_account
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return google_firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        credentials, project = google.auth.default()
        return google_firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

def export_teetimes():
    db = init_firestore()
    
    # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"teetimes_export_{timestamp}.csv"
    
    print(f"Fetching data from 'tee_times' collection...")
    docs = db.collection('tee_times').stream()
    
    fieldnames = ['club_name', 'date', 'time', 'price', 'hour', 'weekday', 'source', 'crawled_at']
    
    count = 0
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for doc in docs:
            data = doc.to_dict()
            # Ensure all fields exist, default to None if missing
            row = {field: data.get(field) for field in fieldnames}
            writer.writerow(row)
            count += 1
            if count % 100 == 0:
                print(f"Exported {count} records...", end='\r')
                
    print(f"\nExport complete! {count} records saved to {filename}")

if __name__ == "__main__":
    export_teetimes()

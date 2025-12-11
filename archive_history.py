import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import os
from collections import defaultdict

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

def init_firestore():
    # Use google.cloud.firestore directly
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

def archive_history():
    db = init_firestore()
    if not db:
        return

    print("Fetching current tee times for aggregation...")
    
    # Query all tee times (or filter by date range if dataset is huge)
    # For now, we fetch all valid future tee times
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    docs = db.collection('tee_times').where('date', '>=', today_str).stream()
    
    # Aggregation structure: data[club][date][hour] = [prices...]
    aggregated = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    count = 0
    for doc in docs:
        d = doc.to_dict()
        club = d.get('club_name')
        date = d.get('date')
        hour = d.get('hour')
        price = d.get('price')
        
        if club and date and hour is not None and price:
            aggregated[club][date][hour].append(price)
            count += 1
            
    print(f"Processed {count} tee times. Creating snapshots...")
    
    batch = db.batch()
    batch_count = 0
    snapshot_time = datetime.datetime.now()
    
    for club, dates in aggregated.items():
        for date, hours in dates.items():
            for hour, prices in hours.items():
                min_price = min(prices)
                avg_price = sum(prices) / len(prices)
                
                # Document ID: YYYYMMDD_Club_Hour_SnapshotTime
                # We use a simplified snapshot ID to allow multiple snapshots per day
                # e.g. 20251211_Plaza_08_1400
                snapshot_str = snapshot_time.strftime("%H%M")
                doc_id = f"{date.replace('-', '')}_{club}_{hour}_{snapshot_str}"
                
                doc_ref = db.collection('price_history').document(doc_id)
                
                data = {
                    "club_name": club,
                    "date": date,
                    "hour": hour,
                    "stats": {
                        "min": min_price,
                        "avg": avg_price,
                        "count": len(prices)
                    },
                    "snapshot_at": snapshot_time,
                    "weekday": datetime.datetime.strptime(date, "%Y-%m-%d").weekday()
                }
                
                batch.set(doc_ref, data)
                batch_count += 1
                
                if batch_count >= 400:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
                    print("Committed batch...")

    if batch_count > 0:
        batch.commit()
        
    print("History archiving completed.")

if __name__ == "__main__":
    archive_history()

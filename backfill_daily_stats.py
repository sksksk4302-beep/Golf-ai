import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import os
from collections import defaultdict

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
        print("Service account file not found. Using ADC...")
        credentials, project = google.auth.default()
        return firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

def backfill_daily_stats():
    db = init_firestore()
    if not db:
        return

    print("Fetching ALL price_history data...")
    docs = db.collection('price_history').stream()
    
    # Structure: stats[date][club][hour] = [prices...]
    stats = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    count = 0
    skipped = 0
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    
    for doc in docs:
        d = doc.to_dict()
        date = d.get('date')
        club = d.get('club_name')
        hour = d.get('hour')
        snapshot_min = d.get('stats', {}).get('min')
        
        if not date or not club or hour is None or snapshot_min is None:
            continue
            
        # Filter: Only process dates BEFORE today
        if date >= today_str:
            skipped += 1
            continue
            
        stats[date][club][hour].append(snapshot_min)
        count += 1
        
    print(f"Processed {count} records. Skipped {skipped} (today/future/invalid).")
    print("Writing to daily_stats...")
    
    batch = db.batch()
    batch_count = 0
    total_written = 0
    
    for date, clubs in stats.items():
        for club, hours in clubs.items():
            for hour, prices in hours.items():
                min_price = min(prices)
                avg_price = sum(prices) / len(prices)
                
                doc_id = f"{date.replace('-', '')}_{club}_{hour}"
                doc_ref = db.collection('daily_stats').document(doc_id)
                
                data = {
                    "club_name": club,
                    "date": date,
                    "hour": hour,
                    "min_price": min_price,
                    "avg_price": avg_price,
                    "snapshot_count": len(prices),
                    "updated_at": firestore.SERVER_TIMESTAMP
                }
                
                batch.set(doc_ref, data)
                batch_count += 1
                total_written += 1
                
                if batch_count >= 400:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
                    print(f"Committed {total_written} docs...")
                    
    if batch_count > 0:
        batch.commit()
        
    print(f"Backfill complete. Total daily_stats documents created: {total_written}")

if __name__ == "__main__":
    backfill_daily_stats()

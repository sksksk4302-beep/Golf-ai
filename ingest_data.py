import datetime
import os
import firebase_admin
from firebase_admin import credentials, firestore
from crawler_utils import crawl_golfpang, crawl_teescan, GOLF_CLUBS

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"
DAYS_TO_CRAWL = 7

def init_firestore():
    # Use google.cloud.firestore directly to specify database
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

def save_tee_times(db, tee_times):
    batch = db.batch()
    count = 0
    
    for item in tee_times:
        # Create a unique ID: YYYYMMDD_ClubName_Time
        # Sanitize club name for ID
        club_safe = item['golf'].replace(" ", "").replace("/", "_")
        doc_id = f"{item['date'].replace('-', '')}_{club_safe}_{item['time'].replace(':', '')}"
        
        doc_ref = db.collection('tee_times').document(doc_id)
        
        # Prepare data
        data = {
            "club_name": item['golf'],
            "date": item['date'],
            "time": item['time'],
            "hour": item['hour_num'],
            "price": item['price'],
            "source": item.get('source', 'Golfpang'),
            "crawled_at": firestore.SERVER_TIMESTAMP,
            "weekday": datetime.datetime.strptime(item['date'], "%Y-%m-%d").weekday()
        }
        
        batch.set(doc_ref, data)
        count += 1
        
        # Commit in batches of 500
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"Saved {count} items...")
            
    if count % 400 != 0:
        batch.commit()
        
    print(f"Total {count} tee times saved to Firestore.")

def main():
    db = init_firestore()
    if not db:
        return

    today = datetime.date.today()
    
    # We pass an empty list for 'favorite' to crawl ALL clubs
    # But wait, crawler_utils.py filters by favorite if provided.
    # If we want ALL clubs, we should pass an empty list?
    # Let's check crawler_utils.py again. It says:
    # if not _fav_ok(name, favorite): continue
    # And _fav_ok returns True if favorite is empty.
    # So passing [] works.
    
    for i in range(DAYS_TO_CRAWL):
        target_date = (today + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        print(f"\n>>> Crawling for {target_date}...")
        
        try:
            # Crawl Golfpang
            data_gp = crawl_golfpang(target_date, [])
            
            # Crawl Teescan
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting Teescan crawl...")
            data_ts = crawl_teescan(target_date, [])
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Teescan crawl finished. Found {len(data_ts)} items.")
            
            data = data_gp + data_ts
            if data:
                print(f"Found {len(data)} tee times. Saving to Firestore...")
                save_tee_times(db, data)
            else:
                print("No data found.")
                
        except Exception as e:
            print(f"Error processing {target_date}: {e}")

if __name__ == "__main__":
    main()

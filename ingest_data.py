import datetime
import os
import firebase_admin
from firebase_admin import credentials, firestore
from crawler_utils import crawl_golfpang, crawl_teescan, GOLF_CLUBS
from weather_utils import get_weather_for_club

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"
DAYS_TO_CRAWL = 14

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

def save_tee_times(db, tee_times, target_date):
    # 이번 업데이트의 고유 ID (예: 202602031950)
    sync_id = datetime.datetime.now().strftime("%Y%m%d%H%M")
    
    print(f"Starting sync for {target_date} with sync_id={sync_id}")

    # 1. Upsert new data (Write all current data with new sync_id)
    # This avoids reading all documents first.
    batch = db.batch()
    count = 0
    ops_count = 0
    
    for item in tee_times:
        club_safe = item['golf'].replace(" ", "").replace("/", "_")
        doc_id = f"{item['date'].replace('-', '')}_{club_safe}_{item['time'].replace(':', '')}"
        doc_ref = db.collection('tee_times').document(doc_id)
        
        # Enrich with weather data
        weather = get_weather_for_club(item['golf'], item['date'])
        
        item_data = {
            "club_name": item['golf'],
            "date": item['date'],
            "time": item['time'],
            "hour": item['hour_num'],
            "price": item['price'],
            "source": item.get('source', 'Golfpang'),
            "weekday": datetime.datetime.strptime(item['date'], "%Y-%m-%d").weekday(),
            "sync_id": sync_id,  # Save current session ID
            "updated_at": firestore.SERVER_TIMESTAMP
        }
        
        if weather:
             item_data['weather'] = weather
        elif item.get('weather'):
             item_data['weather'] = item.get('weather')

        batch.set(doc_ref, item_data, merge=True)
        count += 1
        ops_count += 1
        
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"Processed {count} valid items (upserts)...")
            
    if count % 400 != 0:
        batch.commit()
        
    print(f"Upsert complete. Now clearing stale data for {target_date}...")

    # 2. Delete stale data (Read only what needs to be deleted)
    # Find documents for this date that do NOT have the current sync_id
    stale_docs = db.collection('tee_times') \
        .where('date', '==', target_date) \
        .where('sync_id', '<', sync_id) \
        .stream()
        
    delete_batch = db.batch()
    delete_count = 0
    
    for doc in stale_docs:
        delete_batch.delete(doc.reference)
        delete_count += 1
        ops_count += 1
        
        if delete_count % 400 == 0:
            delete_batch.commit()
            delete_batch = db.batch()
            print(f"Processed {delete_count} stale items (deletes)...")
            
    if delete_count % 400 != 0:
        delete_batch.commit()

    print(f"Sync complete for {target_date}. Total ops: {ops_count} (Upserts: {count}, Deletes: {delete_count}).")

def process_date(target_date, db):
    """
    Crawls data for a single date and saves it to Firestore.
    Returns the count of items saved (or found).
    """
    print(f"\n>>> [Start] Crawling for {target_date}...")
    try:
        # Crawl Golfpang
        data_gp = crawl_golfpang(target_date, [])
        
        # Crawl Teescan
        # print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting Teescan crawl for {target_date}...")
        data_ts = crawl_teescan(target_date, [])
        
        data = data_gp + data_ts
        if data:
            print(f"[{target_date}] Found {len(data)} tee times. Syncing...")
            save_tee_times(db, data, target_date)
            return len(data)
        else:
            print(f"[{target_date}] No data found. Clearing...")
            save_tee_times(db, [], target_date)
            return 0
            
    except Exception as e:
        print(f"Error processing {target_date}: {e}")
        return 0

def main():
    db = init_firestore()
    if not db:
        return

    today = datetime.date.today()
    
    # Prepare list of dates to crawl
    dates_to_crawl = []
    for i in range(DAYS_TO_CRAWL):
        d = (today + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        dates_to_crawl.append(d)
        
    print(f"Starting sequential crawl for {len(dates_to_crawl)} days: {dates_to_crawl}")
    
    # Process dates sequentially to avoid timeout
    # Each date will still use sector-level parallelization (3 concurrent ops)
    # This prevents compounding: 1 date × 3 sectors instead of 3 dates × 3 sectors
    
    total_items = 0
    for date in dates_to_crawl:
        try:
            count = process_date(date, db)
            total_items += count
            print(f">>> [Done] {date} finished. Items: {count}")
        except Exception as e:
            print(f">>> [Error] {date} failed: {e}")

    print(f"\nAll crawling tasks completed. Total items processed: {total_items}")

if __name__ == "__main__":
    main()

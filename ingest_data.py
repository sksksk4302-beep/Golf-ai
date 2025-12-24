import datetime
import os
import firebase_admin
from firebase_admin import credentials, firestore
from crawler_utils import crawl_golfpang, crawl_teescan, GOLF_CLUBS

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
    # 1. Calculate new IDs
    new_ids = set()
    data_map = {}
    
    for item in tee_times:
        club_safe = item['golf'].replace(" ", "").replace("/", "_")
        doc_id = f"{item['date'].replace('-', '')}_{club_safe}_{item['time'].replace(':', '')}"
        new_ids.add(doc_id)
        data_map[doc_id] = item

    # 2. Fetch existing IDs and Data for this date
    print(f"Checking for stale data on {target_date}...")
    existing_docs = db.collection('tee_times').where('date', '==', target_date).stream()
    existing_ids = set()
    existing_data_map = {}
    
    for doc in existing_docs:
        existing_ids.add(doc.id)
        existing_data_map[doc.id] = doc.to_dict()
        
    # 3. Identify IDs to delete
    to_delete = existing_ids - new_ids
    print(f"Found {len(to_delete)} stale items to delete.")
    
    # 4. Batch Operations
    batch = db.batch()
    count = 0
    ops_count = 0 # Track actual DB operations
    
    # Delete operations
    for doc_id in to_delete:
        doc_ref = db.collection('tee_times').document(doc_id)
        batch.delete(doc_ref)
        count += 1
        ops_count += 1
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"Processed {count} operations (deletes)...")

    # Upsert operations (Only if changed)
    skipped_count = 0
    
    for doc_id, item in data_map.items():
        doc_ref = db.collection('tee_times').document(doc_id)
        
        new_data = {
            "club_name": item['golf'],
            "date": item['date'],
            "time": item['time'],
            "hour": item['hour_num'],
            "price": item['price'],
            "source": item.get('source', 'Golfpang'),
            # "crawled_at": firestore.SERVER_TIMESTAMP, # Don't include in comparison
            "weekday": datetime.datetime.strptime(item['date'], "%Y-%m-%d").weekday()
        }
        
        # Check if update is needed
        needs_update = True
        if doc_id in existing_data_map:
            existing = existing_data_map[doc_id]
            # Compare fields (excluding crawled_at)
            # We assume if these fields match, the record is identical.
            if (existing.get('price') == new_data['price'] and
                existing.get('time') == new_data['time'] and
                existing.get('club_name') == new_data['club_name']):
                needs_update = False
        
        if needs_update:
            # Add crawled_at only when writing
            new_data["crawled_at"] = firestore.SERVER_TIMESTAMP
            batch.set(doc_ref, new_data)
            count += 1
            ops_count += 1
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()
                print(f"Processed {count} operations (upserts)...")
        else:
            skipped_count += 1
            
    if count % 400 != 0:
        batch.commit()
        
    print(f"Sync complete for {target_date}. Total ops: {ops_count} (Deletes: {len(to_delete)}, Upserts: {ops_count - len(to_delete)}). Skipped: {skipped_count}")

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
        
    print(f"Starting parallel crawl for {len(dates_to_crawl)} days: {dates_to_crawl}")
    
    # Use ThreadPoolExecutor for parallel processing
    # Adjust max_workers based on Cloud Run resources and target site limits.
    # 4 workers is a good starting point for 14 days (approx 3-4 batches).
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    total_items = 0
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_date = {executor.submit(process_date, date, db): date for date in dates_to_crawl}
        
        for future in as_completed(future_to_date):
            date = future_to_date[future]
            try:
                count = future.result()
                total_items += count
                print(f">>> [Done] {date} finished. Items: {count}")
            except Exception as e:
                print(f">>> [Error] {date} failed: {e}")

    print(f"\nAll crawling tasks completed. Total items processed: {total_items}")

if __name__ == "__main__":
    main()

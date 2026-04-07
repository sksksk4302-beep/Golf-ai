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
            "benefit": item.get('benefit', ''),  # 티스캐너 benefit 필드
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

def cleanup_past_teetimes(db):
    """
    Deletes tee_times documents with dates older than today.
    This prevents stale data from accumulating and increasing Firestore I/O costs.
    """
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    print(f"Cleaning up tee_times older than {today_str}...")

    # Find documents for dates before Today
    stale_docs = db.collection('tee_times') \
        .where('date', '<', today_str) \
        .stream()
        
    delete_batch = db.batch()
    delete_count = 0
    total_deleted = 0
    
    for doc in stale_docs:
        delete_batch.delete(doc.reference)
        delete_count += 1
        total_deleted += 1
        
        if delete_count % 400 == 0:
            delete_batch.commit()
            delete_batch = db.batch()
            delete_count = 0
            print(f"Deleted {total_deleted} stale records...")
            
    if delete_count % 400 != 0:
        delete_batch.commit()

    print(f"Tee times cleanup complete. Total deleted: {total_deleted}")

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
            print(f"[{target_date}] Found {len(data)} tee times (GP:{len(data_gp)}, TS:{len(data_ts)}). Syncing...")
            save_tee_times(db, data, target_date)
            return len(data_gp), len(data_ts)
        else:
            print(f"[{target_date}] No data found. Clearing...")
            save_tee_times(db, [], target_date)
            return 0, 0
            
    except Exception as e:
        print(f"Error processing {target_date}: {e}")
        return 0, 0

def main():
    db = init_firestore()
    if not db:
        return

    # 0. Cleanup past data (only in Hot workflow: D+0)
    # This ensures we don't leak Action minutes in Warm/Cold workflows
    start_day_env = os.environ.get("CRAWL_START_DAY", "0")
    if start_day_env == "0":
        cleanup_past_teetimes(db)

    today = datetime.date.today()
    
    # Support tiered crawling via environment variables
    start_day = int(os.environ.get("CRAWL_START_DAY", 0))
    end_day = int(os.environ.get("CRAWL_END_DAY", DAYS_TO_CRAWL - 1))
    
    # Prepare list of dates to crawl
    dates_to_crawl = []
    for i in range(start_day, end_day + 1):
        d = (today + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        dates_to_crawl.append(d)
        
    print(f"Starting sequential crawl for {len(dates_to_crawl)} days: {dates_to_crawl}")
    
    # Process dates sequentially to avoid timeout
    # Each date will still use sector-level parallelization (3 concurrent ops)
    # This prevents compounding: 1 date × 3 sectors instead of 3 dates × 3 sectors
    
    total_gp_items = 0
    total_ts_items = 0
    for date in dates_to_crawl:
        try:
            gp_count, ts_count = process_date(date, db)
            total_gp_items += gp_count
            total_ts_items += ts_count
            print(f">>> [Done] {date} finished. Items: GP={gp_count}, TS={ts_count}")
        except Exception as e:
            print(f">>> [Error] {date} failed: {e}")

    print(f"\nAll crawling tasks completed. Total GP: {total_gp_items}, Total TS: {total_ts_items}")

    # 9AM KST (UTC 00:xx ~ 01:xx) Verification Logic for GitHub Actions Alerting
    current_utc_hour = datetime.datetime.utcnow().hour
    # Only verify on Crawl Hot (start_day == "0")
    if os.environ.get("CRAWL_START_DAY", "0") == "0" and current_utc_hour in [0, 1]:
        if total_gp_items == 0 or total_ts_items == 0:
            print("\n=====================================================")
            print(f"🚨 ALERT! Critical Crawler Failure at 9 AM KST 🚨")
            print(f"One of the data sources returned 0 results.")
            print(f"Golfpang: {total_gp_items}, TeeScanner: {total_ts_items}")
            print("Failing the Action to trigger GitHub notifications.")
            print("=====================================================")
            import sys
            sys.exit(1)

if __name__ == "__main__":
    main()

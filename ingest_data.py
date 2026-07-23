import datetime
from datetime import timezone, timedelta
import os
import firebase_admin
from firebase_admin import credentials, firestore
from crawler_utils import crawl_golfpang, crawl_teescan
# Weather crawling removed to avoid API timeout issues

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

def save_tee_times(db, tee_times, target_date, sources_with_data=None):
    if sources_with_data is None:
        sources_with_data = set(item.get('source', 'golfpang') for item in tee_times)
    
    print(f"Starting diff & sync for {target_date}, active sources: {sources_with_data}")

    # 1. Fetch existing data for the target_date to minimize writes
    existing_docs = {}
    docs = db.collection('tee_times').where('date', '==', target_date).stream()
    for doc in docs:
        data = doc.to_dict()
        if data.get('source', 'golfpang') in sources_with_data:
            existing_docs[doc.id] = data
            
    batch = db.batch()
    upsert_count = 0
    skip_count = 0
    delete_count = 0
    
    # 2. Process scraped data
    for item in tee_times:
        club_safe = item['golf'].replace(" ", "").replace("/", "_")
        doc_id = f"{item['date'].replace('-', '')}_{club_safe}_{item['time'].replace(':', '')}"
        doc_ref = db.collection('tee_times').document(doc_id)
        
        new_price = item['price']
        new_benefit = item.get('benefit', '')
        
        # Check if identical record already exists
        if doc_id in existing_docs:
            old_data = existing_docs[doc_id]
            if old_data.get('price') == new_price and old_data.get('benefit', '') == new_benefit:
                # No change needed, skip write to save cost
                skip_count += 1
                del existing_docs[doc_id]
                continue
            # Data changed, will upsert
            del existing_docs[doc_id]
            
        # Needs upsert
        item_data = {
            "club_name": item['golf'],
            "date": item['date'],
            "time": item['time'],
            "hour": item['hour_num'],
            "price": new_price,
            "benefit": new_benefit,
            "source": item.get('source', 'Golfpang'),
            "url": item.get('url', ''),
            "source_idx": item.get('source_idx', ''),
            "weekday": datetime.datetime.strptime(item['date'], "%Y-%m-%d").weekday(),
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        batch.set(doc_ref, item_data, merge=True)
        upsert_count += 1
        
        if upsert_count % 400 == 0:
            batch.commit()
            batch = db.batch()
            
    if upsert_count % 400 != 0:
        batch.commit()
        batch = db.batch()

    # 3. Any remaining documents in existing_docs were not in the new scraped data, so they are stale
    for doc_id, old_data in existing_docs.items():
        doc_ref = db.collection('tee_times').document(doc_id)
        batch.delete(doc_ref)
        delete_count += 1
        
        if delete_count > 0 and delete_count % 400 == 0:
            batch.commit()
            batch = db.batch()
            
    if delete_count % 400 != 0:
        batch.commit()

    print(f"Sync complete for {target_date}. Upserts: {upsert_count}, Deletes: {delete_count}, Skipped(Unchanged): {skip_count}")

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
        data_ts = crawl_teescan(target_date, [])
        
        # Track which sources returned data
        sources_with_data = set()
        if data_gp:
            sources_with_data.add('golfpang')
        if data_ts:
            sources_with_data.add('teescan')
        
        # Log warnings when a source returns 0
        if not data_gp:
            print(f"⚠️ [{target_date}] Golfpang returned 0 results!")
        if not data_ts:
            print(f"⚠️ [{target_date}] TeeScanner returned 0 results!")
        
        data = data_gp + data_ts
        if data:
            print(f"[{target_date}] Found {len(data)} tee times (GP:{len(data_gp)}, TS:{len(data_ts)}). Syncing...")
            save_tee_times(db, data, target_date, sources_with_data)
            return len(data_gp), len(data_ts)
        else:
            print(f"[{target_date}] No data found from any source. Clearing...")
            save_tee_times(db, [], target_date, sources_with_data)
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
    # OPTIMIZATION: Only run this once a day around 07:00 KST to drastically reduce Firestore Reads
    start_day_env = os.environ.get("CRAWL_START_DAY", "0")
    if start_day_env == "0":
        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        now_kst = datetime.datetime.now(KST)
        if 6 <= now_kst.hour <= 8:
            cleanup_past_teetimes(db)
        else:
            print("Skipping cleanup_past_teetimes() because it's not the morning (06:00~08:00 KST).")

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

    # Verification Logic for ALL workflows
    if start_day_env == "0":
        crawl_tier = "Hot-A"
    elif start_day_env == "2":
        crawl_tier = "Hot-B"
    elif start_day_env == "4":
        crawl_tier = "Warm"
    else:
        crawl_tier = "Cold"

        
    # Optional Weather Ingestion
    if os.environ.get("CRAWL_WEATHER", "").lower() == "true":
        try:
            from ingest_weather import ingest_weather
            print("\n=====================================================")
            print("Starting weather ingestion as CRAWL_WEATHER=true")
            print("=====================================================")
            ingest_weather()
        except Exception as e:
            print(f"⚠️ Weather ingestion failed (non-fatal): {e}")
    
    # Write crawl execution stats to Firestore
    try:
        from google.cloud import firestore as gc_firestore
        KST = timezone(timedelta(hours=9))
        now_kst = datetime.datetime.now(KST)
        
        crawl_stat = {
            "date": now_kst.strftime("%Y-%m-%d"), # KST Date
            "tier": crawl_tier,
            "crawl_range": f"D+{start_day}~D+{end_day}",
            "golfpang_total": total_gp_items,
            "teescan_total": total_ts_items,
            "dates_crawled": len(dates_to_crawl),
            "status": "success" if (total_gp_items > 0 and total_ts_items > 0) else "partial_fail",
            "completed_at": gc_firestore.SERVER_TIMESTAMP
        }
        # Doc ID: YYYYMMDDHHMM_Tier
        doc_id = f"{now_kst.strftime('%Y%m%d%H%M')}_{crawl_tier}"
        db.collection('crawl_stats').document(doc_id).set(crawl_stat)
        print(f"Successfully recorded crawl statistics in Firestore: {doc_id}")
    except Exception as e:
        print(f"Failed to record crawl statistics in Firestore: {e}")

    # If either source returned 0 results, fail the Action to trigger GitHub notification
    if total_gp_items == 0 or total_ts_items == 0:
        print("\n=====================================================")
        print(f"🚨 ALERT! Crawler Failure in {crawl_tier} (D+{start_day_env}~{end_day}) 🚨")
        print(f"One of the data sources returned 0 results.")
        print(f"Golfpang: {total_gp_items}, TeeScanner: {total_ts_items}")
        print("Failing the Action to trigger GitHub notifications.")
        print("=====================================================")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()

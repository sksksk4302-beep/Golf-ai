import datetime
import os
import firebase_admin
from firebase_admin import credentials, firestore
from crawler_utils import crawl_golfpang, crawl_teescan, GOLF_CLUBS
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
    # 이번 업데이트의 고유 ID (예: 202602031950)
    sync_id = datetime.datetime.now().strftime("%Y%m%d%H%M")
    
    # Determine which sources actually returned data
    if sources_with_data is None:
        sources_with_data = set(item.get('source', 'golfpang') for item in tee_times)
    
    print(f"Starting sync for {target_date} with sync_id={sync_id}, active sources: {sources_with_data}")

    # 1. Upsert new data (Write all current data with new sync_id)
    # This avoids reading all documents first.
    batch = db.batch()
    count = 0
    ops_count = 0
    
    for item in tee_times:
        club_safe = item['golf'].replace(" ", "").replace("/", "_")
        doc_id = f"{item['date'].replace('-', '')}_{club_safe}_{item['time'].replace(':', '')}"
        doc_ref = db.collection('tee_times').document(doc_id)
        
        item_data = {
            "club_name": item['golf'],
            "date": item['date'],
            "time": item['time'],
            "hour": item['hour_num'],
            "price": item['price'],
            "benefit": item.get('benefit', ''),  # 티스캐너 benefit 필드
            "source": item.get('source', 'Golfpang'),
            "url": item.get('url', ''),
            "source_idx": item.get('source_idx', ''),
            "weekday": datetime.datetime.strptime(item['date'], "%Y-%m-%d").weekday(),
            "sync_id": sync_id,  # Save current session ID
            "updated_at": firestore.SERVER_TIMESTAMP
        }

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

    # Only delete stale data for sources that actually returned results.
    # This prevents wiping teescan data when teescan crawl fails/returns 0.
    stale_docs = db.collection('tee_times') \
        .where('date', '==', target_date) \
        .stream()
        
    delete_batch = db.batch()
    delete_count = 0
    skip_count = 0
    
    for doc in stale_docs:
        doc_data = doc.to_dict()
        
        # In-memory filter to avoid composite index requirement
        if str(doc_data.get('sync_id', '999999999999')) >= sync_id:
            continue
            
        doc_source = doc_data.get('source', 'golfpang')
        
        # Only delete if this source had data in current sync
        if doc_source in sources_with_data:
            delete_batch.delete(doc.reference)
            delete_count += 1
            ops_count += 1
        else:
            skip_count += 1
        
        if delete_count > 0 and delete_count % 400 == 0:
            delete_batch.commit()
            delete_batch = db.batch()
            print(f"Processed {delete_count} stale items (deletes)...")
            
    if delete_count % 400 != 0:
        delete_batch.commit()

    if skip_count > 0:
        print(f"⚠️ Preserved {skip_count} items from inactive sources (not in {sources_with_data})")
    print(f"Sync complete for {target_date}. Total ops: {ops_count} (Upserts: {count}, Deletes: {delete_count}, Preserved: {skip_count}).")

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

    # Verification Logic for ALL workflows
    if start_day_env == "0":
        crawl_tier = "Hot-A"
    elif start_day_env == "2":
        crawl_tier = "Hot-B"
    elif start_day_env == "4":
        crawl_tier = "Warm"
    else:
        crawl_tier = "Cold"
        
    # Save crawl log to Firestore
    try:
        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        now_kst = datetime.datetime.now(KST)
        
        db.collection('crawl_runs').add({
            "completed_at": now_kst.isoformat(),
            "date_kst": now_kst.strftime("%Y-%m-%d"),
            "tier": crawl_tier,
            "range": f"D+{start_day}~D+{end_day}",
            "golfpang_count": total_gp_items,
            "teescan_count": total_ts_items,
            "status": "success" if (total_gp_items > 0 and total_ts_items > 0) else "partial",
            "timestamp": firestore.SERVER_TIMESTAMP
        })
        print("Successfully saved crawl run log to Firestore.")
    except Exception as e:
        print(f"Failed to save crawl run log: {e}")
        
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
        crawl_stat = {
            "date": datetime.date.today().strftime("%Y-%m-%d"),
            "tier": crawl_tier,
            "crawl_range": f"D+{start_day}~D+{end_day}",
            "golfpang_total": total_gp_items,
            "teescan_total": total_ts_items,
            "dates_crawled": len(dates_to_crawl),
            "status": "success" if (total_gp_items > 0 and total_ts_items > 0) else "partial_fail",
            "completed_at": gc_firestore.SERVER_TIMESTAMP
        }
        # Doc ID: YYYYMMDDHHMM_Tier
        doc_id = f"{datetime.datetime.now().strftime('%Y%m%d%H%M')}_{crawl_tier}"
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

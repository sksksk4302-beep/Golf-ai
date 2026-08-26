import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import os
from collections import defaultdict
from cleanup_old_history import cleanup_old_data

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

    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")

    # =========================================
    # 1. 오늘 실제로 크롤링된 날짜 범위 파악
    # =========================================
    print("Checking today's crawl_stats to determine changed dates...")
    crawled_dates = set()
    try:
        crawl_docs = db.collection('crawl_stats') \
            .where('date', '==', today_str) \
            .where('status', '==', 'success') \
            .stream()
        for doc in crawl_docs:
            data = doc.to_dict()
            crawl_range = data.get('crawl_range', '')  # e.g., "D+0~D+1"
            try:
                parts = crawl_range.replace('D+', '').split('~')
                start_d = int(parts[0])
                end_d = int(parts[1]) if len(parts) > 1 else start_d
                for i in range(start_d, end_d + 1):
                    d = (today + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                    crawled_dates.add(d)
            except (ValueError, IndexError):
                pass
    except Exception as e:
        print(f"  ⚠️ crawl_stats 조회 실패: {e}")

    if not crawled_dates:
        # Fallback: crawl_stats가 없으면 D+0~D+3 기본값 사용
        print("  ⚠️ crawl_stats에서 정보를 찾지 못함. D+0~D+3 기본값 사용")
        for i in range(4):
            d = (today + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            crawled_dates.add(d)

    crawled_dates_sorted = sorted(crawled_dates)
    print(f"  → Incremental archive 대상: {len(crawled_dates_sorted)}일 ({crawled_dates_sorted})")

    # =========================================
    # 2. 변경된 날짜의 tee_times만 읽기
    # =========================================
    print("Fetching tee times for changed dates only...")
    aggregated = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    count = 0

    DATE_CHUNK = 10
    dates_list = list(crawled_dates_sorted)
    for i in range(0, len(dates_list), DATE_CHUNK):
        date_chunk = dates_list[i:i+DATE_CHUNK]
        docs = db.collection('tee_times').where('date', 'in', date_chunk).stream()
        for doc in docs:
            d = doc.to_dict()
            club = d.get('club_name')
            date = d.get('date')
            hour = d.get('hour')
            price = d.get('price')

            if club and date and hour is not None and price:
                aggregated[club][date][hour].append(price)
                count += 1

    print(f"  → {count}건 티타임 처리 완료. price_history 스냅샷 생성 중...")

    # =========================================
    # 3. 변경된 날짜의 price_history만 쓰기
    # =========================================
    batch = db.batch()
    batch_count = 0
    snapshot_time = datetime.datetime.now()

    for club, dates in aggregated.items():
        for date, hours in dates.items():
            for hour, prices in hours.items():
                min_price = min(prices)
                avg_price = sum(prices) / len(prices)

                doc_id = f"{date.replace('-', '')}_{club}_{hour}"
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
                    "weekday": datetime.datetime.strptime(date, "%Y-%m-%d").weekday(),
                    "expire_at": snapshot_time + datetime.timedelta(days=7)
                }

                batch.set(doc_ref, data, merge=True)
                batch_count += 1

                if batch_count >= 400:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
                    print("  Committed batch...")

    if batch_count > 0:
        batch.commit()

    print(f"  → price_history 스냅샷 완료 ({batch_count}건)")

    # =========================================
    # 4. 오래된 데이터 정리 (1회만 실행)
    # =========================================
    try:
        print("Running cleanup for data older than 7 days...")
        cleanup_old_data()
    except Exception as e:
        print(f"Cleanup failed (non-fatal): {e}")

    # =========================================
    # 5. 어제 날짜 daily_stats 집계
    # =========================================
    aggregate_daily_stats(db)

    print("History archiving completed.")

def aggregate_daily_stats(db):
    """
    Aggregates price_history into daily_stats for Yesterday.
    This ensures we have a fast lookup table for past dates.
    """
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Aggregating daily stats for {yesterday}...")
    
    # 1. Fetch existing daily_stats to avoid redundant writes
    existing_docs = db.collection('daily_stats').where('date', '==', yesterday).stream()
    existing_map = {}
    for doc in existing_docs:
        existing_map[doc.id] = doc.to_dict()
    
    # 2. Query price_history for yesterday
    docs = db.collection('price_history').where('date', '==', yesterday).stream()
    
    # Structure: stats[club][hour] = [prices...]
    stats = defaultdict(lambda: defaultdict(list))
    
    count = 0
    for doc in docs:
        d = doc.to_dict()
        club = d.get('club_name')
        hour = d.get('hour')
        
        snapshot_min = d.get('stats', {}).get('min')
        if club and hour is not None and snapshot_min is not None:
            stats[club][hour].append(snapshot_min)
            count += 1
            
    print(f"Found {count} history records for {yesterday}. Calculating daily stats...")
    
    batch = db.batch()
    batch_count = 0
    skipped_count = 0
    
    for club, hours in stats.items():
        for hour, prices in hours.items():
            min_price = min(prices)
            avg_price = sum(prices) / len(prices)
            snapshot_count = len(prices)
            
            # Doc ID: YYYYMMDD_Club_Hour
            doc_id = f"{yesterday.replace('-', '')}_{club}_{hour}"
            doc_ref = db.collection('daily_stats').document(doc_id)
            
            new_data = {
                "club_name": club,
                "date": yesterday,
                "hour": hour,
                "min_price": min_price,
                "avg_price": avg_price,
                "snapshot_count": snapshot_count,
                "updated_at": firestore.SERVER_TIMESTAMP
            }
            
            # Check if update is needed
            needs_update = True
            if doc_id in existing_map:
                existing = existing_map[doc_id]
                if (existing.get('min_price') == min_price and
                    existing.get('avg_price') == avg_price and
                    existing.get('snapshot_count') == snapshot_count):
                    needs_update = False
            
            if needs_update:
                batch.set(doc_ref, new_data)
                batch_count += 1
                
                if batch_count >= 400:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
            else:
                skipped_count += 1
                
    if batch_count > 0:
        batch.commit()
        
    print(f"Daily stats aggregation for {yesterday} completed. Updated: {batch_count}, Skipped: {skipped_count}")

if __name__ == "__main__":
    archive_history()
    
    print("Running weather ingestion as part of daily archive...")
    from ingest_weather import ingest_weather
    try:
        ingest_weather()
    except Exception as e:
        print(f"Error during weather ingestion: {e}")

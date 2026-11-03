import firebase_admin
from firebase_admin import credentials, firestore
import datetime
from datetime import timezone, timedelta
import os
from collections import defaultdict
from cleanup_old_history import cleanup_old_data

PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"
KST = timezone(timedelta(hours=9))

def init_firestore():
    from google.cloud import firestore
    from google.oauth2 import service_account
    import google.auth

    if os.path.exists(CRED_PATH):
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        credentials, project = google.auth.default()
        return firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

def archive_history():
    db = init_firestore()
    if not db:
        return

    now_kst = datetime.datetime.now(KST)
    today = now_kst.date()
    today_str = today.strftime("%Y-%m-%d")
    yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")

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
            crawl_range = data.get('crawl_range', '')
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
    snapshot_time = datetime.datetime.now(KST)

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

    if batch_count > 0:
        batch.commit()

    print(f"  → price_history 스냅샷 완료 ({batch_count}건)")

    # =========================================
    # 4. 어제 날짜 daily_stats 집계 (단기 7일 전 diff용)
    # =========================================
    aggregate_daily_stats(db, yesterday_str)

    # =========================================
    # 5. [신규] 어제 날짜 BigQuery/GCS 장기 아카이브 확정 + 날씨 결합
    # =========================================
    try:
        from weather_utils import fetch_past_weather_for_clubs
        from archive_store import finalize_day
        print(f"[archive_history] {yesterday_str} 어제 날씨 수집 중...")
        weather_dict = fetch_past_weather_for_clubs(yesterday_str)
        print(f"[archive_history] {yesterday_str} BigQuery/GCS 장기 아카이브 확정 중...")
        finalize_day(yesterday_str, weather_dict)
    except Exception as e:
        print(f"[archive_history] ⚠️ GCS/BigQuery 아카이브 확정 실패 (비치명적): {e}")

    # =========================================
    # 6. [신규] 일별 사용자 접속 통계(stats_daily) 영구 집계
    # =========================================
    try:
        aggregate_user_stats(db, yesterday_str)
    except Exception as e:
        print(f"[archive_history] ⚠️ stats_daily 집계 실패: {e}")

    # =========================================
    # 7. 오래된 데이터 정리 (cleanup)
    # =========================================
    try:
        print("Running cleanup for expired data...")
        cleanup_old_data()
    except Exception as e:
        print(f"Cleanup failed (non-fatal): {e}")

    print("History archiving completed.")

def aggregate_user_stats(db, target_date_str):
    """
    어제 날짜의 접속 로그(access_logs)를 집계하여 stats_daily/{target_date_str} 문서로 영구 보존.
    """
    print(f"Aggregating user stats for {target_date_str}...")
    logs = list(db.collection('access_logs').where('date', '==', target_date_str).stream())
    dau = len(logs)
    total_hits = sum(d.to_dict().get('hits', 1) for d in logs)

    users_approved = list(db.collection('users').where('status', '==', 'approved').stream())
    total_approved = len(users_approved)

    db.collection('stats_daily').document(target_date_str).set({
        "date": target_date_str,
        "dau": dau,
        "total_hits": total_hits,
        "total_approved_users": total_approved,
        "updated_at": firestore.SERVER_TIMESTAMP
    }, merge=True)
    print(f"  → stats_daily/{target_date_str} 완료 (DAU: {dau}, 총 승인회원: {total_approved})")

def aggregate_daily_stats(db, yesterday_str):
    print(f"Aggregating daily stats for {yesterday_str}...")
    existing_docs = db.collection('daily_stats').where('date', '==', yesterday_str).stream()
    existing_map = {}
    for doc in existing_docs:
        existing_map[doc.id] = doc.to_dict()

    docs = db.collection('price_history').where('date', '==', yesterday_str).stream()
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

    print(f"Found {count} history records for {yesterday_str}. Calculating daily stats...")
    batch = db.batch()
    batch_count = 0
    skipped_count = 0

    for club, hours in stats.items():
        for hour, prices in hours.items():
            min_price = min(prices)
            avg_price = sum(prices) / len(prices)
            snapshot_count = len(prices)

            doc_id = f"{yesterday_str.replace('-', '')}_{club}_{hour}"
            doc_ref = db.collection('daily_stats').document(doc_id)

            new_data = {
                "club_name": club,
                "date": yesterday_str,
                "hour": hour,
                "min_price": min_price,
                "avg_price": avg_price,
                "snapshot_count": snapshot_count,
                "updated_at": firestore.SERVER_TIMESTAMP
            }

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

    print(f"Daily stats aggregation for {yesterday_str} completed. Updated: {batch_count}, Skipped: {skipped_count}")

if __name__ == "__main__":
    archive_history()

    # 최초 1회 실행 시 기존 daily_stats 데이터를 GCS/BigQuery로 백필
    try:
        from archive_store import backfill_from_firestore
        backfill_from_firestore()
    except Exception as e:
        print(f"Backfill note: {e}")

    print("Running weather ingestion as part of daily archive...")
    from ingest_weather import ingest_weather
    try:
        ingest_weather()
    except Exception as e:
        print(f"Error during weather ingestion: {e}")

import firebase_admin
from firebase_admin import credentials, firestore
import datetime
from datetime import timezone, timedelta
import os

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

def cleanup_old_data():
    db = init_firestore()
    if not db:
        return

    now_kst = datetime.datetime.now(KST)
    today = now_kst.date()

    # 1. price_history 정리 (7일 전)
    cutoff_7days = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    print(f"Deleting price_history older than {cutoff_7days}...")
    docs = db.collection('price_history').where('date', '<', cutoff_7days).stream()
    batch = db.batch()
    count = 0
    deleted_price = 0
    for doc in docs:
        batch.delete(doc.reference)
        count += 1
        deleted_price += 1
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
    if count > 0:
        batch.commit()
    print(f"  → Deleted {deleted_price} price_history docs.")

    # 2. access_logs 정리 (60일 전으로 연장하여 방문통계 보존)
    cutoff_60days = (today - timedelta(days=60)).strftime("%Y-%m-%d")
    print(f"Deleting access_logs older than {cutoff_60days}...")
    access_docs = db.collection('access_logs').where('date', '<', cutoff_60days).stream()
    batch = db.batch()
    count = 0
    deleted_access = 0
    for doc in access_docs:
        batch.delete(doc.reference)
        count += 1
        deleted_access += 1
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
    if count > 0:
        batch.commit()
    print(f"  → Deleted {deleted_access} access_logs docs.")

    # 3. crawl_stats 정리 (7일 전)
    print(f"Deleting crawl_stats older than {cutoff_7days}...")
    crawl_docs = db.collection('crawl_stats').where('date', '<', cutoff_7days).stream()
    batch = db.batch()
    count = 0
    deleted_crawl = 0
    for doc in crawl_docs:
        batch.delete(doc.reference)
        count += 1
        deleted_crawl += 1
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
    if count > 0:
        batch.commit()
    print(f"  → Deleted {deleted_crawl} crawl_stats docs.")

    # 4. daily_stats 정리 (10일 -> 45일 전으로 연장, 한 번에 최대 5000개)
    cutoff_45days = (today - timedelta(days=45)).strftime("%Y-%m-%d")
    print(f"Deleting daily_stats older than {cutoff_45days} (max 5000 docs per run)...")
    daily_stats_docs = db.collection('daily_stats').where('date', '<', cutoff_45days).limit(5000).stream()
    batch = db.batch()
    count = 0
    deleted_daily = 0
    for doc in daily_stats_docs:
        batch.delete(doc.reference)
        count += 1
        deleted_daily += 1
        if count >= 400:
            batch.commit()
            batch = db.batch()
            count = 0
    if count > 0:
        batch.commit()
    print(f"  → Deleted {deleted_daily} daily_stats docs.")

    print(f"Cleanup complete. Total documents deleted: {deleted_price + deleted_access + deleted_crawl + deleted_daily}")

if __name__ == "__main__":
    cleanup_old_data()

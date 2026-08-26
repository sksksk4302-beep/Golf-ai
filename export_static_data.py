"""
export_static_data.py
Firestore에서 전체 데이터를 읽어 정적 JSON 캐시로 저장하는 스크립트.
크롤링 완료 후 실행되어, 프론트엔드가 /api/static_data 한 번만 호출하면 
모든 데이터를 가져갈 수 있도록 한다.

Incremental Export 모드:
  CRAWL_START_DAY / CRAWL_END_DAY 환경변수가 설정되어 있으면,
  해당 날짜 범위만 Firestore에서 읽고 나머지는 GCS 기존 데이터를 재사용한다.
  환경변수가 없으면 전체(D+0~D+14) Full Export를 수행한다.
"""

import os
import json
import gzip
import datetime
from datetime import timezone, timedelta
from collections import defaultdict
from google.cloud import firestore as google_firestore
from google.cloud import storage as google_storage
import google.auth

PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"
KST = timezone(timedelta(hours=9))
BUCKET_NAME = "golf-ai-480805.firebasestorage.app"

def init_firestore():
    if os.path.exists(CRED_PATH):
        from google.oauth2 import service_account
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return google_firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        credentials, project = google.auth.default()
        return google_firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

def init_storage():
    if os.path.exists(CRED_PATH):
        return google_storage.Client.from_service_account_json(CRED_PATH)
    else:
        return google_storage.Client(project=PROJECT_ID)

def get_region(address):
    if "경기" in address: return "경기"
    if "충청" in address or "충북" in address or "충남" in address: return "충청"
    if "강원" in address: return "강원"
    return "기타"

def format_price(price):
    try:
        return f"{int(price):,}"
    except:
        return str(price)

def _fetch_tee_times_from_firestore(db, target_dates):
    """Firestore에서 지정된 날짜들의 티타임을 읽어온다."""
    tee_times = []
    DATE_CHUNK = 10
    for i in range(0, len(target_dates), DATE_CHUNK):
        date_chunk = target_dates[i:i+DATE_CHUNK]
        docs = db.collection('tee_times').where('date', 'in', date_chunk).stream()
        for doc in docs:
            data = doc.to_dict()
            try:
                price = int(data.get("price", 0))
            except:
                price = 0
            tee_times.append([
                data.get("club_name", ""),
                data.get("date", ""),
                data.get("time", ""),
                data.get("hour", 0),
                price,
                data.get("source", ""),
                data.get("benefit", ""),
                data.get("url", ""),
                data.get("source_idx", "")
            ])
    return tee_times

def _download_gcs_json(blob):
    """GCS blob을 다운로드하고 gzip 압축 여부를 자동으로 처리하여 json 파싱."""
    if not blob.exists():
        return None
    raw_bytes = blob.download_as_bytes()
    try:
        decompressed = gzip.decompress(raw_bytes)
        return json.loads(decompressed.decode('utf-8'))
    except Exception:
        # Gzip 압축되지 않은 일반 텍스트인 경우
        return json.loads(raw_bytes.decode('utf-8'))

def _fetch_tee_times_from_gcs(storage_client, dates, today_str, tomorrow_str):
    """GCS에서 기존 날짜별 파일을 다운로드하여 티타임 데이터를 재사용한다."""
    tee_times = []
    bucket = storage_client.bucket(BUCKET_NAME)
    gcs_failed_dates = []
    
    # 오늘/내일은 메인 static_data.json에만 포함되어 있으므로 별도 처리
    main_dates = [d for d in dates if d in (today_str, tomorrow_str)]
    file_dates = [d for d in dates if d not in (today_str, tomorrow_str)]
    
    # 메인 파일에서 오늘/내일 데이터 추출
    if main_dates:
        try:
            main_blob = bucket.blob("static_data.json")
            main_data = _download_gcs_json(main_blob)
            if main_data:
                main_tee_times = main_data.get("tee_times", [])
                for tt in main_tee_times:
                    if tt[1] in main_dates:
                        tee_times.append(tt)
                print(f"      ✅ GCS 메인파일에서 오늘/내일 재사용: {len([t for t in tee_times])}건")
            else:
                gcs_failed_dates.extend(main_dates)
        except Exception as e:
            print(f"      ❌ GCS 메인파일 다운로드 실패: {e}")
            gcs_failed_dates.extend(main_dates)
    
    # 날짜별 파일에서 D+2~D+14 데이터
    for date_str in file_dates:
        blob = bucket.blob(f"static_data_{date_str}.json")
        try:
            date_data = _download_gcs_json(blob)
            if date_data is not None:
                tee_times.extend(date_data)
                print(f"      ✅ GCS 재사용: static_data_{date_str}.json ({len(date_data)}건)")
            else:
                print(f"      ⚠️ GCS에 static_data_{date_str}.json 없음")
                gcs_failed_dates.append(date_str)
        except Exception as e:
            print(f"      ❌ GCS 다운로드 실패 ({date_str}): {e}")
            gcs_failed_dates.append(date_str)
    
    return tee_times, gcs_failed_dates

def _fetch_daily_stats_from_firestore(db, dates_with_data):
    """Firestore에서 히스토리 데이터를 읽어온다."""
    history_dates = set()
    for d_str in dates_with_data:
        try:
            d_obj = datetime.datetime.strptime(d_str, "%Y-%m-%d").date()
            h_date = (d_obj - timedelta(days=7)).strftime("%Y-%m-%d")
            history_dates.add(h_date)
        except:
            pass
    
    all_daily_stats = []
    history_dates_list = sorted(list(history_dates))
    DATE_CHUNK = 10
    for i in range(0, len(history_dates_list), DATE_CHUNK):
        date_chunk = history_dates_list[i:i+DATE_CHUNK]
        docs = db.collection('daily_stats').where('date', 'in', date_chunk).stream()
        for doc in docs:
            data = doc.to_dict()
            try:
                min_price = int(data.get("min_price", 0))
            except:
                min_price = 0
            all_daily_stats.append([
                data.get("club_name", ""),
                data.get("date", ""),
                data.get("hour", 0),
                min_price
            ])
    return all_daily_stats

def export_data(db=None):
    if db is None:
        db = init_firestore()
    
    now_kst = datetime.datetime.now(KST)
    today = now_kst.date()
    today_str = now_kst.strftime("%Y-%m-%d")
    tomorrow_str = (now_kst + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[export_static_data] 시작: {now_kst.strftime('%Y-%m-%d %H:%M:%S KST')}")
    
    # Incremental 모드 판단
    crawl_start = os.environ.get("CRAWL_START_DAY")
    crawl_end = os.environ.get("CRAWL_END_DAY")
    is_incremental = crawl_start is not None and crawl_end is not None
    
    storage_client = init_storage()
    
    if is_incremental:
        crawl_start_day = int(crawl_start)
        crawl_end_day = int(crawl_end)
        print(f"  📦 Incremental Export 모드: D+{crawl_start_day}~D+{crawl_end_day}만 Firestore에서 읽기")
    else:
        print(f"  📦 Full Export 모드: 전체 D+0~D+14 Firestore에서 읽기")
    
    # =========================================
    # 1. 골프장 목록 (golf_clubs)
    # =========================================
    print("  [1/5] golf_clubs 읽기...")
    clubs_ref = db.collection('golf_clubs').stream()
    all_clubs_raw = []
    for doc in clubs_ref:
        club_data = doc.to_dict()
        club_data['id'] = doc.id
        all_clubs_raw.append(club_data)
    
    # 지역별 그룹핑 (index.html의 fetchClubs 로직과 동일)
    grouped_clubs = defaultdict(list)
    public_clubs = []
    for club in all_clubs_raw:
        name = club.get("name", "")
        address = club.get("address", "")
        club_info = {"name": name, "address": address}
        if "(9)" in name or "(9*2)" in name:
            public_clubs.append(club_info)
        else:
            region = get_region(address)
            grouped_clubs[region].append(club_info)
    if public_clubs:
        grouped_clubs["퍼블릭"] = public_clubs
    
    print(f"    → {len(all_clubs_raw)}개 구장")
    
    # =========================================
    # 2. 티타임 데이터 (tee_times) — Incremental 지원
    # =========================================
    print("  [2/5] tee_times 읽기...")
    
    # 전체 대상 날짜 목록 (D+0 ~ D+14)
    all_target_dates = []
    for i in range(15):
        d = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        all_target_dates.append(d)
    
    all_tee_times = []
    firestore_read_dates = []
    gcs_reuse_dates = []
    
    if is_incremental:
        # 크롤링된 날짜 범위
        crawled_dates = set()
        for i in range(crawl_start_day, crawl_end_day + 1):
            d = (today + timedelta(days=i)).strftime("%Y-%m-%d")
            crawled_dates.add(d)
        
        # 오늘/내일은 항상 Firestore에서 최신 읽기 (메인 파일에 들어가므로)
        must_read_dates = {today_str, tomorrow_str}
        
        for d in all_target_dates:
            if d in crawled_dates or d in must_read_dates:
                firestore_read_dates.append(d)
            else:
                gcs_reuse_dates.append(d)
        
        # 중복 제거 (크롤링 범위와 must_read 겹칠 수 있음)
        firestore_read_dates = sorted(set(firestore_read_dates))
        
        print(f"    → Firestore 읽기 대상: {len(firestore_read_dates)}일 ({firestore_read_dates})")
        print(f"    → GCS 재사용 대상: {len(gcs_reuse_dates)}일")
        
        # Firestore에서 변경된 날짜만 읽기
        if firestore_read_dates:
            fresh_tee_times = _fetch_tee_times_from_firestore(db, firestore_read_dates)
            all_tee_times.extend(fresh_tee_times)
            print(f"    → Firestore에서 {len(fresh_tee_times)}건 읽기 완료")
        
        # GCS에서 나머지 날짜 재사용
        if gcs_reuse_dates:
            reused_tee_times, failed_dates = _fetch_tee_times_from_gcs(
                storage_client, gcs_reuse_dates, today_str, tomorrow_str)
            all_tee_times.extend(reused_tee_times)
            print(f"    → GCS에서 {len(reused_tee_times)}건 재사용 완료")
            
            # GCS에 파일이 없었던 날짜를 Firestore 폴백으로 읽기
            if failed_dates:
                print(f"    → GCS 누락 날짜 Firestore 폴백: {failed_dates}")
                fallback_tee_times = _fetch_tee_times_from_firestore(db, failed_dates)
                all_tee_times.extend(fallback_tee_times)
                print(f"    → Firestore 폴백 {len(fallback_tee_times)}건 추가")
    else:
        # Full Export: 기존 로직 그대로
        firestore_read_dates = all_target_dates
        all_tee_times = _fetch_tee_times_from_firestore(db, all_target_dates)
    
    print(f"    → 총 {len(all_tee_times)}개 티타임 (Firestore: {len(firestore_read_dates)}일, GCS재사용: {len(gcs_reuse_dates)}일)")
    
    # available_dates 계산 (데이터가 존재하는 날짜)
    dates_with_data = set()
    for tt in all_tee_times:
        if tt[1]: # date is index 1
            dates_with_data.add(tt[1])
    available_dates = sorted(list(dates_with_data))
    
    # =========================================
    # 3. 히스토리 데이터 (daily_stats) — diff 계산용
    #    Incremental 모드: 변경된 날짜의 히스토리만 Firestore에서 읽기
    # =========================================
    print("  [3/5] daily_stats 읽기 (7일 전 데이터)...")
    
    existing_data = None  # GCS 기존 데이터 (incremental용)
    
    if is_incremental:
        # 기존 GCS 데이터를 한번만 읽기
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            main_blob = bucket.blob("static_data.json")
            existing_data = _download_gcs_json(main_blob)
        except Exception as e:
            print(f"    ⚠️ GCS 기존 데이터 읽기 실패: {e}")
        
        # 변경된 날짜에 대한 히스토리만 Firestore에서 읽기
        changed_dates = set(firestore_read_dates) & dates_with_data
        fresh_stats = _fetch_daily_stats_from_firestore(db, changed_dates)
        
        # 나머지 날짜의 히스토리는 기존 GCS에서 재사용
        if existing_data:
            existing_stats = existing_data.get("daily_stats", [])
            
            # 기존 히스토리 중 변경되지 않은 날짜에 해당하는 것만 가져오기
            existing_history_dates = set()
            for d_str in (dates_with_data - changed_dates):
                try:
                    d_obj = datetime.datetime.strptime(d_str, "%Y-%m-%d").date()
                    h_date = (d_obj - timedelta(days=7)).strftime("%Y-%m-%d")
                    existing_history_dates.add(h_date)
                except:
                    pass
            
            reused_stats = [s for s in existing_stats if s[1] in existing_history_dates]
            all_daily_stats = fresh_stats + reused_stats
            print(f"    → Firestore 신규: {len(fresh_stats)}건, GCS 재사용: {len(reused_stats)}건")
        else:
            print(f"    ⚠️ GCS 히스토리 재사용 불가, 전체 Firestore 읽기로 폴백")
            all_daily_stats = _fetch_daily_stats_from_firestore(db, dates_with_data)
    else:
        all_daily_stats = _fetch_daily_stats_from_firestore(db, dates_with_data)
    
    print(f"    → {len(all_daily_stats)}개 히스토리 레코드")
    
    # =========================================
    # 4. 날씨 데이터 (weather_forecast)
    #    Incremental 모드: 변경된 날짜의 날씨만 Firestore에서 읽기 + 기존 재사용
    # =========================================
    print("  [4/5] weather_forecast 읽기...")
    club_names = set(tt[0] for tt in all_tee_times) # club_name is index 0
    
    weather_data = {}
    
    if is_incremental:
        # 변경된 날짜의 날씨만 Firestore에서 읽기
        changed_weather_dates = set(firestore_read_dates) & dates_with_data
        weather_doc_refs = []
        for d_str in sorted(changed_weather_dates):
            d_norm = d_str.replace("-", "")
            for club in club_names:
                doc_id = f"{d_norm}_{club}"
                weather_doc_refs.append(db.collection('weather_forecast').document(doc_id))
        
        if weather_doc_refs:
            BATCH_SIZE = 500
            for i in range(0, len(weather_doc_refs), BATCH_SIZE):
                batch_refs = weather_doc_refs[i:i+BATCH_SIZE]
                docs = db.get_all(batch_refs)
                for doc in docs:
                    if doc.exists:
                        w = doc.to_dict()
                        weather_data[doc.id] = {
                            "temp_min": w.get("temp_min"),
                            "temp_max": w.get("temp_max"),
                            "precipitation_sum": w.get("precipitation_sum"),
                            "precip_prob_max": w.get("precip_prob_max"),
                            "weather_code_daily": w.get("weather_code_daily"),
                        }
        
        # 기존 GCS의 날씨 데이터 재사용
        if existing_data:
            existing_weather = existing_data.get("weather", {})
            unchanged_dates_norm = set()
            for d_str in (dates_with_data - changed_weather_dates):
                unchanged_dates_norm.add(d_str.replace("-", ""))
            
            for key, val in existing_weather.items():
                date_part = key.split("_")[0]
                if date_part in unchanged_dates_norm:
                    weather_data[key] = val
            
            print(f"    → Firestore 신규: {len(changed_weather_dates)}일, GCS 재사용: {len(unchanged_dates_norm)}일")
        else:
            print(f"    ⚠️ GCS 날씨 재사용 불가")
    else:
        # Full export: 기존 로직
        weather_doc_refs = []
        for d_str in sorted(dates_with_data):
            d_norm = d_str.replace("-", "")
            for club in club_names:
                doc_id = f"{d_norm}_{club}"
                weather_doc_refs.append(db.collection('weather_forecast').document(doc_id))
        
        if weather_doc_refs:
            BATCH_SIZE = 500
            for i in range(0, len(weather_doc_refs), BATCH_SIZE):
                batch_refs = weather_doc_refs[i:i+BATCH_SIZE]
                docs = db.get_all(batch_refs)
                for doc in docs:
                    if doc.exists:
                        w = doc.to_dict()
                        weather_data[doc.id] = {
                            "temp_min": w.get("temp_min"),
                            "temp_max": w.get("temp_max"),
                            "precipitation_sum": w.get("precipitation_sum"),
                            "precip_prob_max": w.get("precip_prob_max"),
                            "weather_code_daily": w.get("weather_code_daily"),
                        }
    
    print(f"    → {len(weather_data)}개 날씨 레코드")
    
    # =========================================
    # 5. JSON 빌드 및 저장
    # =========================================
    print("  [5/5] JSON 빌드 및 Cloud Storage 업로드...")
    
    # 5-1. 날짜별 티타임 분할
    tee_times_by_date = {}
    for tt in all_tee_times:
        date_str = tt[1] # date is index 1
        if date_str not in tee_times_by_date:
            tee_times_by_date[date_str] = []
        tee_times_by_date[date_str].append(tt)
    
    # 5-2. 오늘/내일 데이터 식별
    initial_tee_times = []
    if today_str in tee_times_by_date:
        initial_tee_times.extend(tee_times_by_date[today_str])
    if tomorrow_str in tee_times_by_date:
        initial_tee_times.extend(tee_times_by_date[tomorrow_str])
    
    static_data = {
        "clubs": dict(grouped_clubs),
        "tee_times": initial_tee_times, # 오늘/내일 데이터만 기본 포함
        "daily_stats": all_daily_stats,
        "weather": weather_data,
        "available_dates": available_dates,
        "generated_at": now_kst.strftime("%Y-%m-%d %H:%M"),
    }
    
    json_str = json.dumps(static_data, ensure_ascii=False)
    json_bytes = json_str.encode('utf-8')
    compressed_bytes = gzip.compress(json_bytes)
    
    # 5-0. 로컬 public 디렉터리에 상시 백업 저장 (Firebase Hosting / 로컬 서빙용)
    public_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'public')
    os.makedirs(public_dir, exist_ok=True)
    json_path = os.path.join(public_dir, 'static_data.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        f.write(json_str)
    
    version_data = {"generated_at": static_data["generated_at"]}
    version_json_str = json.dumps(version_data)
    with open(os.path.join(public_dir, 'version.json'), 'w', encoding='utf-8') as f:
        f.write(version_json_str)

    # Cloud Storage에 업로드 (Gzip 압축 적용으로 네트워크 대역폭 97% 절감)
    print(f"    → Cloud Storage 버킷({BUCKET_NAME})에 Gzip 압축 메타데이터 업로드 시도 (원시: {len(json_bytes)/1024/1024:.2f}MB, Gzip: {len(compressed_bytes)/1024:.1f}KB)...")
    
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob("static_data.json")
        
        # v1 백업 (폴백용)
        if blob.exists():
            backup_blob = bucket.copy_blob(blob, bucket, "static_data_fallback.json")
            backup_blob.cache_control = "public, max-age=60"
            backup_blob.content_encoding = "gzip"
            backup_blob.patch()
            backup_blob.make_public()

        blob.upload_from_string(compressed_bytes, content_type="application/json")
        blob.content_encoding = "gzip"
        blob.cache_control = "public, max-age=300, s-maxage=300"
        blob.patch()
        blob.make_public()
        print(f"    → gs://{BUCKET_NAME}/static_data.json 업로드 및 Gzip 설정 완료 ({len(compressed_bytes)/1024:.1f}KB)")
        
        # 5-3. 날짜별 티타임 파일 업로드
        print("    → 날짜별 티타임 분할 파일 업로드 중...")
        upload_count = 0
        
        if is_incremental:
            upload_target_dates = set()
            for i in range(crawl_start_day, crawl_end_day + 1):
                upload_target_dates.add((today + timedelta(days=i)).strftime("%Y-%m-%d"))
        
        for date_str, times in tee_times_by_date.items():
            if date_str == today_str or date_str == tomorrow_str:
                continue
            
            if is_incremental and date_str not in upload_target_dates:
                continue
            
            date_json = json.dumps(times, ensure_ascii=False)
            date_bytes = date_json.encode('utf-8')
            date_compressed = gzip.compress(date_bytes)

            with open(os.path.join(public_dir, f'static_data_{date_str}.json'), 'w', encoding='utf-8') as f:
                f.write(date_json)
            
            date_blob = bucket.blob(f"static_data_{date_str}.json")
            date_blob.upload_from_string(date_compressed, content_type="application/json")
            date_blob.content_encoding = "gzip"
            date_blob.cache_control = "public, max-age=300, s-maxage=300"
            date_blob.patch()
            date_blob.make_public()
            upload_count += 1
            
        print(f"    → {upload_count}개 날짜별 파일 Gzip 업로드 완료 (전체 {len(tee_times_by_date)}일)")
        
        # version.json 업로드
        version_blob = bucket.blob("version.json")
        version_blob.upload_from_string(version_json_str, content_type="application/json")
        version_blob.cache_control = "public, max-age=300, s-maxage=300"
        version_blob.patch()
        version_blob.make_public()
        print(f"    → gs://{BUCKET_NAME}/version.json 업로드 및 공개 완료")
    except Exception as e:
        print(f"    ❌ Cloud Storage 업로드 실패: {e}")
    

    print(f"[export_static_data] 완료!")
    return static_data


def main():
    # CP949 인코딩 에러 방지 (Windows/Actions 출력용)
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    
    db = init_firestore()
    export_data(db)

if __name__ == "__main__":
    main()

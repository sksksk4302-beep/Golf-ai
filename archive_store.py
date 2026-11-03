import os
import json
import gzip
import time
import datetime
from datetime import timezone, timedelta
from google.cloud import storage as google_storage
from google.cloud import firestore as google_firestore
import google.auth

PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"
BUCKET_NAME = "golf-ai-480805.firebasestorage.app"
KST = timezone(timedelta(hours=9))

def init_storage():
    if os.path.exists(CRED_PATH):
        return google_storage.Client.from_service_account_json(CRED_PATH)
    else:
        return google_storage.Client(project=PROJECT_ID)

def init_firestore():
    if os.path.exists(CRED_PATH):
        from google.oauth2 import service_account
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return google_firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        credentials, project = google.auth.default()
        return google_firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

def init_bigquery():
    try:
        from google.cloud import bigquery
        if os.path.exists(CRED_PATH):
            return bigquery.Client.from_service_account_json(CRED_PATH)
        else:
            return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        print(f"[archive_store] BigQuery client init failed: {e}")
        return None

def merge_tracker(tee_times):
    """
    크롤링 직후 수집된 티타임들로부터 구장·날짜·시간대별 관측 최저가를 GCS archive/tracker.json에 누적 병합.
    동시성 충돌을 방지하기 위해 GCS generation precondition과 재시도를 적용.
    """
    if not tee_times:
        return

    storage_client = init_storage()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob("archive/tracker.json")

    now_kst = datetime.datetime.now(KST)
    today = now_kst.date()

    # 1. 인메모리 티타임에서 이번 회차 최저가 집계
    incoming = {} # (date, club, hour) -> (min_price, src)
    for item in tee_times:
        if isinstance(item, (list, tuple)):
            if len(item) < 6:
                continue
            club = item[0]
            d_str = item[1]
            try:
                hour = int(item[3])
                price = int(item[4])
            except (ValueError, TypeError):
                continue
            src = str(item[5])
        elif isinstance(item, dict):
            club = item.get("club_name") or item.get("golf")
            d_str = item.get("date")
            try:
                hour = int(item.get("hour_num") if "hour_num" in item else item.get("hour", 0))
                price = int(item.get("price", 0))
            except (ValueError, TypeError):
                continue
            src = str(item.get("source", ""))
        else:
            continue

        if not club or not d_str or price <= 0:
            continue

        key = (d_str, club, hour)
        if key not in incoming or price < incoming[key][0]:
            incoming[key] = (price, src)

    if not incoming:
        return

    # 2. GCS에서 tracker.json 다운로드 및 generation 기반 낙관적 락 저장 (최대 4회 재시도)
    max_retries = 4
    for attempt in range(max_retries):
        try:
            raw_bytes = blob.download_as_bytes()
            generation = blob.generation or 0
            tracker_data = json.loads(raw_bytes.decode('utf-8')) if raw_bytes else {}
        except Exception:
            generation = 0
            tracker_data = {}

        # 날짜별 누적 병합
        updated_count = 0
        for (d_str, club, hour), (price, src) in incoming.items():
            if d_str not in tracker_data:
                tracker_data[d_str] = {}

            item_key = f"{club}|{hour}"
            existing = tracker_data[d_str].get(item_key)

            try:
                target_date = datetime.datetime.strptime(d_str, "%Y-%m-%d").date()
                diff_days = (target_date - today).days
            except Exception:
                diff_days = 0

            if existing is None:
                tracker_data[d_str][item_key] = {
                    "min": price,
                    "src": src,
                    "lowD": max(0, diff_days),
                    "updated_at": now_kst.strftime("%Y-%m-%d %H:%M")
                }
                updated_count += 1
            else:
                existing_min = existing.get("min", 99999999)
                if price < existing_min:
                    existing["min"] = price
                    existing["src"] = src
                    existing["lowD"] = max(0, diff_days)
                    existing["updated_at"] = now_kst.strftime("%Y-%m-%d %H:%M")
                    updated_count += 1

        # 오래된 날짜 정리 (KST 오늘 기준 7일 이상 지난 미확정 잔여 데이터 삭제)
        seven_days_ago_str = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        for old_d in list(tracker_data.keys()):
            if old_d < seven_days_ago_str:
                del tracker_data[old_d]

        # 저장 시도
        try:
            json_bytes = json.dumps(tracker_data, ensure_ascii=False).encode('utf-8')
            if generation > 0:
                blob.upload_from_string(json_bytes, content_type="application/json", if_generation_match=generation)
            else:
                blob.upload_from_string(json_bytes, content_type="application/json")
            print(f"[archive_store] tracker.json 누적 갱신 성공 ({updated_count}건 반영, {len(tracker_data)}개 날짜)")
            break
        except Exception as upload_err:
            print(f"[archive_store] tracker.json 저장 충돌/실패(시도 {attempt+1}/{max_retries}): {upload_err}")
            time.sleep(0.5 * (attempt + 1))
            if attempt == max_retries - 1:
                print("[archive_store] tracker.json 갱신 재시도 초과. 다음 크롤링에 병합됩니다.")

def finalize_day(target_date_str, weather_dict=None, overwrite=True):
    """
    어제 날짜(target_date_str)의 관측 역대 최저가 및 날씨를 확정하여
    GCS archive/bq_daily/dt=YYYY-MM-DD/data.jsonl.gz 에 저장하고 tracker에서 제거.
    """
    storage_client = init_storage()
    bucket = storage_client.bucket(BUCKET_NAME)

    daily_gcs_path = f"archive/bq_daily/dt={target_date_str}/data.jsonl.gz"
    out_blob = bucket.blob(daily_gcs_path)
    if not overwrite and out_blob.exists():
        print(f"[archive_store] {target_date_str} 아카이브가 이미 존재하여 스킵합니다.")
        return False

    blob_tracker = bucket.blob("archive/tracker.json")
    tracker_day_data = {}
    try:
        raw = blob_tracker.download_as_bytes()
        all_tracker = json.loads(raw.decode('utf-8'))
        tracker_day_data = all_tracker.get(target_date_str, {})
    except Exception:
        pass

    # tracker에 데이터가 부족한 경우 Firestore daily_stats에서 폴백
    db = init_firestore()
    final_records = {} # (club, hour) -> dict

    # 1. tracker 데이터 채우기
    for item_key, info in tracker_day_data.items():
        parts = item_key.split("|")
        if len(parts) == 2:
            club = parts[0]
            try:
                hour = int(parts[1])
            except ValueError:
                continue
            final_records[(club, hour)] = {
                "min": int(info.get("min", 0)),
                "src": str(info.get("src", "unknown")),
                "lowD": int(info.get("lowD", 0))
            }

    # 2. Firestore daily_stats 폴백 병합
    try:
        daily_docs = db.collection('daily_stats').where('date', '==', target_date_str).stream()
        for doc in daily_docs:
            d = doc.to_dict()
            club = d.get('club_name')
            hour = d.get('hour')
            min_p = d.get('min_price')
            if club and hour is not None and min_p:
                key = (club, int(hour))
                if key not in final_records:
                    final_records[key] = {
                        "min": int(min_p),
                        "src": "daily_stats_fallback",
                        "lowD": 0
                    }
                else:
                    if int(min_p) < final_records[key]["min"]:
                        final_records[key]["min"] = int(min_p)
    except Exception as e:
        print(f"[archive_store] Firestore daily_stats 조회 실패 (비치명적): {e}")

    if not final_records:
        print(f"[archive_store] {target_date_str}에 대한 저장할 최저가 데이터가 없습니다.")
        return False

    # 3. 날씨 정보가 누락된 구장은 Firestore weather_forecast에서 폴백 보충
    if weather_dict is None:
        weather_dict = {}
    missing_clubs = {club for (club, _) in final_records.keys() if club not in weather_dict}
    if missing_clubs:
        try:
            d_norm = target_date_str.replace("-", "")
            refs = [db.collection('weather_forecast').document(f"{d_norm}_{c}") for c in missing_clubs]
            for doc in db.get_all(refs):
                if doc.exists:
                    w = doc.to_dict()
                    c_name = w.get("club_name") or doc.id.split("_", 1)[-1]
                    weather_dict[c_name] = {
                        "temp_min": w.get("temp_min"),
                        "temp_max": w.get("temp_max"),
                        "precip_mm": w.get("precipitation_sum")
                    }
        except Exception:
            pass

    # 요일 계산 (0=월 ~ 6=일)
    try:
        dt_obj = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        weekday_num = dt_obj.weekday()
    except Exception:
        weekday_num = 0

    # JSONL 행 생성
    lines = []
    for (club, hour), val in sorted(final_records.items()):
        w_info = weather_dict.get(club, {})
        temp_min = w_info.get("temp_min")
        temp_max = w_info.get("temp_max")
        precip_mm = w_info.get("precipitation_sum")
        if precip_mm is None:
            precip_mm = w_info.get("precip_mm")

        record = {
            "date": target_date_str,
            "weekday": weekday_num,
            "club_name": club,
            "hour": hour,
            "min_price": val["min"],
            "source": val["src"],
            "observed_days_before": val.get("lowD", 0),
            "temp_min": float(temp_min) if temp_min is not None else None,
            "temp_max": float(temp_max) if temp_max is not None else None,
            "precip_mm": float(precip_mm) if precip_mm is not None else None,
        }
        lines.append(json.dumps(record, ensure_ascii=False))

    jsonl_str = "\n".join(lines) + "\n"
    compressed_bytes = gzip.compress(jsonl_str.encode('utf-8'))

    # GCS 업로드: archive/bq_daily/dt=YYYY-MM-DD/data.jsonl.gz
    out_blob.upload_from_string(compressed_bytes, content_type="application/gzip")
    print(f"[archive_store] ✅ {target_date_str} 아카이브 저장 완료: gs://{BUCKET_NAME}/{daily_gcs_path} ({len(lines)}건)")

    # tracker.json에서 확정된 target_date_str 제거
    try:
        raw = blob_tracker.download_as_bytes()
        all_tracker = json.loads(raw.decode('utf-8'))
        if target_date_str in all_tracker:
            del all_tracker[target_date_str]
            blob_tracker.upload_from_string(json.dumps(all_tracker, ensure_ascii=False).encode('utf-8'), content_type="application/json")
            print(f"[archive_store] tracker.json에서 {target_date_str} 정리 완료")
    except Exception:
        pass

    # BigQuery 테이블 확인 및 생성
    ensure_bigquery_table()
    return True

def ensure_bigquery_table():
    """
    BigQuery golf_archive 데이터셋과 daily_lowest_price 외부 테이블(Hive 파티션 dt=) 자동 생성.
    """
    bq_client = init_bigquery()
    if not bq_client:
        return

    try:
        from google.cloud import bigquery

        dataset_id = f"{PROJECT_ID}.golf_archive"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-northeast3" # 서울 리전
        try:
            bq_client.create_dataset(dataset, exists_ok=True)
            print(f"[archive_store] BigQuery 데이터셋 확인/생성 완료: {dataset_id}")
        except Exception as de:
            print(f"[archive_store] Dataset creation note: {de}")

        table_id = f"{dataset_id}.daily_lowest_price"

        schema = [
            bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("weekday", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("club_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("hour", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("min_price", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("observed_days_before", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("temp_min", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("temp_max", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("precip_mm", "FLOAT", mode="NULLABLE"),
        ]

        external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
        external_config.source_uris = [f"gs://{BUCKET_NAME}/archive/bq_daily/*"]
        external_config.schema = schema
        external_config.compression = "GZIP"

        # Hive 파티션 설정 (dt=YYYY-MM-DD 디렉토리 자동 감지)
        hive_options = bigquery.HivePartitioningOptions()
        hive_options.mode = "AUTO"
        hive_options.source_uri_prefix = f"gs://{BUCKET_NAME}/archive/bq_daily/"
        external_config.hive_partitioning = hive_options

        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        try:
            bq_client.get_table(table_id)
            bq_client.update_table(table, ["external_data_configuration"])
            print(f"[archive_store] BigQuery 외부 테이블 갱신 완료: {table_id}")
        except Exception:
            bq_client.create_table(table)
            print(f"[archive_store] BigQuery 외부 테이블 신규 생성 완료: {table_id}")

    except Exception as bq_err:
        print(f"[archive_store] BigQuery 테이블 자동 생성 실패 (비치명적, GCS 파일은 보존됨): {bq_err}")

def backfill_from_firestore():
    """
    Firestore daily_stats에 보관되어 있는 최근 일자들을 읽어 GCS 및 BigQuery 아카이브로 최초 1회 백필.
    백필 완료 후 archive/backfill_v1_done.json 마커를 생성하여 이후 매일 반복 읽기를 차단.
    """
    storage_client = init_storage()
    bucket = storage_client.bucket(BUCKET_NAME)
    marker_blob = bucket.blob("archive/backfill_v1_done.json")
    if marker_blob.exists():
        print("[archive_store] 백필이 이미 완료되어 있습니다 (스킵).")
        return

    db = init_firestore()
    now_kst = datetime.datetime.now(KST)
    today_str = now_kst.strftime("%Y-%m-%d")

    print("[archive_store] Firestore daily_stats 최초 1회 백필 시작...")
    dates_found = set()
    try:
        docs = db.collection('daily_stats').select(['date']).stream()
        for doc in docs:
            d_dict = doc.to_dict()
            d = d_dict.get('date')
            if d and d < today_str:
                dates_found.add(d)
    except Exception as e:
        print(f"[archive_store] daily_stats 날짜 조회 실패: {e}")
        return

    print(f"[archive_store] 백필 대상 과거 날짜 {len(dates_found)}개: {sorted(list(dates_found))}")
    from weather_utils import fetch_past_weather_for_clubs
    for target_d in sorted(list(dates_found)):
        w_dict = fetch_past_weather_for_clubs(target_d)
        finalize_day(target_d, weather_dict=w_dict, overwrite=False)

    marker_blob.upload_from_string(
        json.dumps({"completed_at": now_kst.strftime("%Y-%m-%d %H:%M:%S")}, ensure_ascii=False),
        content_type="application/json"
    )
    print("[archive_store] ✅ 최초 1회 백필 완료 및 마커 생성 완료!")

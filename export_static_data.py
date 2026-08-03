"""
export_static_data.py
Firestore에서 전체 데이터를 읽어 정적 JSON 캐시로 저장하는 스크립트.
크롤링 완료 후 실행되어, 프론트엔드가 /api/static_data 한 번만 호출하면 
모든 데이터를 가져갈 수 있도록 한다.
"""

import os
import json
import datetime
from datetime import timezone, timedelta
from collections import defaultdict
from google.cloud import firestore as google_firestore
from google.cloud import storage as google_storage
import google.auth

PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"
KST = timezone(timedelta(hours=9))

def init_firestore():
    if os.path.exists(CRED_PATH):
        from google.oauth2 import service_account
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return google_firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        credentials, project = google.auth.default()
        return google_firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

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

def export_data(db=None):
    if db is None:
        db = init_firestore()
    
    now_kst = datetime.datetime.now(KST)
    today = now_kst.date()
    print(f"[export_static_data] 시작: {now_kst.strftime('%Y-%m-%d %H:%M:%S KST')}")
    
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
    
    # alert_enabled 구장 목록 (픽업용)
    alert_clubs = set()
    for club in all_clubs_raw:
        if club.get('alert_enabled'):
            name = club.get('name')
            if name:
                alert_clubs.add(name)
    
    print(f"    → {len(all_clubs_raw)}개 구장, alert_enabled: {len(alert_clubs)}개")
    
    # =========================================
    # 2. 티타임 데이터 (tee_times) — 오늘~14일
    # =========================================
    print("  [2/5] tee_times 읽기...")
    target_dates = []
    for i in range(15):
        d = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        target_dates.append(d)
    
    all_tee_times = []
    # Firestore 'in' 쿼리는 최대 30개 → 날짜를 10개씩 묶어 쿼리
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
                
            all_tee_times.append({
                "club_name": data.get("club_name", ""),
                "date": data.get("date", ""),
                "time": data.get("time", ""),
                "hour": data.get("hour", 0),
                "price": price,
                "source": data.get("source", ""),
                "benefit": data.get("benefit", ""),
                "url": data.get("url", ""),
                "source_idx": data.get("source_idx", ""),
            })
    
    print(f"    → {len(all_tee_times)}개 티타임")
    
    # available_dates 계산 (데이터가 존재하는 날짜)
    dates_with_data = set()
    for tt in all_tee_times:
        if tt["date"]:
            dates_with_data.add(tt["date"])
    available_dates = sorted(list(dates_with_data))
    
    # =========================================
    # 3. 히스토리 데이터 (daily_stats) — diff 계산용
    # =========================================
    print("  [3/5] daily_stats 읽기 (7일 전 데이터)...")
    # 티타임이 존재하는 각 날짜에 대해 7일 전 날짜의 daily_stats 조회
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
    for i in range(0, len(history_dates_list), DATE_CHUNK):
        date_chunk = history_dates_list[i:i+DATE_CHUNK]
        docs = db.collection('daily_stats').where('date', 'in', date_chunk).stream()
        for doc in docs:
            data = doc.to_dict()
            try:
                min_price = int(data.get("min_price", 0))
            except:
                min_price = 0
                
            all_daily_stats.append({
                "club_name": data.get("club_name", ""),
                "date": data.get("date", ""),
                "hour": data.get("hour", 0),
                "min_price": min_price,
            })
    
    print(f"    → {len(all_daily_stats)}개 히스토리 레코드")
    
    # =========================================
    # 4. 날씨 데이터 (weather_forecast)
    # =========================================
    print("  [4/5] weather_forecast 읽기...")
    # 구장별, 날짜별 날씨 문서 배치 조회
    club_names = set(tt["club_name"] for tt in all_tee_times)
    weather_doc_refs = []
    weather_doc_keys = []
    for d_str in sorted(dates_with_data):
        d_norm = d_str.replace("-", "")
        for club in club_names:
            doc_id = f"{d_norm}_{club}"
            weather_doc_refs.append(db.collection('weather_forecast').document(doc_id))
            weather_doc_keys.append(doc_id)
    
    weather_data = {}
    if weather_doc_refs:
        # batch get (500개씩)
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
    print("  [5/5] JSON 빌드 및 Firestore 저장...")
    
    static_data = {
        "clubs": dict(grouped_clubs),
        "tee_times": all_tee_times,
        "daily_stats": all_daily_stats,
        "weather": weather_data,
        "available_dates": available_dates,
        "generated_at": now_kst.strftime("%Y-%m-%d %H:%M"),
    }
    
    json_str = json.dumps(static_data, ensure_ascii=False)
    
    # Cloud Storage에 업로드 (Firebase Hosting/CDN 캐싱용)
    bucket_name = "golf-ai-480805.firebasestorage.app"
    print(f"    → Cloud Storage 버킷({bucket_name})에 업로드 시도...")
    
    try:
        if os.path.exists(CRED_PATH):
            storage_client = google_storage.Client.from_service_account_json(CRED_PATH)
        else:
            storage_client = google_storage.Client(project=PROJECT_ID)
            
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("static_data.json")
        blob.upload_from_string(json_str, content_type="application/json")
        
        # public 서빙을 위해 캐시 컨트롤 설정 및 접근 권한 공개
        blob.cache_control = "public, max-age=60"
        blob.patch()
        blob.make_public()
        
        print(f"    → gs://{bucket_name}/static_data.json 업로드 및 공개 완료")
    except Exception as e:
        print(f"    ❌ Cloud Storage 업로드 실패: {e}")
        # 실패 시 로컬 파일 백업
        public_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'public')
        os.makedirs(public_dir, exist_ok=True)
        json_path = os.path.join(public_dir, 'static_data.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
        print(f"    → {json_path} 로컬 저장 백업 완료")
    
    # =========================================
    # 6. 픽업 HTML 생성 (기존 refresh_pickup 대체)
    # =========================================
    print("  [6] 픽업 HTML 생성...")
    _generate_pickup_html(db, now_kst, all_tee_times, alert_clubs, all_daily_stats)
    
    print(f"[export_static_data] 완료!")
    return static_data

def _generate_pickup_html(db, now_kst, all_tee_times, alert_clubs, all_daily_stats):
    """픽업 페이지 HTML을 생성하여 system_cache/pickup_html에 저장"""
    today_str = now_kst.strftime("%Y-%m-%d")
    tomorrow_str = (now_kst + timedelta(days=1)).strftime("%Y-%m-%d")
    current_time_str = now_kst.strftime("%H:%M")
    
    # 픽업 그룹 분류
    group_data = {
        'today_part2': {},
        'today_part3': {},
        'tomorrow_part1': {},
        'tomorrow_part2': {},
    }
    
    titles = [
        ('today_part2', f'오늘 2부 줍줍 (11-13)'),
        ('today_part3', f'오늘 3부 줍줍 (14-17)'),
        ('tomorrow_part1', f'내일 1부 줍줍 (07-10)'),
        ('tomorrow_part2', f'내일 2부 줍줍 (11-13)'),
    ]
    
    for tt in all_tee_times:
        club = tt.get('club_name')
        if club not in alert_clubs:
            continue
        
        date_str = tt.get('date')
        time_str = tt.get('time', '')
        price = tt.get('price', float('inf'))
        try:
            price = int(price)
        except:
            continue
        
        try:
            hour = int(time_str.split(':')[0])
        except:
            continue
        
        group_key = None
        is_tomorrow = False
        
        if date_str == today_str:
            if time_str < current_time_str:
                continue
            if 11 <= hour <= 13:
                group_key = 'today_part2'
            elif 14 <= hour <= 17:
                group_key = 'today_part3'
        elif date_str == tomorrow_str:
            is_tomorrow = True
            if 7 <= hour <= 10:
                group_key = 'tomorrow_part1'
            elif 11 <= hour <= 13:
                group_key = 'tomorrow_part2'
        
        if not group_key:
            continue
        
        current = group_data[group_key].get(club)
        if not current or price < current['price'] or \
           (price == current['price'] and tt.get('source') == 'teescan' and current.get('source') != 'teescan'):
            group_data[group_key][club] = {
                'price': price,
                'time': time_str,
                'source': tt.get('source', ''),
                'is_tomorrow': is_tomorrow,
            }
    
    # 히스토리 맵 구축 (diff 계산용)
    history_map = {}
    for ds in all_daily_stats:
        h_club = ds.get('club_name')
        h_hour = ds.get('hour')
        h_price = ds.get('min_price')
        h_date = ds.get('date', '')
        if h_club and h_hour is not None:
            history_map[(h_club, str(h_hour), h_date)] = h_price
            history_map[(h_club, int(h_hour), h_date)] = h_price
    
    # 그룹별 아이템 빌드
    groups_out = []
    has_data = False
    
    for key, title in titles:
        club_mins = group_data[key]
        if not club_mins:
            continue
        
        has_data = True
        items = []
        for club in sorted(club_mins.keys(), key=lambda c: club_mins[c]['price']):
            info = club_mins[club]
            source_kr = "골팡" if info['source'] == 'golfpang' else ("티스캐너" if info['source'] == 'teescan' else info['source'])
            
            # diff 계산 — 해당 티타임 날짜의 7일 전 히스토리 조회
            hour_str = str(int(info['time'].split(':')[0]))
            tt_date = today_str if not info['is_tomorrow'] else tomorrow_str
            try:
                history_date = (datetime.datetime.strptime(tt_date, "%Y-%m-%d").date() - timedelta(days=7)).strftime("%Y-%m-%d")
            except:
                history_date = ""
            
            hist_price = history_map.get((club, hour_str, history_date))
            if hist_price is None:
                hist_price = history_map.get((club, int(hour_str), history_date))
            diff = (info['price'] - hist_price) if hist_price else 0
            
            items.append({
                'club': club,
                'time': info['time'],
                'is_tomorrow': info['is_tomorrow'],
                'formatted_price': format_price(info['price']),
                'source_kr': source_kr,
                'diff': diff,
            })
        groups_out.append({'title': title, 'items': items})
    
    # HTML 생성
    html = _build_pickup_html(groups_out, has_data, now_kst.strftime("%Y-%m-%d %H:%M"))
    
    # Firestore에 저장
    db.collection('system_cache').document('pickup_html').set({
        'html': html,
        'updated_at': now_kst,
    })
    print(f"    → pickup_html 저장 완료 (갱신시간: {now_kst.strftime('%H:%M')})")

def _build_pickup_html(groups, has_data, last_updated):
    """픽업 페이지 HTML을 문자열로 직접 생성"""
    
    items_html = ""
    if not has_data:
        items_html = '<div class="group-card"><div class="empty-msg">조건에 맞는 잔여 티타임이 없습니다.</div></div>'
    else:
        for group in groups:
            if not group['items']:
                continue
            rows = ""
            for item in group['items']:
                tomorrow_html = '<span class="tomorrow">[내일]</span>' if item['is_tomorrow'] else ''
                
                diff_html = ""
                if item['diff'] and item['diff'] > 0:
                    diff_html = f'<span class="diff-badge up">▲{item["diff"]:,}</span>'
                elif item['diff'] and item['diff'] < 0:
                    diff_html = f'<span class="diff-badge down">▼{abs(item["diff"]):,}</span>'
                
                rows += f"""
                        <li class="club-item">
                            <div class="club-time">
                                {tomorrow_html}
                                {item['time']}
                            </div>
                            <div class="club-name">{item['club']}</div>
                            <div class="club-price">
                                {item['formatted_price']}원
                                {diff_html}
                            </div>
                            <div class="club-source">{item['source_kr']}</div>
                        </li>"""
            
            items_html += f"""
                <div class="group-card">
                    <h2 class="group-title"><span>🚀</span> {group['title']}</h2>
                    <ul class="club-list">{rows}
                    </ul>
                </div>"""
    
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>시간대별 티타임 줍줍</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; background-color: #f6f8fa; color: #24292f; }}
        .header {{ background-color: #24292f; color: white; padding: 15px 20px; text-align: center; }}
        .header h1 {{ margin: 0; font-size: 1.2rem; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 15px; }}
        .group-card {{ background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.12); margin-bottom: 20px; overflow: hidden; }}
        .group-title {{ background: #f6f8fa; border-bottom: 1px solid #d0d7de; padding: 12px 15px; font-weight: 600; font-size: 1.05rem; margin: 0; display: flex; align-items: center; }}
        .group-title span {{ margin-right: 8px; }}
        .club-list {{ list-style: none; margin: 0; padding: 0; }}
        .club-item {{ border-bottom: 1px solid #e1e4e8; padding: 12px 15px; display: grid; grid-template-columns: 60px 1fr auto 60px; gap: 10px; align-items: center; }}
        .club-item:last-child {{ border-bottom: none; }}
        .club-time {{ font-size: 1.15rem; font-weight: 700; color: #24292f; text-align: left; }}
        .club-time .tomorrow {{ font-size: 0.75rem; color: #d29922; display: block; margin-bottom: 2px; line-height: 1; }}
        .club-name {{ font-weight: 600; color: #0969da; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-size: 1.05rem; }}
        .club-price {{ font-weight: 700; font-size: 1.05rem; color: #cf222e; text-align: right; }}
        .club-source {{ font-size: 0.95rem; font-weight: 500; color: #57606a; text-align: right; }}
        .empty-msg {{ padding: 20px; text-align: center; color: #57606a; font-size: 0.9rem; }}
        @media (max-width: 480px) {{
            .club-item {{ grid-template-columns: 55px 1fr auto 55px; gap: 6px; padding: 12px 10px; }}
            .club-time {{ font-size: 1.05rem; }}
            .club-name {{ font-size: 0.95rem; }}
            .club-price {{ font-size: 0.95rem; }}
            .club-source {{ font-size: 0.85rem; }}
        }}
        .diff-badge {{ font-size: 0.7rem; margin-left: 2px; font-weight: 500; }}
        .diff-badge.up {{ color: #E57373; }}
        .diff-badge.down {{ color: #64B5F6; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 시간대별 최저가 티타임 줍줍</h1>
    </div>
    <div class="container">
        <div style="background-color: #fff3cd; color: #856404; padding: 12px; border-radius: 6px; margin-bottom: 20px; text-align: center; font-size: 0.95rem; font-weight: 600; border: 1px solid #ffeeba;">
            [안내] 본 페이지의 데이터는 카카오톡 발송 시점 데이터로 고정됩니다. (최종 갱신: {last_updated})
        </div>
        {items_html}
    </div>
</body>
</html>"""


def main():
    # CP949 인코딩 에러 방지 (Windows/Actions 출력용)
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    
    db = init_firestore()
    export_data(db)

if __name__ == "__main__":
    main()

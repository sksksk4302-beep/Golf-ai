from flask import Flask, render_template, request, jsonify, send_from_directory, redirect
from flask_cors import CORS
from datetime import datetime, timedelta
import os
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import firestore as google_firestore
import google.auth
from collections import defaultdict
import json
from cachetools import cached, TTLCache

# Initialize Caches
tee_times_cache = TTLCache(maxsize=200, ttl=300) # 5 min cache
history_cache = TTLCache(maxsize=100, ttl=3600)  # 1 hour cache
dates_cache = TTLCache(maxsize=20, ttl=300)      # 5 min cache

app = Flask(__name__)
CORS(app)

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

@app.after_request
def add_header(response):
    if not request.path.endswith('.json'):
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
    return response

# Initialize Firestore
def init_firestore():
    if os.path.exists(CRED_PATH):
        from google.oauth2 import service_account
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return google_firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        credentials, project = google.auth.default()
        return google_firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

db = init_firestore()

# Memory cache for IP log debouncing (60s cooldown per IP)
_ip_log_cooldown = {}

def log_access(ip, is_pickup=False):
    try:
        from datetime import timezone
        KST = timezone(timedelta(hours=9))
        now = datetime.now(KST)
        
        # Debounce check: skip if logged within last 60 seconds
        cache_key = f"{ip}_{is_pickup}"
        last_logged = _ip_log_cooldown.get(cache_key)
        if last_logged and (now - last_logged).total_seconds() < 60:
            return  # Skip duplicate write within 60s
            
        _ip_log_cooldown[cache_key] = now
        
        # Clean up old keys from cooldown dict if it gets too large
        if len(_ip_log_cooldown) > 5000:
            cutoff = now - timedelta(seconds=120)
            _ip_log_cooldown.clear()
        
        date_str = now.strftime('%Y-%m-%d')
        doc_id = f"{date_str}_{ip.replace('.', '_')}"
        
        doc_ref = db.collection('access_logs').document(doc_id)
        update_data = {
            "date": date_str,
            "ip": ip,
            "last_active": google_firestore.SERVER_TIMESTAMP
        }
        if is_pickup:
            update_data["pickup_hits"] = google_firestore.Increment(1)
        else:
            update_data["hits"] = google_firestore.Increment(1)
            
        doc_ref.set(update_data, merge=True)
    except Exception as e:
        print(f"Failed to log access: {e}")

from club_utils import get_golf_clubs

def get_region(address):
    if "경기" in address: return "경기"
    if "충청" in address or "충북" in address or "충남" in address: return "충청"
    if "강원" in address: return "강원"
    return "기타"

# Cached proxy URL (avoids Firestore read on every page load)
_proxy_cache = {'url': None, 'timestamp': None}
def get_proxy_worker_url():
    from datetime import timezone
    KST = timezone(timedelta(hours=9))
    now = datetime.now(KST)
    if _proxy_cache['url'] and _proxy_cache['timestamp']:
        if (now - _proxy_cache['timestamp']).total_seconds() < 3600:  # 1 hour
            return _proxy_cache['url']
    try:
        doc = db.collection('config').document('proxy').get()
        if doc.exists:
            url = doc.to_dict().get('url', '')
            if url:
                _proxy_cache['url'] = url
                _proxy_cache['timestamp'] = now
                return url
    except Exception as e:
        print(f"Failed to fetch proxy from Firestore: {e}")
    
    # Fallback to env variables
    fallback = os.environ.get("GPANG_PROXY_WORKER") or os.environ.get("TEESCAN_PROXY_URL") or ""
    _proxy_cache['url'] = fallback
    _proxy_cache['timestamp'] = now
    return fallback

@app.route("/")
def index():
    try:
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        if ip:
            client_ip = ip.split(",")[0].strip()
            log_access(client_ip, is_pickup=False)
    except Exception as e:
        print(f"Index access logging failed: {e}")
        
    proxy_url = get_proxy_worker_url()
    return render_template("index.html", proxy_url=proxy_url)



@app.route("/pickups")
def pickups():
    try:
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        if ip:
            client_ip = ip.split(",")[0].strip()
            log_access(client_ip, is_pickup=True)
    except Exception as e:
        pass

    doc = db.collection('system_cache').document('pickup_html').get()
    if doc.exists:
        return doc.to_dict().get('html', "Cache generation error.")
    else:
        return "데이터 갱신 중입니다. 잠시 후 다시 시도해주세요.", 404

@app.route("/version.json")
def serve_version_json():
    public_dir = os.path.join(app.root_path, "public")
    if os.path.exists(os.path.join(public_dir, "version.json")):
        res = send_from_directory(public_dir, "version.json", mimetype="application/json")
        res.headers["Cache-Control"] = "public, max-age=300"
        return res
    return redirect("https://storage.googleapis.com/golf-ai-480805.firebasestorage.app/version.json")

@app.route("/static_data.json")
def serve_static_data_json():
    public_dir = os.path.join(app.root_path, "public")
    if os.path.exists(os.path.join(public_dir, "static_data.json")):
        res = send_from_directory(public_dir, "static_data.json", mimetype="application/json")
        res.headers["Cache-Control"] = "public, max-age=300"
        return res
    return redirect("https://storage.googleapis.com/golf-ai-480805.firebasestorage.app/static_data.json")

@app.route("/static_data_fallback.json")
def serve_static_data_fallback_json():
    public_dir = os.path.join(app.root_path, "public")
    if os.path.exists(os.path.join(public_dir, "static_data.json")):
        res = send_from_directory(public_dir, "static_data.json", mimetype="application/json")
        res.headers["Cache-Control"] = "public, max-age=300"
        return res
    return redirect("https://storage.googleapis.com/golf-ai-480805.firebasestorage.app/static_data_fallback.json")

@app.route("/static_data_<date_str>.json")
def serve_static_data_date_json(date_str):
    filename = f"static_data_{date_str}.json"
    public_dir = os.path.join(app.root_path, "public")
    if os.path.exists(os.path.join(public_dir, filename)):
        res = send_from_directory(public_dir, filename, mimetype="application/json")
        res.headers["Cache-Control"] = "public, max-age=300"
        return res
    return redirect(f"https://storage.googleapis.com/golf-ai-480805.firebasestorage.app/{filename}")

@app.route("/api/static_data")
def get_static_data():
    """정적 데이터 캐시 반환 — 프론트엔드가 초기 로딩 시 1회만 호출"""
    doc = db.collection('system_cache').document('static_data').get()
    if doc.exists:
        return jsonify(doc.to_dict())
    else:
        return jsonify({"error": "Data not ready. Run export_static_data.py first."}), 404

@app.route("/api/booking-contact", methods=["GET"])
def get_booking_contact():
    idx = request.args.get("idx")
    if not idx:
        return jsonify({"error": "Missing idx"}), 400
        
    try:
        from crawler_utils import _make_session, AJAX_HEADERS, _get_url
        from bs4 import BeautifulSoup
        
        raw_url = f"https://www.golfpang.com/web/round/booking_addcon.do?idx={idx}"
        url = _get_url(raw_url)
        
        with _make_session() as s:
            r = s.get(url, headers=AJAX_HEADERS, timeout=(5, 10), verify=False)
            r.encoding = 'utf-8'
            
            soup = BeautifulSoup(r.text, "html.parser")
            nickname_span = soup.select_one(".nickname")
            phone_span = soup.select_one(".phone")
            
            manager = nickname_span.get_text(strip=True) if nickname_span else ""
            phone = phone_span.get_text(strip=True) if phone_span else ""
            
            return jsonify({
                "manager": manager,
                "phone": phone
            })
    except Exception as e:
        print(f"Error fetching booking contact for idx {idx}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/clubs", methods=["GET", "POST", "PUT", "DELETE"])
def api_admin_clubs():
    try:
        if request.method == "GET":
            # Fetch all clubs from Firestore
            clubs = get_golf_clubs(db, force_refresh=True)
            
            # Aggregate stats from today's tee_times
            from datetime import timezone, timedelta, datetime
            KST = timezone(timedelta(hours=9))
            today_str = datetime.now(KST).strftime('%Y-%m-%d')
            
            # Simple stats gathering
            tee_times_ref = db.collection('tee_times').where('date', '>=', today_str).stream()
            club_stats = {}
            for doc in tee_times_ref:
                data = doc.to_dict()
                c_name = data.get('club_name')
                src = data.get('source')
                if not c_name or not src: continue
                if c_name not in club_stats:
                    club_stats[c_name] = {'ts': False, 'gp': False}
                if src == 'teescan': club_stats[c_name]['ts'] = True
                if src == 'golfpang': club_stats[c_name]['gp'] = True
                
            # Merge stats into clubs
            for c in clubs:
                c_name = c.get('name')
                c['status'] = club_stats.get(c_name, {'ts': False, 'gp': False})
                
            return jsonify(clubs)
            
        elif request.method == "POST":
            # Add new club
            data = request.json
            name = data.get('name', '').strip()
            if not name:
                return jsonify({"error": "Name is required"}), 400
                
            # Save to Firestore
            doc_ref = db.collection('golf_clubs').document(name)
            if doc_ref.get().exists:
                return jsonify({"error": "Club already exists"}), 400
                
            doc_ref.set(data)
            get_golf_clubs(db, force_refresh=True) # refresh cache
            return jsonify({"success": True, "message": "Added successfully"})
            
        elif request.method == "PUT":
            # Update existing club
            data = request.json
            old_name = data.get('old_name', '').strip()
            name = data.get('name', '').strip()
            if not old_name or not name:
                return jsonify({"error": "Name is required"}), 400
                
            if old_name != name:
                # Rename means creating new and deleting old
                db.collection('golf_clubs').document(name).set(data)
                db.collection('golf_clubs').document(old_name).delete()
            else:
                db.collection('golf_clubs').document(name).set(data, merge=True)
                
            get_golf_clubs(db, force_refresh=True) # refresh cache
            return jsonify({"success": True, "message": "Updated successfully"})
            
        elif request.method == "DELETE":
            # Delete club
            name = request.json.get('name', '').strip()
            if not name:
                return jsonify({"error": "Name is required"}), 400
                
            db.collection('golf_clubs').document(name).delete()
            get_golf_clubs(db, force_refresh=True) # refresh cache
            return jsonify({"status": "success", "message": "Updated successfully"})
    except Exception as e:
        print(f"Admin API Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/admin/cdn_fallback_log", methods=["POST"])
def log_cdn_fallback():
    try:
        data = request.get_json() or {}
        filename = data.get("filename", "unknown")
        
        db.collection('system_logs').document('cdn_status').set({
            'filename': filename,
            'timestamp': firestore.SERVER_TIMESTAMP,
            'ip_prefix': request.remote_addr[:9] if request.remote_addr else 'unknown'
        }, merge=True)
        return jsonify({"status": "ok"})
    except Exception as e:
        print(f"CDN log error: {e}")
        return jsonify({"status": "error"}), 500

@app.route("/admin/status")
@app.route("/admin/stats")
def admin_stats():
    try:
        from datetime import timezone
        KST = timezone(timedelta(hours=9))
        
        # 1. Fetch user access logs
        docs = db.collection('access_logs').order_by('date', direction=google_firestore.Query.DESCENDING).limit(1000).stream()
        
        rows_html = ""
        for d in docs:
            data = d.to_dict()
            last_active = data.get('last_active')
            last_active_str = "-"
            if last_active:
                if hasattr(last_active, 'astimezone'):
                    kst_time = last_active.astimezone(KST)
                    last_active_str = kst_time.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    last_active_str = str(last_active)
            
            uid_str = data.get('uid') or data.get('ip') or 'Unknown'
            # 익명 로그인 UID는 보통 28자입니다. 앞 6자리만 보여줘도 유저 구분에 충분합니다.
            short_uid = uid_str[:6] + ".." if len(uid_str) > 15 else uid_str
            
            os_info = data.get('os', '')
            if os_info:
                short_uid += f" <span style='color:#6e7781; font-size:0.8rem;'>({os_info})</span>"
                    
            rows_html += f"""
            <tr style="border-bottom: 1px solid #e1e4e8;">
                <td style="padding: 14px 16px; font-weight: bold; color: #24292f;">{data.get('date')}</td>
                <td style="padding: 14px 16px; font-family: monospace; color: #0969da; font-weight: 600;">{short_uid}</td>
                <td style="padding: 14px 16px; font-weight: bold; text-align: center; color: #1f2328;">{data.get('hits', 0):,} 회</td>
                <td style="padding: 14px 16px; font-weight: bold; text-align: center; color: #cf222e;">{data.get('pickup_hits', 0):,} 회</td>
                <td style="padding: 14px 16px; color: #57606a; font-size: 0.9rem;">{last_active_str}</td>
            </tr>
            """

        # 2. Fetch crawler statistics
        crawl_docs = db.collection('crawl_stats').order_by('completed_at', direction=google_firestore.Query.DESCENDING).limit(50).stream()
        
        crawl_rows_html = ""
        for c_doc in crawl_docs:
            c_data = c_doc.to_dict()
            completed_at = c_data.get('completed_at')
            completed_at_str = "-"
            if completed_at:
                if hasattr(completed_at, 'astimezone'):
                    completed_at_kst = completed_at.astimezone(KST)
                    completed_at_str = completed_at_kst.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    completed_at_str = str(completed_at)
                    
            status_val = c_data.get('status', 'success')
            status_badge = ""
            if status_val == 'success':
                status_badge = '<span class="badge success">성공</span>'
            else:
                status_badge = '<span class="badge fail">오류</span>'
                
            tier_val = c_data.get('tier', 'Unknown')
            
            crawl_rows_html += f"""
            <tr style="border-bottom: 1px solid #e1e4e8;">
                <td style="padding: 14px 16px; font-weight: bold; color: #24292f;">{completed_at_str}</td>
                <td style="padding: 14px 16px;"><span class="badge tier">{tier_val}</span></td>
                <td style="padding: 14px 16px; color: #57606a; font-size: 0.9rem;">{c_data.get('crawl_range', '-')}</td>
                <td style="padding: 14px 16px; font-weight: bold; text-align: center; color: #0969da;">{c_data.get('golfpang_total', 0):,} 건</td>
                <td style="padding: 14px 16px; font-weight: bold; text-align: center; color: #1a7f37;">{c_data.get('teescan_total', 0):,} 건</td>
                <td style="padding: 14px 16px; text-align: center;">{status_badge}</td>
            </tr>
            """

        # 3. Calculate metrics for today
        now_kst = datetime.now(KST)
        today_str = now_kst.strftime('%Y-%m-%d')
        today_crawl_docs = db.collection('crawl_stats').where('date', '==', today_str).stream()
        
        today_gp_sum = 0
        today_ts_sum = 0
        today_crawls_count = 0
        has_failure_today = False
        
        for doc in today_crawl_docs:
            data = doc.to_dict()
            today_gp_sum += data.get('golfpang_total', 0)
            today_ts_sum += data.get('teescan_total', 0)
            today_crawls_count += 1
            if data.get('status') != 'success':
                has_failure_today = True

        # 4. Get last crawler state
        last_crawl_docs = db.collection('crawl_stats').order_by('completed_at', direction=google_firestore.Query.DESCENDING).limit(1).stream()
        last_crawl = None
        for doc in last_crawl_docs:
            last_crawl = doc.to_dict()

        if last_crawl:
            lc_time = last_crawl.get('completed_at')
            if lc_time and hasattr(lc_time, 'astimezone'):
                lc_kst = lc_time.astimezone(KST)
                last_crawl_time_str = lc_kst.strftime('%Y-%m-%d %H:%M:%S KST')
            else:
                last_crawl_time_str = str(lc_time) if lc_time else "-"
            last_crawl_status = last_crawl.get('status', 'success')
            last_crawl_gp = last_crawl.get('golfpang_total', 0)
            last_crawl_ts = last_crawl.get('teescan_total', 0)
            last_crawl_tier = last_crawl.get('tier', 'Unknown')
        else:
            last_crawl_time_str = "기록 없음"
            last_crawl_status = "unknown"
            last_crawl_gp = 0
            last_crawl_ts = 0
            last_crawl_tier = "-"

        # Build Status Indicator Card classes
        status_card_class = "info"
        status_text = "기록 없음"
        if last_crawl_status == "success":
            status_card_class = "success"
            status_text = "정상동작 중"
        elif last_crawl_status == "partial_fail":
            status_card_class = "warning"
            status_text = "오류 감지"

        # 5. Check CDN Fallback Status
        cdn_status_doc = db.collection('system_logs').document('cdn_status').get()
        cdn_status_html = '<span class="badge success">정상 (GCS CDN)</span>'
        if cdn_status_doc.exists:
            cdn_data = cdn_status_doc.to_dict()
            last_fallback = cdn_data.get('timestamp')
            if last_fallback:
                # If there was a fallback in the last 2 hours, show warning
                if hasattr(last_fallback, 'astimezone'):
                    time_diff = datetime.now(timezone.utc) - last_fallback
                else:
                    time_diff = timedelta(days=99) # fallback if timezone naive
                    
                if time_diff.total_seconds() < 7200:
                    cdn_status_html = f'<span class="badge fail">오류 (Cloud Run 폴백 중)</span> <span style="font-size:0.8rem;color:#cf222e;">마지막 발생: {last_fallback.astimezone(KST).strftime("%H:%M")}</span>'

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Golf AI - 관리자 대시보드</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
            <style>
                body {{
                    font-family: 'Inter', sans-serif;
                    background-color: #f6f8fa;
                    margin: 0;
                    padding: 30px 20px;
                    color: #24292f;
                }}
                .container {{
                    max-width: 1000px;
                    margin: 0 auto;
                    background: white;
                    padding: 35px;
                    border-radius: 20px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.03);
                    border: 1px solid #e1e4e8;
                }}
                header {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 30px;
                }}
                h1 {{
                    font-size: 1.6rem;
                    color: #1f2328;
                    margin: 0;
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }}
                .badge-header {{
                    background: #ddf4ff;
                    color: #0969da;
                    font-size: 0.8rem;
                    padding: 6px 12px;
                    border-radius: 20px;
                    font-weight: 600;
                }}
                /* Summary Cards */
                .summary-cards {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                    gap: 20px;
                    margin-bottom: 35px;
                }}
                .card {{
                    background: #ffffff;
                    border: 1px solid #e1e4e8;
                    border-radius: 14px;
                    padding: 20px;
                    box-shadow: 0 3px 12px rgba(0,0,0,0.01);
                    display: flex;
                    flex-direction: column;
                }}
                .card-title {{
                    font-size: 0.85rem;
                    color: #57606a;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    margin-bottom: 10px;
                }}
                .card-value {{
                    font-size: 1.8rem;
                    font-weight: 700;
                    color: #24292f;
                    margin-bottom: 6px;
                }}
                .card-desc {{
                    font-size: 0.8rem;
                    color: #57606a;
                    margin-top: auto;
                }}
                .card.success {{ border-left: 5px solid #1a7f37; }}
                .card.warning {{ border-left: 5px solid #cf222e; }}
                .card.info {{ border-left: 5px solid #0969da; }}
                
                /* Tabs */
                .tabs {{
                    display: flex;
                    gap: 8px;
                    border-bottom: 1px solid #d0d7de;
                    margin-bottom: 25px;
                }}
                .tab-btn {{
                    padding: 10px 20px;
                    font-size: 0.95rem;
                    font-weight: 600;
                    color: #57606a;
                    background: none;
                    border: none;
                    cursor: pointer;
                    border-bottom: 2px solid transparent;
                    transition: all 0.2s ease;
                }}
                .tab-btn:hover {{
                    color: #24292f;
                    background-color: #f6f8fa;
                }}
                .tab-btn.active {{
                    color: #0969da;
                    border-bottom: 2px solid #0969da;
                }}
                
                /* Tables */
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                th {{
                    background-color: #f6f8fa;
                    color: #57606a;
                    font-weight: 600;
                    text-align: left;
                    padding: 14px 16px;
                    font-size: 0.9rem;
                    border-bottom: 1px solid #d0d7de;
                }}
                td {{
                    padding: 14px 16px;
                    font-size: 0.95rem;
                    color: #24292f;
                }}
                tr:hover {{
                    background-color: #f6f8fa;
                }}
                
                /* Badges */
                .badge {{
                    display: inline-block;
                    padding: 4px 10px;
                    font-size: 0.75rem;
                    font-weight: 600;
                    border-radius: 2em;
                }}
                .badge.success {{
                    background-color: #dafbe1;
                    color: #1a7f37;
                }}
                .badge.fail {{
                    background-color: #ffebe9;
                    color: #cf222e;
                }}
                .badge.tier {{
                    background-color: #ddf4ff;
                    color: #0969da;
                }}
                
                .responsive-table {{
                    overflow-x: auto;
                    border: 1px solid #e1e4e8;
                    border-radius: 12px;
                    background: white;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <header>
                    <h1>📊 Golf AI 관리자 대시보드</h1>
                    <span class="badge-header">실시간 시스템 모니터</span>
                </header>
                
                <!-- Summary Metrics Cards -->
                <div class="summary-cards">
                    <div class="card {status_card_class}">
                        <div class="card-title">수집기 작동 상태</div>
                        <div class="card-value">{status_text}</div>
                        <div class="card-desc">최근 1시간 내 이상 여부 점검</div>
                    </div>
                    <div class="card success">
                        <div class="card-title">골팡 오늘 수집</div>
                        <div class="card-value">{today_gp_sum:,} 건</div>
                        <div class="card-desc">금일 총 {today_crawls_count}회 수집 동작 완료</div>
                    </div>
                    <div class="card success">
                        <div class="card-title">티스캐너 오늘 수집</div>
                        <div class="card-value">{today_ts_sum:,} 건</div>
                        <div class="card-desc">최근 수집: {last_crawl_time_str.split(' ')[0]}</div>
                    </div>
                </div>
                
                <!-- Tab Headers -->
                <div class="tabs">
                    <button class="tab-btn active" onclick="openTab('crawls')">🔄 크롤링 현황</button>
                    <button class="tab-btn" onclick="openTab('access')">👤 사용자 접속 통계</button>
                    <button class="tab-btn" onclick="openTab('clubs')">🏌️ 구장 매핑 관리</button>
                </div>
                
                <!-- Tab Contents -->
                <div id="tab-crawls" class="tab-content">
                    <div class="responsive-table">
                        <table>
                            <thead>
                                <tr>
                                    <th>완료 시간 (KST)</th>
                                    <th>수집 분류 (Tier)</th>
                                    <th>수집 기간</th>
                                    <th style="text-align: center;">골팡 수집</th>
                                    <th style="text-align: center;">티스캐너 수집</th>
                                    <th style="text-align: center;">최종 상태</th>
                                </tr>
                            </thead>
                            <tbody>
                                {crawl_rows_html if crawl_rows_html else '<tr><td colspan="6" style="text-align:center; padding:30px; color:#888;">크롤링 수집 기록이 없습니다.</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                </div>
                
                <div id="tab-access" class="tab-content" style="display: none;">
                    <div class="responsive-table">
                        <table>
                            <thead>
                                <tr>
                                    <th>날짜</th>
                                    <th>사용자 IP</th>
                                    <th style="text-align: center;">메인 조회 횟수</th>
                                    <th style="text-align: center;">상세보기(Pickup) 횟수</th>
                                    <th>마지막 활동 시간 (KST)</th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows_html if rows_html else '<tr><td colspan="4" style="text-align:center; padding:30px; color:#888;">접속 기록이 없습니다.</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                </div>
                
                <div id="tab-clubs" class="tab-content" style="display: none;">
                    <div style="margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center;">
                        <h3 style="margin: 0; font-size: 1.1rem; color: #24292f;">구장 목록 및 매핑</h3>
                        <button onclick="addClubRow()" style="padding: 6px 12px; background: #2da44e; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600;">+ 신규 구장 추가</button>
                    </div>
                    <div class="responsive-table">
                        <table>
                            <thead>
                                <tr>
                                    <th>구장명</th>
                                    <th>지역(주소)</th>
                                    <th style="text-align: center;">티스캐너 SEQ</th>
                                    <th style="text-align: center;">골팡 코드</th>
                                    <th style="text-align: center;">수집 상태(오늘)</th>
                                    <th style="text-align: center;">🔔 알림</th>
                                    <th style="text-align: center;">관리</th>
                                </tr>
                            </thead>
                            <tbody id="clubs-tbody">
                                <tr><td colspan="6" style="text-align:center; padding:30px;">로딩 중...</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
            
            <script>
                function openTab(tabName) {{
                    document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
                    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
                    document.getElementById('tab-' + tabName).style.display = 'block';
                    event.currentTarget.classList.add('active');
                }}
                
                // --- Clubs Management Logic ---
                let clubsData = [];
                async function loadClubs() {{
                    try {{
                        const res = await fetch('/api/admin/clubs', {{ cache: 'no-store' }});
                        clubsData = await res.json();
                        renderClubs();
                    }} catch (e) {{
                        document.getElementById('clubs-tbody').innerHTML = '<tr><td colspan="6" style="text-align:center;color:red;">데이터 로딩 실패</td></tr>';
                    }}
                }}
                
                function renderClubs() {{
                    const tbody = document.getElementById('clubs-tbody');
                    tbody.innerHTML = '';
                    if (clubsData.length === 0) {{
                        tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;">구장 데이터가 없습니다.</td></tr>';
                        return;
                    }}
                    clubsData.forEach((club, index) => {{
                        const tr = document.createElement('tr');
                        const tsBadge = club.status.ts ? '<span style="color:green;font-weight:bold;">✅ TS</span>' : '<span style="color:red;font-weight:bold;">❌ TS</span>';
                        const gpBadge = club.status.gp ? '<span style="color:green;font-weight:bold;">✅ GP</span>' : '<span style="color:red;font-weight:bold;">❌ GP</span>';
                        const statusHtml = `${{tsBadge}} / ${{gpBadge}}`;
                        const checkedStr = club.alert_enabled ? 'checked' : '';
                        const alertHtml = `<input type="checkbox" onchange="toggleAlert(${{index}})" ${{checkedStr}} style="cursor:pointer; width:18px; height:18px;">`;
                        
                        tr.innerHTML = `
                            <td>${{club.name}}</td>
                            <td><span style="font-size:0.8rem;color:#666;">${{club.address || ''}}</span></td>
                            <td style="text-align:center;">${{club.seq ? `<a href="https://www.teescanner.com/booking/detail?golfclub_seq=${{club.seq}}" target="_blank" style="color:#0969da; text-decoration:underline;">${{club.seq}}</a>` : '-'}}</td>
                            <td style="text-align:center;">${{club.Golpang_code || '-'}}</td>
                            <td style="text-align:center;">${{statusHtml}}</td>
                            <td style="text-align:center;">${{alertHtml}}</td>
                            <td style="text-align:center;">
                                <button onclick="editClub(${{index}})" style="padding:4px 8px;cursor:pointer;">수정</button>
                                <button onclick="deleteClub('${{club.name}}')" style="padding:4px 8px;cursor:pointer;color:red;">삭제</button>
                            </td>
                        `;
                        tbody.appendChild(tr);
                    }});
                }}
                
                async function toggleAlert(index) {{
                    const club = clubsData[index];
                    club.alert_enabled = !club.alert_enabled;
                    
                    const data = {{
                        old_name: club.name,
                        name: club.name,
                        address: club.address || '',
                        seq: club.seq || '',
                        Golpang_code: club.Golpang_code || '',
                        alert_enabled: club.alert_enabled
                    }};
                    
                    try {{
                        const res = await fetch('/api/admin/clubs', {{
                            method: 'PUT',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify(data)
                        }});
                        if(!res.ok) throw new Error('Network error');
                    }} catch (e) {{
                        alert('알림 설정 저장에 실패했습니다.');
                        loadClubs();
                    }}
                }}
                
                function editClub(index) {{
                    const club = clubsData[index];
                    const tbody = document.getElementById('clubs-tbody');
                    const tr = tbody.children[index];
                    const checked = club.alert_enabled ? 'checked' : '';
                    tr.innerHTML = `
                        <td><input type="text" id="edit-name-${{index}}" value="${{club.name}}" style="width:100px;"></td>
                        <td><input type="text" id="edit-address-${{index}}" value="${{club.address || ''}}" style="width:150px;"></td>
                        <td style="text-align:center;"><input type="text" id="edit-seq-${{index}}" value="${{club.seq || ''}}" style="width:60px;text-align:center;"></td>
                        <td style="text-align:center;"><input type="text" id="edit-gp-${{index}}" value="${{club.Golpang_code || ''}}" style="width:80px;text-align:center;"></td>
                        <td style="text-align:center;">-</td>
                        <td style="text-align:center;"><input type="checkbox" id="edit-alert-${{index}}" ${{checked}}></td>
                        <td style="text-align:center;">
                            <button onclick="saveClub(${{index}}, '${{club.name}}')" style="padding:4px 8px;cursor:pointer;background:#0969da;color:white;border:none;">저장</button>
                            <button onclick="renderClubs()" style="padding:4px 8px;cursor:pointer;">취소</button>
                        </td>
                    `;
                }}
                
                async function saveClub(index, oldName) {{
                    const data = {{
                        old_name: oldName,
                        name: document.getElementById(`edit-name-${{index}}`).value,
                        address: document.getElementById(`edit-address-${{index}}`).value,
                        seq: document.getElementById(`edit-seq-${{index}}`).value,
                        Golpang_code: document.getElementById(`edit-gp-${{index}}`).value,
                        alert_enabled: document.getElementById(`edit-alert-${{index}}`).checked
                    }};
                    const method = oldName ? 'PUT' : 'POST';
                    await fetch('/api/admin/clubs', {{
                        method: method,
                        headers: {{'Content-Type': 'application/json'}},
                        body: JSON.stringify(data)
                    }});
                    loadClubs();
                }}
                
                async function deleteClub(name) {{
                    if(confirm(`정말 '${{name}}' 구장을 삭제하시겠습니까?`)) {{
                        await fetch('/api/admin/clubs', {{
                            method: 'DELETE',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{name: name}})
                        }});
                        loadClubs();
                    }}
                }}
                
                function addClubRow() {{
                    const tbody = document.getElementById('clubs-tbody');
                    const tr = document.createElement('tr');
                    const index = 'new';
                    tr.innerHTML = `
                        <td><input type="text" id="edit-name-${{index}}" placeholder="구장명" style="width:100px;"></td>
                        <td><input type="text" id="edit-address-${{index}}" placeholder="주소" style="width:150px;"></td>
                        <td style="text-align:center;"><input type="text" id="edit-seq-${{index}}" placeholder="SEQ" style="width:60px;text-align:center;"></td>
                        <td style="text-align:center;"><input type="text" id="edit-gp-${{index}}" placeholder="골팡코드" style="width:80px;text-align:center;"></td>
                        <td style="text-align:center;">-</td>
                        <td style="text-align:center;"><input type="checkbox" id="edit-alert-${{index}}"></td>
                        <td style="text-align:center;">
                            <button onclick="saveClub('${{index}}', '')" style="padding:4px 8px;cursor:pointer;background:#2da44e;color:white;border:none;">추가</button>
                            <button onclick="renderClubs()" style="padding:4px 8px;cursor:pointer;">취소</button>
                        </td>
                    `;
                    tbody.insertBefore(tr, tbody.firstChild);
                }}
                
                // Initialize
                window.onload = function() {{
                    loadClubs();
                }};
            </script>
        </body>
        </html>
        """
        return html
    except Exception as e:
        return f"관리자 통계 로드 실패: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

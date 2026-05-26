from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from datetime import datetime, timedelta
import os
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import firestore as google_firestore
import google.auth
from collections import defaultdict
import json

app = Flask(__name__)
CORS(app)

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

@app.after_request
def add_header(response):
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

# IP Access Logger
def log_access(ip):
    try:
        from datetime import timezone
        KST = timezone(timedelta(hours=9))
        now = datetime.now(KST)
        date_str = now.strftime('%Y-%m-%d')
        doc_id = f"{date_str}_{ip.replace('.', '_')}"
        
        doc_ref = db.collection('access_logs').document(doc_id)
        doc_ref.set({
            "date": date_str,
            "ip": ip,
            "hits": google_firestore.Increment(1),
            "last_active": google_firestore.SERVER_TIMESTAMP
        }, merge=True)
    except Exception as e:
        print(f"Failed to log access: {e}")

# Load Club Data for Regions
GOLF_CLUBS = []
try:
    with open(os.path.join("static", "golf_clubs.json"), "r", encoding="utf-8") as f:
        GOLF_CLUBS = json.load(f)
except Exception as e:
    print(f"Error loading golf_clubs.json: {e}")

def get_region(address):
    if "경기" in address: return "경기"
    if "충청" in address or "충북" in address or "충남" in address: return "충청"
    if "강원" in address: return "강원"
    return "기타"

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/clubs", methods=["GET"])
def get_clubs():
    # Group clubs by region
    grouped = defaultdict(list)
    for club in GOLF_CLUBS:
        region = get_region(club.get("address", ""))
        grouped[region].append({
            "name": club["name"],
            "address": club.get("address", "")
        })
    return jsonify(grouped)

@app.route("/api/available_dates", methods=["GET"])
def get_available_dates():
    """Check next 14 days and return dates that have tee times.
    For today, only include if there are tee times AFTER the current time.
    """
    available = []
    now = datetime.now()  # Local time (KST in production)
    today = now.date()
    current_time_str = now.strftime("%H:%M")  # "HH:MM" format for comparison
    
    # Check next 14 days
    for i in range(14):
        check_date = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        if i == 0:
            # For today: only count tee times strictly after current time
            docs = list(
                db.collection('tee_times')
                  .where('date', '==', check_date)
                  .stream()
            )
            # Filter to times after now
            future_docs = [d for d in docs if (d.to_dict().get('time', '') or '') > current_time_str]
            if future_docs:
                available.append(check_date)
        else:
            # For future dates: any tee time is fine
            docs = db.collection('tee_times').where('date', '==', check_date).limit(1).stream()
            if any(docs):
                available.append(check_date)
            
    return jsonify(available)

@app.route("/api/prices", methods=["POST"])
def get_prices():
    try:
        # IP 로깅 호출
        try:
            ip = request.headers.get("X-Forwarded-For", request.remote_addr)
            if ip:
                client_ip = ip.split(",")[0].strip()
                log_access(client_ip)
        except Exception as e:
            print(f"Logging invocation failed: {e}")

        data = request.get_json()
        dates = data.get("dates", []) # List of "YYYY-MM-DD"
        times = data.get("times", []) # List of hour strings "06", "07"
        clubs = data.get("clubs", []) # List of club names
        today_str = data.get("today", "")  # "YYYY-MM-DD" of client's today
        min_time = data.get("min_time", "")  # "HH:MM" - filter for today's date only
        
        if not dates or not clubs:
            return jsonify([])

        results = []
        
        # Optimization: Query by date AND club (using 'in' operator)
        for date in dates:
            # Firestore 'in' limit is 30. Split clubs into chunks of 30.
            CHUNKS_SIZE = 30
            club_chunks = [clubs[i:i + CHUNKS_SIZE] for i in range(0, len(clubs), CHUNKS_SIZE)]
            
            history_map = {} # (club_name, hour) -> min_price
            
            # 1. Fetch History (7 days ago) for this date
            history_date_obj = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=7)
            history_date_str = history_date_obj.strftime("%Y-%m-%d")
            
            for chunk in club_chunks:
                # Query only specified clubs for history
                hist_docs = db.collection('daily_stats') \
                    .where('date', '==', history_date_str) \
                    .where('club_name', 'in', chunk) \
                    .stream()
                
                for h_doc in hist_docs:
                    h_data = h_doc.to_dict()
                    h_club = h_data.get('club_name')
                    h_hour = h_data.get('hour')
                    h_price = h_data.get('min_price')
                    
                    if h_club and h_hour is not None:
                        history_map[(h_club, str(h_hour))] = h_price
                        history_map[(h_club, int(h_hour))] = h_price

            # 2. Fetch Current Data (Filtered by clubs)
            for chunk in club_chunks:
                docs = db.collection('tee_times') \
                    .where('date', '==', date) \
                    .where('club_name', 'in', chunk) \
                    .stream()
                
                for doc in docs:
                    item = doc.to_dict()
                    
                    # Filter by Time (Hour) if specified
                    item_hour = item.get('hour') # int or str
                    item_time = item.get('time', '')  # "HH:MM" string

                    # For today's date: skip tee times at or before current time
                    if today_str and min_time and date == today_str:
                        if item_time <= min_time:
                            continue
                    
                    if times:
                        normalized_times = [str(int(t)) for t in times] # "06" -> "6"
                        if str(int(item_hour)) not in normalized_times:
                            continue

                    # 3. Lookup History from Map
                    hist_price = history_map.get((item['club_name'], item_hour))
                    
                    diff = 0
                    if hist_price:
                        diff = item['price'] - hist_price
                    
                    results.append({
                        "club_name": item['club_name'],
                        "date": item['date'],
                        "time": item['time'], # "06:12"
                        "price": item['price'],
                        "diff": diff,
                        "source": item.get('source', 'Unknown'),
                        "benefit": item.get('benefit', ''),  # 티스캐너 benefit 필드
                        "url": item.get('url', ''),
                        "source_idx": item.get('source_idx', ''),
                        "history_price": hist_price
                    })

        # Sort by Price
        results.sort(key=lambda x: x['price'])
        
        return jsonify(results)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/get_weather", methods=["POST"])
def get_weather():
    """
    Fetch weather data for specified clubs and dates.
    Request: { "clubs": ["태광", ...], "dates": ["01/22", ...] }
    """
    try:
        data = request.get_json()
        clubs = data.get("clubs", [])
        dates = data.get("dates", [])
        
        if not clubs or not dates:
            return jsonify([])
        
        results = []
        
        # Convert date format (01/22 -> 2025-01-22 or similar)
        # Note: The frontend sends "01/22" format from the tee time table
        current_year = datetime.now().year
        date_pairs = []  # List of (original_format, normalized_format)
        for d in dates:
            try:
                # Handle MM/DD format
                parts = d.split("/")
                if len(parts) == 2:
                    month, day = parts
                    normalized = f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"
                    date_pairs.append((d, normalized))
                else:
                    # Already in YYYY-MM-DD format
                    date_pairs.append((d, d))
            except:
                pass
        
        # Prepare document references
        doc_refs = []
        doc_info = [] # Keep track of which doc corresponds to which club/date
        for club in clubs:
            for original_date, normalized_date in date_pairs:
                doc_id = f"{normalized_date.replace('-', '')}_{club}"
                doc_ref = db.collection('weather_forecast').document(doc_id)
                doc_refs.append(doc_ref)
                doc_info.append((club, original_date))
        
        # Batch fetch all documents
        if not doc_refs:
            return jsonify([])
            
        docs = db.get_all(doc_refs)
        
        for i, doc in enumerate(docs):
            if doc.exists:
                weather_data = doc.to_dict()
                club, original_date = doc_info[i]
                results.append({
                    "club_name": club,
                    "date": original_date,
                    "temp_min": weather_data.get("temp_min"),
                    "temp_max": weather_data.get("temp_max"),
                    "precip_prob_max": weather_data.get("precip_prob_max"),
                    "weather_code_daily": weather_data.get("weather_code_daily"),
                    "hourly": weather_data.get("hourly", [])
                })
        
        return jsonify(results)
        
    except Exception as e:
        print(f"Weather Error: {e}")
        return jsonify([])

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

@app.route("/admin/stats")
def admin_stats():
    try:
        docs = db.collection('access_logs').order_by('date', direction=google_firestore.Query.DESCENDING).limit(1000).stream()
        
        # Build a beautiful, minimal, modern HTML table
        rows_html = ""
        for d in docs:
            data = d.to_dict()
            last_active = data.get('last_active')
            last_active_str = "-"
            if last_active:
                if hasattr(last_active, 'astimezone'):
                    from datetime import timezone
                    KST = timezone(timedelta(hours=9))
                    kst_time = last_active.astimezone(KST)
                    last_active_str = kst_time.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    last_active_str = str(last_active)
                    
            rows_html += f"""
            <tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 12px; font-weight: bold; color: #555;">{data.get('date')}</td>
                <td style="padding: 12px; font-family: monospace; color: #007bff;">{data.get('ip')}</td>
                <td style="padding: 12px; font-weight: bold; text-align: center; color: #28a745;">{data.get('hits')} 회</td>
                <td style="padding: 12px; color: #666; font-size: 0.9rem;">{last_active_str}</td>
            </tr>
            """
            
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Golf AI - 접속 통계 관리자</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
            <style>
                body {{
                    font-family: 'Inter', sans-serif;
                    background-color: #f8f9fa;
                    margin: 0;
                    padding: 20px;
                    color: #333;
                }}
                .container {{
                    max-width: 900px;
                    margin: 0 auto;
                    background: white;
                    padding: 25px;
                    border-radius: 16px;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.05);
                }}
                h1 {{
                    font-size: 1.5rem;
                    color: #2c3e50;
                    margin-bottom: 20px;
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 10px;
                }}
                th {{
                    background-color: #f1f3f5;
                    color: #495057;
                    font-weight: 600;
                    text-align: left;
                    padding: 12px;
                    font-size: 0.9rem;
                }}
                tr:hover {{
                    background-color: #f8f9fa;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Golf AI - 일별 접속 통계 (최근 1,000건)</h1>
                <div style="overflow-x: auto;">
                    <table>
                        <thead>
                            <tr>
                                <th>날짜</th>
                                <th>사용자 IP</th>
                                <th style="text-align: center;">조회 횟수</th>
                                <th>마지막 활동 시간 (KST)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows_html if rows_html else '<tr><td colspan="4" style="text-align:center; padding:20px; color:#888;">접속 기록이 없습니다.</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>
        </body>
        </html>
        """
        return html
    except Exception as e:
        return f"관리자 통계 로드 실패: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

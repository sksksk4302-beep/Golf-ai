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
    """Check next 14 days and return dates that have tee times."""
    available = []
    today = datetime.now().date()
    
    # Check next 14 days
    for i in range(14):
        check_date = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        # Limit 1 is enough to know if data exists
        docs = db.collection('tee_times').where('date', '==', check_date).limit(1).stream()
        if any(docs):
            available.append(check_date)
            
    return jsonify(available)

@app.route("/api/prices", methods=["POST"])
def get_prices():
    try:
        data = request.get_json()
        dates = data.get("dates", []) # List of "YYYY-MM-DD"
        times = data.get("times", []) # List of hour strings "06", "07"
        clubs = data.get("clubs", []) # List of club names
        
        if not dates or not clubs:
            return jsonify([])

        results = []
        
        # Optimization: Query by date, then filter by club and time
        for date in dates:
            # 1. Pre-fetch History (7 days ago) for this date
            # Instead of N+1 reads, we do 1 read (query) per date.
            history_date_obj = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=7)
            history_date_str = history_date_obj.strftime("%Y-%m-%d")
            
            history_map = {} # (club_name, hour) -> min_price
            
            # Fetch all daily_stats for the history date
            # This might return ~100-200 docs, which is 1 read op per doc returned + 1 query op.
            # If we have 50 items to show, N+1 approach is 50 reads.
            # If we have 200 stats but only show 5 items, this might be more expensive?
            # However, usually users see many items. And "Entity Reads" are cheap enough that 
            # reducing latency of N round-trips is also worth it.
            # Also, we can filter history query by clubs if list is small, but 'in' query limit is 10.
            # Given the use case (showing many tee times), fetching all stats for the day is safer/simpler.
            
            hist_docs = db.collection('daily_stats').where('date', '==', history_date_str).stream()
            for h_doc in hist_docs:
                h_data = h_doc.to_dict()
                # Key: (Club, Hour)
                # Ensure types match. h_data['hour'] is likely int from archive_history.
                h_club = h_data.get('club_name')
                h_hour = h_data.get('hour')
                h_price = h_data.get('min_price')
                
                if h_club and h_hour is not None:
                    history_map[(h_club, str(h_hour))] = h_price
                    # Also store as int just in case
                    history_map[(h_club, int(h_hour))] = h_price

            # 2. Fetch Current Data
            docs = db.collection('tee_times').where('date', '==', date).stream()
            
            for doc in docs:
                item = doc.to_dict()
                
                # Filter by Club
                if item['club_name'] not in clubs:
                    continue
                
                # Filter by Time (Hour)
                item_hour = item.get('hour') # int or str
                
                if times:
                    normalized_times = [str(int(t)) for t in times] # "06" -> "6"
                    if str(int(item_hour)) not in normalized_times:
                        continue

                # 3. Lookup History from Map
                # item['hour'] comes from ingest_data, which is int.
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
                    "history_price": hist_price
                })

        # Sort by Price
        results.sort(key=lambda x: x['price'])
        
        return jsonify(results)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

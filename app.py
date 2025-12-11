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

        # 1. Fetch Current Data
        # Firestore 'in' query is limited to 10 items. If we have many dates/clubs, we might need multiple queries.
        # For simplicity, let's query by date (most restrictive usually) and filter in memory.
        
        results = []
        
        # Optimization: Query by date, then filter by club and time
        for date in dates:
            docs = db.collection('tee_times').where('date', '==', date).stream()
            
            for doc in docs:
                item = doc.to_dict()
                
                # Filter by Club
                if item['club_name'] not in clubs:
                    continue
                
                # Filter by Time (Hour)
                # item['hour'] is int or str? Ingest saves as item['hour_num'] which comes from crawler.
                # Let's assume it matches the format in 'times' list or convert.
                # Ingest saves 'hour': item['hour_num'] (e.g., 6, 7, 13)
                # Input 'times' might be ["06", "07"] or [6, 7]
                
                item_hour = str(item.get('hour'))
                # Normalize input times to string without leading zero for comparison if needed, or handle both
                # Let's assume input times are strings like "6", "14" or "06".
                
                # Simple check: if times filter is provided, check if item_hour is in it.
                # We need to be careful with "06" vs "6".
                if times:
                    normalized_times = [str(int(t)) for t in times] # "06" -> "6"
                    if str(int(item_hour)) not in normalized_times:
                        continue

                # 2. Fetch History (7 days ago) for comparison
                # To avoid N+1 queries, we could batch fetch history or just fetch on demand.
                # For MVP, let's fetch on demand or maybe just return current price and let frontend fetch history?
                # Or better: Calculate history date and fetch.
                
                # History Key: YYYYMMDD_Club_Hour_SnapshotTime
                # We need aggregated history for that hour.
                # History collection: 'price_history'
                # Doc ID: YYYYMMDD_Club_Hour_SnapshotTime
                # We want the latest snapshot for that day? Or average?
                # Let's try to find a matching history record.
                
                history_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
                
                # We can't easily get the exact history doc ID without snapshot time.
                # Query history by club, date, hour
                hist_docs = db.collection('price_history')\
                              .where('club_name', '==', item['club_name'])\
                              .where('date', '==', history_date)\
                              .where('hour', '==', item['hour'])\
                              .limit(1).stream() # Get one snapshot (any is fine, or latest)
                
                hist_price = None
                for h_doc in hist_docs:
                    h_data = h_doc.to_dict()
                    hist_price = h_data.get('stats', {}).get('min') # Compare with min price of last week
                    break
                
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

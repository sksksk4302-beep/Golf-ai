import os
import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from weather_utils import get_club_list, fetch_weather_forecast, parse_weather_data

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

def init_firestore():
    from google.cloud import firestore
    from google.oauth2 import service_account
    import google.auth

    if os.path.exists(CRED_PATH):
        print(f"Using service account file: {CRED_PATH}")
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        print("Service account file not found. Using ADC...")
        credentials, project = google.auth.default()
        return firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

def ingest_weather():
    db = init_firestore()
    clubs = get_club_list()
    print(f"Starting weather ingestion for {len(clubs)} clubs...")
    
    batch = db.batch()
    batch_count = 0
    total_updates = 0
    
    for club in clubs:
        print(f"Fetching weather for {club}...")
        raw_data = fetch_weather_forecast(club)
        if not raw_data:
            continue
            
        parsed_days = parse_weather_data(raw_data)
        
        for day in parsed_days:
            # Doc ID: YYYYMMDD_Club
            date_str = day["date"]
            doc_id = f"{date_str.replace('-', '')}_{club}"
            doc_ref = db.collection('weather_forecast').document(doc_id)
            
            data = {
                "club_name": club,
                "date": date_str,
                "temp_max": day["temp_max"],
                "temp_min": day["temp_min"],
                "precip_prob_max": day["precip_prob_max"],
                "weather_code_daily": day["weather_code_daily"],
                "hourly": day["hourly"], # List of 24 dicts
                "updated_at": firestore.SERVER_TIMESTAMP
            }
            
            batch.set(doc_ref, data)
            batch_count += 1
            total_updates += 1
            
            if batch_count >= 400:
                batch.commit()
                batch = db.batch()
                batch_count = 0
                print("Committed batch...")
                
    if batch_count > 0:
        batch.commit()
        
    print(f"Weather ingestion completed. Total records updated: {total_updates}")

if __name__ == "__main__":
    ingest_weather()

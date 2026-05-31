import os
import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from weather_utils import (
    get_club_list, 
    get_club_region, 
    get_unique_regions, 
    get_region_coords, 
    fetch_weather_batch, 
    parse_weather_data
)

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
    regions = get_unique_regions()
    region_coords = get_region_coords()
    
    # 이번 업데이트의 고유 ID
    sync_id = datetime.datetime.now().strftime("%Y%m%d%H%M")
    
    print(f"Starting weather ingestion for {len(clubs)} clubs across {len(regions)} regions with sync_id={sync_id}...")
    
    # 1. Fetch weather in batch for all unique regions
    latitudes = [region_coords[r][0] for r in regions]
    longitudes = [region_coords[r][1] for r in regions]
    
    print(f"Fetching weather batch for {len(regions)} regions...")
    batch_raw = fetch_weather_batch(latitudes, longitudes)
    
    if not batch_raw or len(batch_raw) != len(regions):
        print(f"Error: Weather batch fetch returned mismatch or empty results (expected {len(regions)}, got {len(batch_raw) if batch_raw else 0}).")
        return
        
    # Map region -> parsed daily forecasts
    region_forecasts = {}
    for i, region in enumerate(regions):
        raw_data = batch_raw[i]
        parsed = parse_weather_data(raw_data)
        region_forecasts[region] = parsed
        
    print(f"Successfully fetched and parsed weather for all {len(regions)} regions. Upserting to Firestore...")
    
    batch = db.batch()
    batch_count = 0
    total_upserts = 0
    total_ops = 0
    
    # 2. Upsert weather data for each club based on its region
    for club in clubs:
        region = get_club_region(club)
        if not region or region not in region_forecasts:
            print(f"Warning: No weather data for club {club} (region: {region})")
            continue
            
        parsed_days = region_forecasts[region]
        
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
                "precipitation_sum": day.get("precipitation_sum"),
                "precip_prob_max": day["precip_prob_max"],
                "weather_code_daily": day["weather_code_daily"],
                "hourly": day["hourly"], # List of 24 dicts
                "sync_id": sync_id,
                "updated_at": firestore.SERVER_TIMESTAMP
            }
            
            batch.set(doc_ref, data, merge=True)
            batch_count += 1
            total_upserts += 1
            total_ops += 1
            
            if batch_count >= 400:
                batch.commit()
                batch = db.batch()
                batch_count = 0
                print(f"Committed upsert batch ({total_upserts} total)...")
                
    if batch_count > 0:
        batch.commit()
        
    print(f"Upsert complete. Cleaning up stale weather data...")

    # 3. Delete stale weather data
    stale_docs = db.collection('weather_forecast') \
        .where('sync_id', '<', sync_id) \
        .stream()
        
    delete_batch = db.batch()
    delete_count = 0
    
    for doc in stale_docs:
        delete_batch.delete(doc.reference)
        delete_count += 1
        total_ops += 1
        
        if delete_count >= 400:
            delete_batch.commit()
            delete_batch = db.batch()
            delete_count = 0
            print(f"Committed delete batch ({total_ops} total ops)...")
            
    if delete_count > 0:
        delete_batch.commit()
        
    print(f"Weather ingestion completed. Total ops: {total_ops} (Upserts: {total_upserts}, Deletes: {total_ops - total_upserts})")

if __name__ == "__main__":
    ingest_weather()

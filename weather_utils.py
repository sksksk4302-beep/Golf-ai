import json
import os
import requests
from typing import Dict, Optional, List, Any

# Load club coordinates and regions
CLUB_COORDS = {}
CLUB_REGION = {}
REGION_CLUBS = {}

try:
    # Try multiple paths for robustness
    paths = [
        "static/golf_clubs.json",
        "c:/Users/09153/Documents/NawabariGolf-main/static/golf_clubs.json",
        os.path.join(os.path.dirname(__file__), "static", "golf_clubs.json")
    ]
    
    json_path = None
    for p in paths:
        if os.path.exists(p):
            json_path = p
            break
            
    if json_path:
        with open(json_path, "r", encoding="utf-8") as f:
            clubs = json.load(f)
            
            def extract_region(address: str) -> str:
                parts = address.split()
                if len(parts) >= 2:
                    prov = parts[0]
                    city = parts[1]
                    if '경기' in prov: prov = '경기도'
                    elif '충북' in prov or '충청북' in prov: prov = '충청북도'
                    elif '충남' in prov or '충청남' in prov: prov = '충청남도'
                    elif '강원' in prov: prov = '강원도'
                    if city == '여주군': city = '여주시'
                    if '진천' in city: prov = '충청북도'
                    return f"{prov} {city}"
                return address

            for club in clubs:
                if "name" in club and "lat" in club and "lng" in club:
                    name = club["name"]
                    lat = club["lat"]
                    lng = club["lng"]
                    CLUB_COORDS[name] = (lat, lng)
                    
                    addr = club.get("address", "")
                    region = extract_region(addr)
                    CLUB_REGION[name] = region
                    
                    if region not in REGION_CLUBS:
                        REGION_CLUBS[region] = []
                    REGION_CLUBS[region].append((lat, lng))
    else:
        print("Warning: golf_clubs.json not found for weather utils")
except Exception as e:
    print(f"Error loading golf_clubs.json: {e}")

# Compute representative coordinates for each region (centroid)
REGION_COORDS = {}
for region, coords in REGION_CLUBS.items():
    if coords:
        avg_lat = sum(c[0] for c in coords) / len(coords)
        avg_lng = sum(c[1] for c in coords) / len(coords)
        REGION_COORDS[region] = (avg_lat, avg_lng)

def get_club_list() -> List[str]:
    return list(CLUB_COORDS.keys())

def get_club_region(club_name: str) -> Optional[str]:
    return CLUB_REGION.get(club_name)

def get_unique_regions() -> List[str]:
    return list(REGION_COORDS.keys())

def get_region_coords() -> Dict[str, tuple]:
    return REGION_COORDS

def fetch_weather_batch(latitudes: List[float], longitudes: List[float], days: int = 14) -> List[Dict[str, Any]]:
    """
    Fetches 14-day hourly and daily forecast for multiple coordinates in one batch request.
    Returns a list of raw weather responses from Open-Meteo.
    """
    if not latitudes or not longitudes:
        return []
        
    try:
        # Open-Meteo API (Batch mode)
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": ",".join(str(lat) for lat in latitudes),
            "longitude": ",".join(str(lng) for lng in longitudes),
            "hourly": "temperature_2m,precipitation_probability,weathercode",
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max,weathercode",
            "timezone": "Asia/Tokyo",
            "forecast_days": days
        }
        
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
    except Exception as e:
        print(f"Weather batch fetch failed: {e}")
        
    return []

def parse_weather_data(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parses raw Open-Meteo response into a list of daily records,
    each containing hourly details.
    """
    parsed_days = []
    
    if not raw_data or "daily" not in raw_data or "hourly" not in raw_data:
        return []
        
    daily = raw_data["daily"]
    hourly = raw_data["hourly"]
    
    # Iterate through days
    for i, date in enumerate(daily["time"]):
        day_record = {
            "date": date,
            "temp_max": daily["temperature_2m_max"][i],
            "temp_min": daily["temperature_2m_min"][i],
            "precipitation_sum": daily["precipitation_sum"][i],
            "precip_prob_max": daily["precipitation_probability_max"][i],
            "weather_code_daily": daily["weathercode"][i],
            "hourly": []
        }
        
        # Extract hourly data for this date (24 hours)
        start_idx = i * 24
        end_idx = start_idx + 24
        
        # Safety check for index bounds
        if end_idx <= len(hourly["time"]):
            for h in range(24):
                idx = start_idx + h
                hour_time = hourly["time"][idx] # e.g., "2025-01-21T00:00"
                hour_only = int(hour_time.split("T")[1].split(":")[0])
                
                day_record["hourly"].append({
                    "hour": hour_only,
                    "temp": hourly["temperature_2m"][idx],
                    "precip_prob": hourly["precipitation_probability"][idx],
                    "code": hourly["weathercode"][idx]
                })
        
        parsed_days.append(day_record)
        
    return parsed_days


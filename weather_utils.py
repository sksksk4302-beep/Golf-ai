import json
import os
import requests
from typing import Dict, Optional, List, Any

# Load club coordinates
CLUB_COORDS = {}
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
            for club in clubs:
                if "name" in club and "lat" in club and "lng" in club:
                    CLUB_COORDS[club["name"]] = (club["lat"], club["lng"])
    else:
        print("Warning: golf_clubs.json not found for weather utils")
except Exception as e:
    print(f"Error loading golf_clubs.json: {e}")

def get_club_list() -> List[str]:
    return list(CLUB_COORDS.keys())

def fetch_weather_forecast(club_name: str, days: int = 14) -> Optional[Dict[str, Any]]:
    """
    Fetches 14-day hourly and daily forecast for a club.
    Returns a dictionary with 'daily' and 'hourly' data.
    """
    if club_name not in CLUB_COORDS:
        return None
        
    lat, lng = CLUB_COORDS[club_name]
    
    try:
        # Open-Meteo API
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lng,
            "hourly": "temperature_2m,precipitation_probability,weathercode",
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max,weathercode",
            "timezone": "Asia/Tokyo",
            "forecast_days": days
        }
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Weather fetch failed for {club_name}: {e}")
        
    return None

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

# Cache to store weather data: {club_name: [parsed_daily_records]}
WEATHER_CACHE = {}

def get_weather_for_club(club_name: str, target_date: str) -> Optional[Dict[str, Any]]:
    """
    Returns the weather forecast for a specific club and date.
    Uses caching to avoid repeated API calls.
    """
    if club_name not in CLUB_COORDS:
        return None

    # Check cache first
    if club_name not in WEATHER_CACHE:
        print(f"Fetching weather for {club_name}...")
        raw_data = fetch_weather_forecast(club_name)
        if raw_data:
            WEATHER_CACHE[club_name] = parse_weather_data(raw_data)
        else:
            WEATHER_CACHE[club_name] = [] # Mark as failed/empty to avoid retrying

    # Look for the specific date
    for day_record in WEATHER_CACHE[club_name]:
        if day_record["date"] == target_date:
            return day_record
            
    return None

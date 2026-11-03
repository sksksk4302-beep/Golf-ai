import json
import os
import requests
from typing import Dict, Optional, List, Any

# Load club coordinates and regions
CLUB_COORDS = {}
CLUB_REGION = {}
REGION_CLUBS = {}

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
    Parses raw Open-Meteo response into a list of daily records.
    """
    parsed_days = []
    
    if not raw_data or "daily" not in raw_data:
        return []
        
    daily = raw_data["daily"]
    
    # Iterate through days
    for i, date in enumerate(daily["time"]):
        day_record = {
            "date": date,
            "temp_max": daily["temperature_2m_max"][i],
            "temp_min": daily["temperature_2m_min"][i],
            "precipitation_sum": daily["precipitation_sum"][i],
            "precip_prob_max": daily["precipitation_probability_max"][i],
            "weather_code_daily": daily["weathercode"][i]
        }
        
        parsed_days.append(day_record)
        
    return parsed_days

def fetch_past_weather_for_clubs(target_date_str: str) -> Dict[str, Dict[str, Any]]:
    """
    어제 날짜(target_date_str)에 대한 지역별 실제 날씨(최저, 최고, 강수량)를 조회하여
    club_name -> {temp_min, temp_max, precip_mm} 매핑으로 반환.
    """
    club_weather_map = {}
    try:
        coords = get_region_coords()
        regions = list(coords.keys())
        if not regions:
            return club_weather_map
        lats = [coords[r][0] for r in regions]
        lngs = [coords[r][1] for r in regions]

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": ",".join(str(lat) for lat in lats),
            "longitude": ",".join(str(lng) for lng in lngs),
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "Asia/Tokyo",
            "start_date": target_date_str,
            "end_date": target_date_str
        }
        res = requests.get(url, params=params, timeout=10)
        region_weather = {}
        if res.status_code == 200:
            data = res.json()
            if isinstance(data, dict):
                data = [data]
            for i, item in enumerate(data):
                if i >= len(regions):
                    break
                reg = regions[i]
                d = item.get("daily", {})
                t_max = (d.get("temperature_2m_max") or [None])[0]
                t_min = (d.get("temperature_2m_min") or [None])[0]
                precip = (d.get("precipitation_sum") or [None])[0]
                region_weather[reg] = {
                    "temp_min": t_min,
                    "temp_max": t_max,
                    "precip_mm": precip
                }

        # 1. 정적 구장 매핑 우선 반영
        for c_name, reg in CLUB_REGION.items():
            if reg in region_weather:
                club_weather_map[c_name] = region_weather[reg]

        # 2. Firestore에 신규 추가된 구장도 반영
        try:
            from club_utils import get_golf_clubs
            from archive_store import init_firestore
            db = init_firestore()
            clubs = get_golf_clubs(db)
            for club in clubs:
                c_name = club.get("name")
                if c_name and c_name not in club_weather_map:
                    reg = get_club_region(c_name) or extract_region(club.get("address", ""))
                    if reg and reg in region_weather:
                        club_weather_map[c_name] = region_weather[reg]
        except Exception:
            pass
    except Exception as e:
        print(f"[weather_utils] fetch_past_weather_for_clubs 실패: {e}")

    return club_weather_map

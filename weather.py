import requests
from datetime import datetime, timedelta
import json
import os

SERVICE_KEY = os.getenv("0pufYd46gOsX61f/gjCIhoD1jrtJcgclBVmFnsryJ5AxXV9g1+Td+26feW3O46x9tl0iIY7DJS12GFuHlraF4w==")  # 기상청 API 인증키 (환경변수에서 읽기)
VILAGE_FCST_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"

def fetch_weather(golf_name, lat, lng, base_date=None):
    # 좌표 변환 (위경도 → 격자)
    nx, ny = convert_grid(lat, lng)
    
    # 날짜 및 시간 설정
    now = datetime.now()
    base_dt = now if base_date is None else datetime.strptime(base_date, "%Y-%m-%d")
    base_date = base_dt.strftime("%Y%m%d")
    base_time = get_base_time(now)

    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": "1",
        "numOfRows": "1000",
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }

    try:
        r = requests.get(VILAGE_FCST_URL, params=params, timeout=6)
        r.raise_for_status()
        items = r.json()["response"]["body"]["items"]["item"]

        # 시간별 예보 정리
        forecast = {}
        for it in items:
            fcst_time = it["fcstTime"]
            category = it["category"]
            value = it["fcstValue"]
            if fcst_time not in forecast:
                forecast[fcst_time] = {}
            forecast[fcst_time][category] = value

        # 시간별 날씨 요약: 강수형태(PTY), 기온(TMP), 강수량(PCP or POP)
        result = {}
        for time_str, values in forecast.items():
            hour = int(time_str[:2])
            desc = "맑음"
            if values.get("PTY") in {"1", "4"}: desc = "비"
            elif values.get("PTY") == "3": desc = "눈"
            elif values.get("SKY") == "4": desc = "흐림"
            elif values.get("SKY") == "3": desc = "구름"

            result[hour] = {
                "desc": desc,
                "temp": float(values.get("TMP", 0)),
                "rain": float(values.get("PCP", "0").replace("강수없음", "0").replace("mm", "").replace("-", "0"))
            }

        return result  # 시간대별 dict 반환

    except Exception as e:
        print(f"❌ {golf_name} 날씨 수집 실패:", e)
        return {}

def convert_grid(lat, lon):
    # 격자 변환 알고리즘 (기상청 기준)
    import math
    RE = 6371.00877  # 지구 반경
    GRID = 5.0       # 격자 간격
    SLAT1 = 30.0
    SLAT2 = 60.0
    OLON = 126.0
    OLAT = 38.0
    XO = 43
    YO = 136

    DEGRAD = math.pi / 180.0
    re = RE / GRID
    slat1 = SLAT1 * DEGRAD
    slat2 = SLAT2 * DEGRAD
    olon = OLON * DEGRAD
    olat = OLAT * DEGRAD

    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = math.pow(sf, sn) * math.cos(slat1) / sn
    ro = math.tan(math.pi * 0.25 + olat * 0.5)
    ro = re * sf / math.pow(ro, sn)

    ra = math.tan(math.pi * 0.25 + lat * DEGRAD * 0.5)
    ra = re * sf / math.pow(ra, sn)
    theta = lon * DEGRAD - olon
    if theta > math.pi: theta -= 2.0 * math.pi
    if theta < -math.pi: theta += 2.0 * math.pi
    theta *= sn

    x = int(ra * math.sin(theta) + XO + 0.5)
    y = int(ro - ra * math.cos(theta) + YO + 0.5)
    return x, y

def get_base_time(now):
    # 가장 최근 발표 시간 기준
    hour = now.hour
    base_times = [2, 5, 8, 11, 14, 17, 20, 23]
    for bt in reversed(base_times):
        if hour >= bt:
            return f"{bt:02d}00"
    return "2300"

if __name__ == "__main__":
    from golf_clubs import get_club_latlng  # 예시: golf_clubs.json 에서 불러온다고 가정
    print(fetch_weather("세현", 37.199742, 127.340926))

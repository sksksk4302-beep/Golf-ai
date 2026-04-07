import requests
from datetime import datetime, timedelta

def test_teescan(seq, name, date_str):
    url = (
        "https://foapi.teescanner.com/v1/booking/getTeeTimeListbyGolfclub"
        f"?golfclub_seq={seq}&roundDay={date_str}&orderType="
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        tee_times = data.get("data", {}).get("teeTimeList", [])
        result_code = data.get("resultCode", "?")
        result_msg = data.get("resultMsg", "?")
        print(f"  {name} (seq={seq}) date={date_str}: {len(tee_times)} tee times | code={result_code} msg={result_msg}")
        if not tee_times and data:
            print(f"    Full response: {str(data)[:300]}")
        return len(tee_times)
    except Exception as e:
        print(f"  {name} (seq={seq}) date={date_str}: ERROR - {e}")
        return -1

if __name__ == "__main__":
    today = datetime.now()
    
    # Test dates: today+1 (tomorrow) vs next week (D+7~10)
    test_dates = []
    for d in [1, 2, 7, 8, 9, 10]:
        test_dates.append((today + timedelta(days=d)).strftime("%Y-%m-%d"))
    
    # Test a few clubs with seq
    test_clubs = [
        ("51", "태광"),
        ("114055", "세현"),
        ("48", "블루원용인"),
        ("175", "스카이밸리"),
        ("154", "아리지"),
    ]
    
    for date_str in test_dates:
        print(f"\n=== Date: {date_str} ===")
        total = 0
        for seq, name in test_clubs:
            count = test_teescan(seq, name, date_str)
            if count > 0:
                total += count
        print(f"  TOTAL: {total} tee times for this date")

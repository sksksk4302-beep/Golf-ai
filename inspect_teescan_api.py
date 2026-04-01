import requests
import json
from datetime import datetime, timedelta

def get_teescan_times(seq, date_str):
    url = (
        "https://foapi.teescanner.com/v1/booking/getTeeTimeListbyGolfclub"
        f"?golfclub_seq={seq}&roundDay={date_str}&orderType="
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    print(f"Requesting: {url}")
    try:
        r = requests.get(url, headers=headers, timeout=10)
        print(f"Status Code: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            tee_times = data.get("data", {}).get("teeTimeList", [])
            print(f"Found {len(tee_times)} tee times.")
            if tee_times:
                first_item = tee_times[0]
                print("First item keys:", first_item.keys())
                print("First item 'benefit' value:", first_item.get("benefit"))
                # print("First item full dump:", json.dumps(first_item, indent=2, ensure_ascii=False))
                
                # Check if any item has benefit
                benefits = [t.get("benefit") for t in tee_times if t.get("benefit")]
                print(f"Items with non-empty benefit: {len(benefits)}")
                if benefits:
                    print(f"Example benefit: {benefits[0]}")
            else:
                print("No tee times found for this date.")
        else:
            print(f"Error: {r.text}")
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Target date: {tomorrow}")
    # Club: Taekwang (seq 51)
    get_teescan_times("51", tomorrow)

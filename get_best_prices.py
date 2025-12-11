import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import os

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

def init_firestore():
    if not os.path.exists(CRED_PATH):
        print(f"[Error] Service account key not found at '{CRED_PATH}'.")
        return None
    
    from google.cloud import firestore
    from google.oauth2 import service_account

    cred = service_account.Credentials.from_service_account_file(CRED_PATH)
    return firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")

def get_best_prices(club_name, target_date):
    db = init_firestore()
    if not db:
        return

    print(f"Fetching best prices for {club_name} on {target_date}...")

    # 1. Fetch Current Data
    docs = db.collection('tee_times')\
             .where('club_name', '==', club_name)\
             .where('date', '==', target_date)\
             .stream()

    current_prices = {} # hour -> min_price
    
    for doc in docs:
        d = doc.to_dict()
        hour = d.get('hour')
        price = d.get('price')
        
        if hour is not None and price:
            if hour not in current_prices or price < current_prices[hour]:
                current_prices[hour] = price

    if not current_prices:
        print("No current tee times found.")
        return

    # 2. Fetch History Data (7 days ago)
    target_dt = datetime.datetime.strptime(target_date, "%Y-%m-%d")
    history_date = (target_dt - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    
    print(f"Comparing with history from {history_date}...")
    
    # We want the LATEST snapshot for that history date
    # Since we can't easily query "latest snapshot" for all hours in one go without complex indexing,
    # we'll fetch all snapshots for that date/club and process in memory.
    hist_docs = db.collection('price_history')\
                  .where('club_name', '==', club_name)\
                  .where('date', '==', history_date)\
                  .stream()
                  
    history_prices = {} # hour -> min_price
    
    # Process history to find the min price for each hour across all snapshots of that day
    # (Or should we compare against the SAME time snapshot? User said "last week same weekday")
    # Let's use the absolute minimum recorded for that day as the baseline.
    for doc in hist_docs:
        d = doc.to_dict()
        hour = d.get('hour')
        stats = d.get('stats', {})
        min_p = stats.get('min')
        
        if hour is not None and min_p:
            if hour not in history_prices or min_p < history_prices[hour]:
                history_prices[hour] = min_p

    # 3. Display Comparison
    print(f"\n{'Hour':<5} | {'Current':<10} | {'Last Week':<10} | {'Diff':<10}")
    print("-" * 45)
    
    sorted_hours = sorted(current_prices.keys())
    for hour in sorted_hours:
        curr = current_prices[hour]
        hist = history_prices.get(hour, None)
        
        diff_str = "-"
        hist_str = "N/A"
        
        if hist is not None:
            hist_str = f"{hist:,}"
            diff = curr - hist
            if diff > 0:
                diff_str = f"+{diff:,} (▲)"
            elif diff < 0:
                diff_str = f"{diff:,} (▼)"
            else:
                diff_str = "0 (-)"
                
        print(f"{hour:02d}   | {curr:<10,} | {hist_str:<10} | {diff_str:<10}")

if __name__ == "__main__":
    # Example usage: Test with a club and date that likely has data
    # You might need to adjust this based on what's in your DB
    import sys
    if len(sys.argv) > 2:
        get_best_prices(sys.argv[1], sys.argv[2])
    else:
        # Default test
        today = datetime.date.today().strftime("%Y-%m-%d")
        # Find a club name from ingest_data or just hardcode one we saw earlier
        get_best_prices("기흥", today) 

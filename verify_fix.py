
import datetime
import logging
import sys
from crawler_utils import crawl_golfpang, crawl_golfpang_specific_club

# Setup logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def test_infinite_loop_fix():
    print("\n>>> Testing Infinite Loop Fix (Sector Mode)...")
    # Use a date far in the future where data is sparse or empty
    target_date = (datetime.date.today() + datetime.timedelta(days=20)).strftime("%Y-%m-%d")
    print(f"Target Date: {target_date}")
    
    # Use sector 5 (Gyeonggi South)
    # It should run for a few pages and then stop if no data matches
    # Note: crawl_golfpang filters by GOLF_CLUBS list.
    # If the site returns data but it doesn't match our clubs, it counts as "empty page" for us.
    
    try:
        data = crawl_golfpang(target_date, [], sectors=[5])
        print(f"Found {len(data)} items.")
    except Exception as e:
        print(f"Error: {e}")

def test_specific_club_crawl():
    print("\n>>> Testing Specific Club Crawl (Club ID=6, 골프존 안성H)...")
    target_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Target Date: {target_date}")
    
    try:
        # Sector 5 contains Club 6
        data = crawl_golfpang_specific_club(target_date, "6", 5)
        print(f"Found {len(data)} items.")
        for item in data[:5]:
            print(f"  - {item['golf']} {item['time']} {item['price']}")
            
        # Verify all items are indeed Club 6 (or name matches)
        # Note: crawl_golfpang_specific_club sets the name from GOLF_CLUBS
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_infinite_loop_fix()
    test_specific_club_crawl()

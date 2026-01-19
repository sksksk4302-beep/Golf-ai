
import datetime
from crawler_utils import crawl_golfpang_specific_club, GOLF_CLUBS

def test_specific_crawl():
    # Pick a club with an ID
    target_club = next((c for c in GOLF_CLUBS if c.get("golfpang_id")), None)
    if not target_club:
        print("No club with golfpang_id found.")
        return

    print(f"Testing specific crawl for {target_club['name']} (ID: {target_club['golfpang_id']})")
    
    today = datetime.date.today()
    date_str = (today + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    
    # We need to know the sector. The current _sector_from_address returns None.
    # But crawl_golfpang_specific_club takes a sector argument.
    # Let's try sector 5 (Gyeonggi) as a guess or try to find the correct one.
    # Actually, the sector might not matter for specific club search if the ID is unique?
    # But the form data includes 'sector'.
    
    # Let's try with sector 1, 4, 5, 8, 16 until we find data or just try 5.
    # Most clubs in the list seem to be in Gyeonggi (sector 5?).
    
    sector = 5 # Default guess
    
    start_time = datetime.datetime.now()
    results = crawl_golfpang_specific_club(date_str, target_club['golfpang_id'], sector)
    end_time = datetime.datetime.now()
    
    print(f"Found {len(results)} items in {(end_time - start_time).total_seconds()} seconds.")
    if results:
        print(results[0])

if __name__ == "__main__":
    test_specific_crawl()

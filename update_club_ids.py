
import json
import os
import requests
import urllib3
from bs4 import BeautifulSoup
from crawler_utils import _make_session, AJAX_HEADERS, NODE_URL, LIST_URL, COMMON_HEADERS, _norm_name

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Sectors to check (from crawler_utils.py default)
SECTORS = [1, 4, 5, 8, 16]

def fetch_all_club_ids():
    print("Fetching all club IDs from Golfpang...")
    
    headers = AJAX_HEADERS.copy()
    headers["x-customer-check"] = "gp-post-key-2019"
    
    club_map = {} # name -> id
    
    with _make_session() as s:
        s.get(LIST_URL, headers=COMMON_HEADERS, verify=False)
        
        for sector in SECTORS:
            print(f"Fetching sector {sector}...")
            payload = {"Depth": "2", "GID": str(sector), "SUB_GID": ""}
            try:
                r = s.post(NODE_URL, headers=headers, data=payload, verify=False)
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, "html.parser")
                    options = soup.find_all("option")
                    print(f"  Found {len(options)} clubs.")
                    for opt in options:
                        val = opt.get("value")
                        txt = opt.get_text(strip=True)
                        if val:
                            # Normalize name for matching
                            norm_txt = _norm_name(txt)
                            club_map[norm_txt] = val
                            # Also store original name just in case
                            club_map[txt] = val
                else:
                    print(f"  Failed sector {sector}: {r.status_code}")
            except Exception as e:
                print(f"  Error sector {sector}: {e}")
                
    return club_map

def update_json(club_id_map):
    json_path = os.path.join("static", "golf_clubs.json")
    with open(json_path, "r", encoding="utf-8") as f:
        clubs = json.load(f)
        
    updated_count = 0
    for club in clubs:
        # Try to match by Golpang_code first, then name
        gp_code = club.get("Golpang_code", "")
        name = club.get("name", "")
        
        matched_id = None
        
        # 1. Try exact match with Golpang_code
        if gp_code and gp_code in club_id_map:
            matched_id = club_id_map[gp_code]
        # 2. Try normalized match with Golpang_code
        elif gp_code and _norm_name(gp_code) in club_id_map:
            matched_id = club_id_map[_norm_name(gp_code)]
        # 3. Try exact/normalized match with name
        elif name and name in club_id_map:
            matched_id = club_id_map[name]
        elif name and _norm_name(name) in club_id_map:
            matched_id = club_id_map[_norm_name(name)]
            
        if matched_id:
            club["golfpang_id"] = matched_id
            updated_count += 1
        else:
            print(f"Could not find ID for: {name} (Code: {gp_code})")
            
    print(f"Updated {updated_count} / {len(clubs)} clubs.")
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(clubs, f, indent=2, ensure_ascii=False)
        
if __name__ == "__main__":
    ids = fetch_all_club_ids()
    update_json(ids)


import requests
import urllib3
from bs4 import BeautifulSoup
from crawler_utils import _make_session, AJAX_HEADERS, NODE_URL, LIST_URL, COMMON_HEADERS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def verify_node_fix():
    print("Verifying node.do fix...")
    
    # Update headers with the key provided by user
    headers = AJAX_HEADERS.copy()
    headers["x-customer-check"] = "gp-post-key-2019"
    
    with _make_session() as s:
        # Bootstrap (List URL)
        s.get(LIST_URL, headers=COMMON_HEADERS, verify=False)
        
        # Test Sector 5 (GID=5)
        # Payload from user: Depth=2, GID=5, SUB_GID=
        payload = {
            "Depth": "2",
            "GID": "5",
            "SUB_GID": ""
        }
        
        print(f"Sending payload: {payload}")
        try:
            r = s.post(NODE_URL, headers=headers, data=payload, verify=False)
            print(f"Status: {r.status_code}")
            
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                options = soup.find_all("option")
                print(f"Found {len(options)} options.")
                
                # Print some examples to verify we got club IDs
                count = 0
                for opt in options:
                    val = opt.get("value")
                    txt = opt.get_text(strip=True)
                    if val: # Skip empty value
                        print(f"Club: {txt} -> ID: {val}")
                        count += 1
                        if count >= 5: break
            else:
                print(f"Error text: {r.text[:200]}")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    verify_node_fix()

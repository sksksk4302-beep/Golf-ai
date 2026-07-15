import os
import requests
import datetime
from google.cloud import firestore as google_firestore

PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

def init_firestore():
    from google.cloud import firestore as gcloud_firestore
    from google.oauth2 import service_account
    import google.auth
    
    if os.path.exists(CRED_PATH):
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return gcloud_firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        creds, _ = google.auth.default()
        return gcloud_firestore.Client(project=PROJECT_ID, credentials=creds, database="teetime")

def refresh_kakao_token(db):
    doc_ref = db.collection('config').document('kakao')
    doc = doc_ref.get()
    
    if not doc.exists:
        print("❌ Kakao config not found in Firestore. Please run setup_kakao.py first.")
        return None
        
    config = doc.to_dict()
    rest_api_key = config.get("rest_api_key")
    refresh_token = config.get("refresh_token")
    
    if not rest_api_key or not refresh_token:
        print("❌ Incomplete Kakao config.")
        return None
        
    print("Refreshing Kakao Access Token...")
    token_url = "https://kauth.kakao.com/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": rest_api_key,
        "refresh_token": refresh_token
    }
    
    response = requests.post(token_url, data=data)
    if response.status_code != 200:
        print("❌ Failed to refresh token!")
        print(response.json())
        return None
        
    tokens = response.json()
    access_token = tokens.get("access_token")
    
    # Sometimes Kakao issues a new refresh token if the old one is expiring soon
    new_refresh_token = tokens.get("refresh_token")
    if new_refresh_token:
        print("Got a new refresh token. Updating Firestore...")
        doc_ref.update({
            "refresh_token": new_refresh_token,
            "updated_at": google_firestore.SERVER_TIMESTAMP
        })
        
    return access_token

def format_price(price):
    try:
        p = int(price)
        if p >= 1000:
            return f"{p // 1000}k"
        return str(p)
    except:
        return str(price)

def get_lowest_prices(db):
    from datetime import timezone, timedelta
    import datetime
    KST = timezone(timedelta(hours=9))
    now_kst = datetime.datetime.now(KST)
    today_str = now_kst.strftime("%Y-%m-%d")
    current_time_str = now_kst.strftime("%H:%M")
    
    # 1. Get clubs with alert_enabled == True
    clubs_ref = db.collection('golf_clubs').where('alert_enabled', '==', True).stream()
    alert_clubs = []
    for c in clubs_ref:
        name = c.to_dict().get('name')
        if name:
            alert_clubs.append(name)
            
    if not alert_clubs:
        print("No clubs selected for alerts.")
        return None
        
    print(f"Target clubs for alert: {alert_clubs}")
    
    # 2. Get today's tee times
    tee_times_ref = db.collection('tee_times').where('date', '==', today_str).stream()
    
    club_mins = {}
    
    for t in tee_times_ref:
        data = t.to_dict()
        club = data.get('club_name')
        
        # Only interested in alert clubs
        if club not in alert_clubs:
            continue
            
        time_str = data.get('time', '')
        # Filter for times >= current time
        if time_str < current_time_str:
            continue
            
        price = data.get('price', float('inf'))
        try:
            price = int(price)
        except:
            continue
            
        if club not in club_mins or price < club_mins[club]['price']:
            club_mins[club] = {
                'price': price,
                'time': time_str,
                'source': data.get('source', '')
            }
            
    # 3. Format message
    if not club_mins:
        return f"🚀 [오늘의 구장별 티타임 줍줍]\n\n알림 설정된 구장 중 {current_time_str} 이후 잔여 티타임이 없습니다."
        
    msg_lines = [
        f"🚀 [오늘의 구장별 티타임 줍줍]",
        ""
    ]
    
    # Sort clubs by price (lowest first)
    sorted_clubs = sorted(club_mins.keys(), key=lambda c: club_mins[c]['price'])
    for club in sorted_clubs:
        info = club_mins[club]
        source_kr = "골팡" if info['source'] == 'golfpang' else ("티스캐너" if info['source'] == 'teescan' else info['source'])
        msg_lines.append(f"⛳ {club}: {info['time']} / {format_price(info['price'])} / {source_kr}")
            
    # If no clubs had times, we already returned early above, so we don't need to handle it again here.
    return "\n".join(msg_lines)

def send_kakao_message(access_token, text):
    url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    import json
    template_object = {
        "object_type": "text",
        "text": text,
        "link": {
            "web_url": "https://golf-ai-480805.du.r.appspot.com",
            "mobile_web_url": "https://golf-ai-480805.du.r.appspot.com"
        },
        "button_title": " "
    }
    
    data = {
        "template_object": json.dumps(template_object)
    }
    
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        try:
            print("✅ 카카오톡 메시지 전송 성공!")
        except UnicodeEncodeError:
            print("KakaoTalk message sent successfully!")
    else:
        try:
            print(f"❌ 카카오톡 전송 실패: {response.status_code}")
        except UnicodeEncodeError:
            print(f"Failed to send KakaoTalk message: {response.status_code}")
        print(response.text)

def main():
    db = init_firestore()
    if not db:
        return
        
    access_token = refresh_kakao_token(db)
    if not access_token:
        return
        
    message_text = get_lowest_prices(db)
    if not message_text:
        return
    try:
        print("Sending message:\n" + message_text)
    except UnicodeEncodeError:
        print("Sending message: (Contains emoji, skipped printing to Windows console)")
        
    send_kakao_message(access_token, message_text)

if __name__ == "__main__":
    main()

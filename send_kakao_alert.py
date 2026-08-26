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
            "web_url": "https://golf-ai-480805.web.app/",
            "mobile_web_url": "https://golf-ai-480805.web.app/"
        },
        "button_title": "나와바리 골프"
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



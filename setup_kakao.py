import os
import requests
import firebase_admin
from firebase_admin import credentials, firestore
import webbrowser

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

def main():
    print("=" * 60)
    print("📱 카카오톡 '나에게 보내기' 자동화 설정 스크립트")
    print("=" * 60)
    print("1. 카카오 디벨로퍼스(https://developers.kakao.com/)에 로그인하세요.")
    print("2. [내 애플리케이션] -> [애플리케이션 추가] 를 클릭해 앱을 만듭니다.")
    print("3. 좌측 [카카오 로그인] 메뉴에서 상태를 'ON'으로 변경합니다.")
    print("4. [카카오 로그인] -> [Redirect URI]에 'http://localhost' 를 등록합니다.")
    print("5. 좌측 [카카오톡 메시지] 메뉴에서 '카카오톡 메시지 API'를 활성화합니다.")
    print("6. 좌측 [동의항목]에서 '카카오톡 메시지 전송(talk_message)'을 '선택 동의' 또는 '필수 동의'로 설정합니다.")
    print("7. [앱 키] 메뉴로 이동하여 'REST API 키'를 복사합니다.\n")
    
    rest_api_key = input("👉 복사한 REST API 키를 붙여넣으세요: ").strip()
    if not rest_api_key:
        print("API 키가 입력되지 않았습니다.")
        return
        
    redirect_uri = "http://localhost"
    
    auth_url = f"https://kauth.kakao.com/oauth/authorize?client_id={rest_api_key}&redirect_uri={redirect_uri}&response_type=code&scope=talk_message"
    
    print("\n" + "=" * 60)
    print("아래 링크가 브라우저에서 열립니다. (안 열리면 직접 복사해서 접속하세요)")
    print("접속 후 카카오 로그인을 진행하고, 권한 동의를 해주세요.")
    print(auth_url)
    print("=" * 60)
    
    try:
        webbrowser.open(auth_url)
    except:
        pass
        
    print("\n동의를 완료하면 브라우저 주소창이 아래와 같이 바뀝니다.")
    print("http://localhost/?code=어쩌구저쩌구...")
    
    auth_code = input("👉 주소창에서 'code=' 뒤에 있는 긴 영어/숫자 코드를 복사해서 붙여넣으세요: ").strip()
    if not auth_code:
        print("인증 코드가 입력되지 않았습니다.")
        return
        
    print("\n토큰 발급을 요청합니다...")
    token_url = "https://kauth.kakao.com/oauth/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": rest_api_key,
        "redirect_uri": redirect_uri,
        "code": auth_code
    }
    
    response = requests.post(token_url, data=data)
    if response.status_code != 200:
        print("❌ 토큰 발급 실패!")
        print(response.json())
        return
        
    tokens = response.json()
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    
    if not refresh_token:
        print("❌ Refresh 토큰을 받지 못했습니다.")
        return
        
    print("✅ 토큰 발급 성공! Firestore에 저장합니다...")
    
    db = init_firestore()
    if not db:
        print("❌ Firestore 초기화 실패")
        return
        
    kakao_config = {
        "rest_api_key": rest_api_key,
        "refresh_token": refresh_token,
        "updated_at": firestore.SERVER_TIMESTAMP
    }
    
    db.collection('config').document('kakao').set(kakao_config)
    print("🎉 완료! 카카오톡 설정이 성공적으로 저장되었습니다.")
    print("이제 매일 아침 자동으로 메시지가 전송됩니다.")

if __name__ == "__main__":
    main()

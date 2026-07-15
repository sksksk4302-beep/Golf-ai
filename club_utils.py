import os
import firebase_admin
from firebase_admin import credentials, firestore

PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

_cached_clubs = None

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

def get_golf_clubs(db=None, force_refresh=False):
    global _cached_clubs
    if _cached_clubs is not None and not force_refresh:
        return _cached_clubs
        
    if db is None:
        db = init_firestore()
        
    try:
        clubs_ref = db.collection('golf_clubs').stream()
        clubs = []
        for doc in clubs_ref:
            club_data = doc.to_dict()
            club_data['id'] = doc.id
            clubs.append(club_data)
        _cached_clubs = clubs
        return clubs
    except Exception as e:
        print(f"Error fetching golf clubs from Firestore: {e}")
        return []

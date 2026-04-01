import firebase_admin
from firebase_admin import credentials, firestore
import os
from datetime import datetime

# Configuration
PROJECT_ID = "golf-ai-480805"
CRED_PATH = "service-account.json"

def init_firestore():
    if os.path.exists(CRED_PATH):
        from google.oauth2 import service_account
        from google.cloud import firestore
        cred = service_account.Credentials.from_service_account_file(CRED_PATH)
        return firestore.Client(project=PROJECT_ID, credentials=cred, database="teetime")
    else:
        import google.auth
        from google.cloud import firestore
        credentials, project = google.auth.default()
        return firestore.Client(project=PROJECT_ID, credentials=credentials, database="teetime")

db = init_firestore()

print("Checking for items with benefit...")

# Query for items with non-empty benefit
# Note: != "" query might not work as expected in all Firestore modes if not indexed,
# so we'll just pull some Teescan items and check them.

docs = db.collection('tee_times')\
    .where('source', '==', 'teescan')\
    .limit(50)\
    .stream()

count = 0
benefit_count = 0
for doc in docs:
    data = doc.to_dict()
    count += 1
    benefit = data.get('benefit')
    if benefit:
        print(f"Found benefit: {benefit} | Club: {data.get('club_name')} | Date: {data.get('date')} | Price: {data.get('price')}")
        benefit_count += 1
    else:
        # verify if key exists but is empty
        if 'benefit' in data:
             pass # Key exists
        else:
             print(f"Missing benefit key entirely: {doc.id}")

print(f"--------------------------------------------------")
print(f"Total Teescan items checked: {count}")
print(f"Items with non-empty benefit: {benefit_count}")

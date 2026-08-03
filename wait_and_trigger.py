import subprocess
import time

print("Waiting for Firestore index to finish...")
while True:
    result = subprocess.run(
        ["gcloud", "firestore", "operations", "list", "--database=teetime", "--project=golf-ai-480805"],
        capture_output=True, text=True
    )
    if "PROCESSING" not in result.stdout:
        print("Index creation finished! Triggering Kakao alert...")
        break
    print("Still processing, waiting 15 seconds...")
    time.sleep(15)

# Trigger Kakao
subprocess.run([
    "gcloud", "run", "jobs", "execute", "kakao-alert-job", "--region", "asia-northeast3", "--wait"
])
print("Done.")

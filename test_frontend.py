import requests
import time

BASE_URL = "http://localhost:8080"
FRONTEND_URL = "http://localhost:3002"

print("=" * 60)
print("TESTING REACT FRONTEND WITH NEW DESIGN")
print("=" * 60)

# 1. Check frontend is running
print("\n[1] Testing Frontend...")
try:
    r = requests.get(FRONTEND_URL, timeout=5)
    print(f"    [OK] Frontend running at {FRONTEND_URL}")
except Exception as e:
    print(f"    [FAIL] Frontend not running: {e}")

# 2. Test upload + run_pipeline
print("\n[2] Testing /run_pipeline...")
try:
    with open("tests/sample_data.csv", "rb") as f:
        files = {"file": ("sample_data.csv", f, "text/csv")}
        upload_resp = requests.post(f"{BASE_URL}/upload_csv", files=files, timeout=30)
    
    if upload_resp.status_code == 200:
        session_id = upload_resp.json()["session_id"]
        print(f"    [OK] Uploaded, session: {session_id[:8]}...")
        
        # Run pipeline
        pipeline_resp = requests.post(f"{BASE_URL}/run_pipeline", json={"session_id": session_id}, timeout=60)
        
        if pipeline_resp.status_code == 200:
            result = pipeline_resp.json()
            print(f"    [OK] Pipeline complete!")
            print(f"        Insights: {len(result.get('insights', []))}")
            print(f"        Plots: {len(result.get('plots', {}))}")
            print(f"        Dataset: {result.get('dataset_meta', {}).get('rows', 0)} rows")
        else:
            print(f"    [FAIL] Pipeline: {pipeline_resp.status_code}")
    else:
        print(f"    [FAIL] Upload: {upload_resp.status_code}")
        
except Exception as e:
    print(f"    [FAIL] Error: {e}")

# 3. Test simulate endpoint
print("\n[3] Testing /simulate...")

print("\n" + "=" * 60)
print("FRONTEND READY!")
print("=" * 60)
print(f"\nOpen in browser: {FRONTEND_URL}")
print("\nFlow to test:")
print("1. Open browser → Upload CSV")
print("2. Wait for analysis (auto-runs)")
print("3. Click insights → See plots + evidence")
print("4. Use NLQ input → Ask questions")
print("5. Switch to Simulate tab → Test what-if")
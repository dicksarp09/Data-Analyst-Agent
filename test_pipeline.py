import requests
import json
import time
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_URL = "http://127.0.0.1:8080"

print("=" * 60)
print("TESTING FULL PIPELINE")
print("=" * 60)

# 1. Upload CSV
print("\n[1] Uploading CSV...")
with open("tests/sample_data.csv", "rb") as f:
    files = {"file": ("sample_data.csv", f, "text/csv")}
    response = requests.post(f"{BASE_URL}/upload_csv", files=files)

if response.status_code == 200:
    data = response.json()
    session_id = data.get("session_id")
    print(f"    [OK] Upload successful! Session: {session_id[:8]}...")
else:
    print(f"    [FAIL] Upload failed: {response.text}")
    exit(1)

time.sleep(1)

# 2. Get Schema
print("\n[2] Getting Schema...")
response = requests.get(f"{BASE_URL}/schema/{session_id}")
if response.status_code == 200:
    schema = response.json()
    print(f"    [OK] Schema: {len(schema.get('columns', []))} columns")
    print(f"    Columns: {schema.get('columns')}")
else:
    print(f"    [FAIL] Schema failed")

# 3. Get Data Quality
print("\n[3] Getting Data Quality...")
response = requests.get(f"{BASE_URL}/data_quality/{session_id}")
if response.status_code == 200:
    quality = response.json()
    print(f"    [OK] Total rows: {quality.get('total_rows')}")
    print(f"    Valid rows: {quality.get('valid_rows')}")
else:
    print(f"    [FAIL] Quality check failed")

# 4. Run Explore (Signal Detection)
print("\n[4] Running Signal Detection...")
response = requests.post(f"{BASE_URL}/explore", json={"session_id": session_id})
if response.status_code == 200:
    signals = response.json()
    print(f"    [OK] Found {signals.get('signal_count')} signals")
else:
    print(f"    [FAIL] Explore failed: {response.text}")

# 5. Generate Hypotheses
print("\n[5] Generating Hypotheses...")
response = requests.post(f"{BASE_URL}/hypotheses", json={"session_id": session_id})
if response.status_code == 200:
    hypotheses = response.json()
    print(f"    [OK] Generated {hypotheses.get('node_count')} hypotheses")
else:
    print(f"    [FAIL] Hypotheses failed: {response.text}")

# 6. Execute Hypotheses
print("\n[6] Executing Hypotheses...")
response = requests.post(f"{BASE_URL}/execute", json={"session_id": session_id})
if response.status_code == 200:
    execution = response.json()
    print(f"    [OK] Execution complete")
    print(f"    Accepted: {len(execution.get('accepted_hypotheses', []))}")
    print(f"    Rejected: {len(execution.get('rejected_hypotheses', []))}")
else:
    print(f"    [FAIL] Execute failed: {response.text}")

# 7. Generate Insights & Plots (Phase 4)
print("\n[7] Generating Phase 4 Insights & Plots...")
response = requests.post(f"{BASE_URL}/phase4/run", json={"session_id": session_id})
if response.status_code == 200:
    phase4 = response.json()
    print(f"    [OK] Insights: {phase4.get('insight_count')}")
    print(f"    [OK] Plots: {phase4.get('plot_count')}")
else:
    print(f"    [FAIL] Phase4 failed: {response.text}")

# 8. Test NLQ Interface
print("\n[8] Testing NLQ Interface...")
response = requests.post(f"{BASE_URL}/nlq/ask", json={
    "question": "What is the revenue trend?",
    "session_id": session_id
})
if response.status_code == 200:
    nlq = response.json()
    print(f"    [OK] NLQ Response: {nlq.get('answer')[:100]}...")
    print(f"    Confidence: {nlq.get('confidence')}")
else:
    print(f"    [FAIL] NLQ failed (expected - needs proper setup)")

# 9. Test Report Export
print("\n[9] Testing Report Export...")
response = requests.get(f"{BASE_URL}/report/{session_id}?format=json")
if response.status_code == 200:
    print(f"    [OK] JSON Report: {len(response.text)} chars")
else:
    print(f"    [FAIL] Report failed")

response = requests.get(f"{BASE_URL}/report/{session_id}?format=html")
if response.status_code == 200:
    print(f"    [OK] HTML Report: {len(response.text)} chars")
else:
    print(f"    [FAIL] HTML Report failed")

# 10. Test Chat Interface (using existing /chat endpoint with 'query')
print("\n[10] Testing Chat Interface...")
response = requests.post(f"{BASE_URL}/chat", json={
    "session_id": session_id,
    "query": "What are the key insights?"
})
if response.status_code == 200:
    chat = response.json()
    print(f"    [OK] Chat Response: {str(chat.get('answer'))[:100]}...")
else:
    print(f"    [FAIL] Chat failed: {response.text[:100]}")

print("\n" + "=" * 60)
print("PIPELINE TEST COMPLETE!")
print("=" * 60)

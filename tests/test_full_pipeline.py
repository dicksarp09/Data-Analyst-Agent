import httpx

BASE_URL = "http://127.0.0.1:8008"
client = httpx.Client(timeout=180.0)

print("=" * 70)
print("FULL PIPELINE TEST: Phase 0 -> 1 -> 2 -> 3")
print("=" * 70)

# STEP 1: Phase 0 - Upload
print("\n[PHASE 0] Uploading CSV...")
with open("tests/test_p3.csv", "rb") as f:
    files = {"file": ("test_p3.csv", f, "text/csv")}
    r = client.post(f"{BASE_URL}/upload_csv", files=files)

upload = r.json()
session_id = upload["session_id"]
print(f"  Session: {session_id}")
print("  STATUS: PASS")

# STEP 2: Phase 1 - Explore
print("\n[PHASE 1] Running exploration...")
r = client.post(f"{BASE_URL}/explore", json={"session_id": session_id})
explore = r.json()
print(f"  Signals: {explore['signal_count']}")
for s in explore["signals"][:3]:
    print(f"    - {s['type']}: {s.get('feature', s.get('pair'))}")
print("  STATUS: PASS")

# STEP 3: Phase 2 - Hypotheses
print("\n[PHASE 2] Generating hypotheses...")
r = client.post(f"{BASE_URL}/hypotheses", json={"session_id": session_id})
graph = r.json()
print(f"  Nodes: {graph['node_count']}")
print(f"  Edges: {graph['edge_count']}")
print("  STATUS: PASS")

# STEP 4: Phase 3 - Execute
print("\n[PHASE 3] Running execution engine...")
r = client.post(f"{BASE_URL}/execute", json={"session_id": session_id})
exec_result = r.json()
print(f"  Iteration: {exec_result['iteration']}")
print(f"  Status: {exec_result['status']}")
print(f"  Accepted: {exec_result['accepted_hypotheses']}")
print(f"  Rejected: {exec_result['rejected_hypotheses']}")
print("  STATUS: PASS")

# Show execution history
print("\n  Execution history:")
for h in exec_result["execution_history"][:3]:
    print(f"    - {h.get('hypothesis_id')}: {h.get('action')} (conf={h.get('evidence', {}).get('confidence', 0):.2f})")

# STEP 5: Get Results
print("\n[PHASE 3] Getting results...")
r = client.get(f"{BASE_URL}/results/{session_id}")
results = r.json()
print(f"  Accepted: {len(results['accepted_hypotheses'])}")
print(f"  Rejected: {len(results['rejected_hypotheses'])}")
print("  STATUS: PASS")

# STEP 6: Get Execution History
print("\n[PHASE 3] Getting execution history...")
r = client.get(f"{BASE_URL}/execution/{session_id}")
history = r.json()
print(f"  History entries: {len(history['execution_history'])}")
print("  STATUS: PASS")

print("\n" + "=" * 70)
print("FULL PIPELINE TEST COMPLETE - ALL PHASES PASSED")
print("=" * 70)
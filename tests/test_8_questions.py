"""
End-to-end test for all 8 question types using /auto_analyze endpoint
"""
import os
import sys
import uuid
import json

sys.path.insert(0, os.getcwd())

import httpx

BASE_URL = "http://127.0.0.1:8015"

QUESTIONS = {
    "descriptive": "What is the average revenue?",
    "trend": "Why did revenue drop over time?",
    "comparative": "Why do mobile users convert less than desktop users?",
    "causal": "What caused conversion rate decline?",
    "anomaly": "What unusual behavior exists in revenue?",
    "correlation": "Is latency related to conversion?",
    "segment_discovery": "What user groups exist in the data?",
    "change_detection": "What changed before and after Jan 15?",
}


def test_question_types():
    client = httpx.Client(base_url=BASE_URL, timeout=120.0)
    
    print("\n=== UPLOAD CSV ===")
    with open("tests/sample_data.csv", "rb") as f:
        upload_resp = client.post(
            "/upload_csv",
            files={"file": f}
        )
    
    if upload_resp.status_code != 200:
        print(f"Upload failed: {upload_resp.status_code} - {upload_resp.text}")
        return
    
    upload_data = upload_resp.json()
    session_id = upload_data["session_id"]
    print(f"Session ID: {session_id}")
    print(f"Upload: {upload_data}")
    
    # Verify session is registered
    schema_resp = client.get(f"/schema/{session_id}")
    print(f"Schema check: {schema_resp.status_code}")
    
    print("\n=== PHASE 1: EXPLORE ===")
    explore_resp = client.post("/explore", json={"session_id": session_id})
    print(f"Explore: {explore_resp.json()}")
    
    print("\n=== PHASE 2: HYPOTHESES ===")
    hyp_resp = client.post("/hypotheses", json={"session_id": session_id})
    print(f"Hypotheses: {hyp_resp.json()}")
    
    print("\n=== PHASE 3: EXECUTE ===")
    exec_resp = client.post("/execute", json={"session_id": session_id})
    print(f"Execute: {exec_resp.json()}")
    
    print("\n=== PHASE 4: RUN ===")
    phase4_resp = client.post("/phase4/run", json={"session_id": session_id})
    print(f"Phase4: {phase4_resp.json()}")
    
    print("\n" + "="*60)
    print("TESTING 8 QUESTION TYPES")
    print("="*60)
    
    results = {}
    
    for qtype, question in QUESTIONS.items():
        print(f"\n--- {qtype.upper()} ---")
        print(f"Question: {question}")
        
        try:
            resp = client.post("/auto_analyze", json={
                "session_id": session_id,
                "query": question
            })
            
            if resp.status_code == 200:
                data = resp.json()
                results[qtype] = {
                    "status": "success",
                    "question_type": data.get("question_type"),
                    "executed_phases": data.get("executed_phases"),
                    "answer": data.get("answer", "")[:200] + "..." if data.get("answer") else "",
                    "plots_count": data.get("plots_count", 0),
                    "insights_count": data.get("insights_count", 0),
                }
                print(f"  Detected Type: {data.get('question_type')}")
                print(f"  Executed Phases: {data.get('executed_phases')}")
                print(f"  Answer: {results[qtype]['answer'][:100]}...")
            else:
                results[qtype] = {
                    "status": "error",
                    "error": resp.text
                }
                print(f"  ERROR: {resp.status_code} - {resp.text}")
                
        except Exception as e:
            results[qtype] = {
                "status": "exception",
                "error": str(e)
            }
            print(f"  EXCEPTION: {e}")
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    for qtype, result in results.items():
        status = result.get("status", "unknown")
        print(f"{qtype}: {status}")
        if status == "success":
            print(f"  - Detected: {result.get('question_type')}")
            print(f"  - Phases: {result.get('executed_phases')}")
            print(f"  - Plots: {result.get('plots_count')}, Insights: {result.get('insights_count')}")
    
    success_count = sum(1 for r in results.values() if r.get("status") == "success")
    print(f"\nSuccess: {success_count}/8")
    
    with open("data/test_results.json", "w") as f:
        json.dump({"session_id": session_id, "results": results}, f, indent=2, default=str)
    
    print("\nResults saved to data/test_results.json")


if __name__ == "__main__":
    test_question_types()
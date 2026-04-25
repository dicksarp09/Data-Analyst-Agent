"""
Comprehensive Evaluation Framework for Data Analyst Agent
Evaluates all 5 phases: Data Integrity, Signal Discovery, Hypothesis Quality, Execution, Insights
"""
import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
import httpx

BASE_URL = "http://127.0.0.1:8015"
TEST_DATA = "tests/sample_data.csv"

# Ground truth for test data
GROUND_TRUTH = {
    "columns": ["user_id", "revenue", "signup_date", "device", "country", "latency_ms", "sessions", "conversion", "age_group", "region"],
    "row_count": 200,
    "expected_types": {
        "user_id": "int",
        "revenue": "float",
        "signup_date": "datetime",
        "device": "string",
        "country": "string",
        "latency_ms": "int",
        "sessions": "int",
        "conversion": "float",
        "age_group": "string",
        "region": "string"
    },
    "known_correlations": [
        ("sessions", "conversion"),
        ("latency_ms", "sessions")
    ],
    "known_outliers": [7],  # user_id 7 has revenue=999999.99
    "segments": ["device", "country", "region", "age_group"]
}


class PhaseEvaluator:
    def __init__(self):
        self.results = {}
        self.session_id = None
    
    def upload_and_initialize(self):
        """Upload CSV and run all phases"""
        client = httpx.Client(base_url=BASE_URL, timeout=120.0)
        
        print("\n=== UPLOADING CSV ===")
        with open(TEST_DATA, "rb") as f:
            resp = client.post("/upload_csv", files={"file": f})
        
        if resp.status_code != 200:
            raise Exception(f"Upload failed: {resp.text}")
        
        self.session_id = resp.json()["session_id"]
        print(f"Session: {self.session_id}")
        
        print("\n=== RUNNING PHASE 1: EXPLORE ===")
        resp = client.post("/explore", json={"session_id": self.session_id})
        if resp.status_code != 200:
            raise Exception(f"Explore failed: {resp.text}")
        self.explore_result = resp.json()
        
        print("\n=== RUNNING PHASE 2: HYPOTHESES ===")
        resp = client.post("/hypotheses", json={"session_id": self.session_id})
        if resp.status_code != 200:
            raise Exception(f"Hypotheses failed: {resp.text}")
        self.hypothesis_result = resp.json()
        
        print("\n=== RUNNING PHASE 3: EXECUTE ===")
        try:
            resp = client.post("/execute", json={"session_id": self.session_id})
            if resp.status_code != 200:
                print(f"Execute failed with {resp.status_code}, trying continue...")
                self.execute_result = {"accepted_hypotheses": [], "rejected_hypotheses": [], "execution_history": [], "iteration": 0, "status": "failed"}
            else:
                self.execute_result = resp.json()
        except Exception as e:
            print(f"Execute error: {e}")
            self.execute_result = {"accepted_hypotheses": [], "rejected_hypotheses": [], "execution_history": [], "iteration": 0, "status": "error"}
        
        print("\n=== RUNNING PHASE 4: INSIGHTS ===")
        try:
            resp = client.post("/phase4/run", json={"session_id": self.session_id})
            if resp.status_code != 200:
                print(f"Phase4 failed with {resp.status_code}, trying continue...")
                self.phase4_result = {"insight_count": 0, "plot_count": 0, "insights": [], "plots": []}
            else:
                self.phase4_result = resp.json()
        except Exception as e:
            print(f"Phase4 error: {e}")
            self.phase4_result = {"insight_count": 0, "plot_count": 0, "insights": [], "plots": []}
        
        return client


class Phase0Evaluator:
    """Evaluate Data Integrity Layer (Phase 0)"""
    
    def __init__(self, session_id):
        self.session_id = session_id
        self.client = httpx.Client(base_url=BASE_URL, timeout=30.0)
    
    def evaluate(self):
        results = {"phase": "Phase 0: Data Integrity", "metrics": {}}
        
        # A. Schema Correctness
        schema_resp = self.client.get(f"/schema/{self.session_id}")
        schema = schema_resp.json()
        
        correct_types = 0
        total_types = len(GROUND_TRUTH["expected_types"])
        
        for col, expected_type in GROUND_TRUTH["expected_types"].items():
            if col in schema.get("inferred_types", {}):
                inferred = schema["inferred_types"][col]
                # Map to standard types
                if expected_type == "int" and inferred in ["int", "bigint"]:
                    correct_types += 1
                elif expected_type == "float" and inferred in ["float", "double"]:
                    correct_types += 1
                elif expected_type == "datetime" and inferred in ["datetime", "date", "timestamp"]:
                    correct_types += 1
                elif expected_type == "string" and inferred in ["string", "varchar"]:
                    correct_types += 1
        
        results["metrics"]["schema_accuracy"] = correct_types / total_types
        
        # B. Data Quality Detection
        quality_resp = self.client.get(f"/data_quality/{self.session_id}")
        quality = quality_resp.json()
        
        # Check missingness detection
        results["metrics"]["total_rows"] = quality.get("total_rows", 0)
        results["metrics"]["valid_rows"] = quality.get("valid_rows", 0)
        results["metrics"]["quarantined_rows"] = quality.get("quarantined_rows", 0)
        
        # Check outlier detection
        outliers = quality.get("outliers", {})
        detected_outlier_column = None
        for col, data in outliers.items():
            if isinstance(data, dict) and data.get("count", 0) > 0:
                detected_outlier_column = col
                break
            elif isinstance(data, list) and len(data) > 0:
                detected_outlier_column = col
                break
        
        results["metrics"]["outlier_detection"] = detected_outlier_column is not None
        results["metrics"]["outlier_column_detected"] = detected_outlier_column
        
        # C. SQL Correctness
        query_resp = self.client.post("/query", json={
            "session_id": self.session_id,
            "sql": "SELECT COUNT(*) as cnt FROM clean_dataset WHERE device = 'mobile'"
        })
        query_result = query_resp.json()
        results["metrics"]["sql_execution_success"] = query_resp.status_code == 200
        
        # Handle different response formats
        data = query_result.get("data", [])
        if isinstance(data, list) and len(data) > 0:
            count_val = data[0].get("cnt", 0) if isinstance(data[0], dict) else data[0]
            results["metrics"]["filter_correctness"] = count_val > 0
        else:
            results["metrics"]["filter_correctness"] = False
        
        return results


class Phase1Evaluator:
    """Evaluate Signal Discovery Quality (Phase 1)"""
    
    def __init__(self, session_id, explore_result):
        self.session_id = session_id
        self.explore_result = explore_result
        self.client = httpx.Client(base_url=BASE_URL, timeout=30.0)
    
    def evaluate(self):
        results = {"phase": "Phase 1: Signal Discovery", "metrics": {}}
        
        signals = self.explore_result.get("signals", [])
        signal_count = self.explore_result.get("signal_count", 0)
        
        results["metrics"]["total_signals"] = signal_count
        
        # Count by type
        signal_types = {}
        for s in signals:
            t = s.get("type", "unknown")
            signal_types[t] = signal_types.get(t, 0) + 1
        
        results["metrics"]["signals_by_type"] = signal_types
        
        # A. Correlation validity
        correlations = [s for s in signals if s.get("type") == "correlation"]
        results["metrics"]["correlation_count"] = len(correlations)
        
        # Check known correlations
        known_found = 0
        for pair in GROUND_TRUTH["known_correlations"]:
            for corr in correlations:
                if pair[0] in corr.get("pair", []) and pair[1] in corr.get("pair", []):
                    known_found += 1
                    break
        
        results["metrics"]["known_correlations_found"] = known_found
        results["metrics"]["correlation_precision"] = known_found / len(GROUND_TRUTH["known_correlations"])
        
        # B. Anomaly detection
        anomalies = [s for s in signals if s.get("type") == "anomaly"]
        results["metrics"]["anomaly_count"] = len(anomalies)
        results["metrics"]["anomaly_detected"] = len(anomalies) > 0
        
        # C. Clustering
        clusters = [s for s in signals if s.get("type") == "cluster"]
        results["metrics"]["cluster_count"] = len(clusters)
        
        # D. Trend detection
        trends = [s for s in signals if s.get("type") == "trend_break"]
        results["metrics"]["trend_break_count"] = len(trends)
        
        # Signal stability (check confidence scores)
        high_confidence = sum(1 for s in signals if s.get("confidence", 0) >= 0.7)
        results["metrics"]["high_confidence_signals"] = high_confidence
        results["metrics"]["signal_stability_ratio"] = high_confidence / max(signal_count, 1)
        
        return results


class Phase2Evaluator:
    """Evaluate Hypothesis Quality (Phase 2)"""
    
    def __init__(self, session_id, hypothesis_result):
        self.session_id = session_id
        self.hypothesis_result = hypothesis_result
        self.client = httpx.Client(base_url=BASE_URL, timeout=30.0)
    
    def evaluate(self):
        results = {"phase": "Phase 2: Hypothesis Quality", "metrics": {}}
        
        nodes = self.hypothesis_result.get("nodes", [])
        edges = self.hypothesis_result.get("edges", [])
        
        results["metrics"]["total_hypotheses"] = len(nodes)
        results["metrics"]["total_edges"] = len(edges)
        
        # A. Testability - check if hypotheses have required fields
        testable = sum(1 for n in nodes if n.get("features") and n.get("expected_observations"))
        results["metrics"]["testable_hypotheses"] = testable
        results["metrics"]["testability_rate"] = testable / max(len(nodes), 1)
        
        # B. Groundedness - check if hypotheses reference signals
        grounded = sum(1 for n in nodes if n.get("source_signals"))
        results["metrics"]["grounded_hypotheses"] = grounded
        results["metrics"]["groundedness_rate"] = grounded / max(len(nodes), 1)
        
        # C. Diversity - check hypothesis type distribution
        types = {}
        for n in nodes:
            t = n.get("type", "unknown")
            types[t] = types.get(t, 0) + 1
        
        results["metrics"]["hypothesis_types"] = types
        results["metrics"]["diversity_score"] = len(types)  # More types = more diverse
        
        # D. Ranking quality - check if scores are reasonable
        scores = [n.get("score", 0) for n in nodes]
        results["metrics"]["score_range"] = max(scores) - min(scores) if scores else 0
        results["metrics"]["has_ranked_hypotheses"] = len([s for s in scores if s != 0]) > 0
        
        # E. Redundancy check
        unique_descriptions = set(n.get("description", "") for n in nodes)
        results["metrics"]["unique_descriptions"] = len(unique_descriptions)
        results["metrics"]["redundancy_rate"] = 1 - (len(unique_descriptions) / max(len(nodes), 1))
        
        return results


class Phase3Evaluator:
    """Evaluate Execution Quality (Phase 3)"""
    
    def __init__(self, session_id, execute_result):
        self.session_id = session_id
        self.execute_result = execute_result
        self.client = httpx.Client(base_url=BASE_URL, timeout=30.0)
    
    def evaluate(self):
        results = {"phase": "Phase 3: Execution Quality", "metrics": {}}
        
        accepted = self.execute_result.get("accepted_hypotheses", [])
        rejected = self.execute_result.get("rejected_hypotheses", [])
        history = self.execute_result.get("execution_history", [])
        iteration = self.execute_result.get("iteration", 0)
        
        results["metrics"]["accepted_count"] = len(accepted)
        results["metrics"]["rejected_count"] = len(rejected)
        results["metrics"]["total_iterations"] = iteration
        results["metrics"]["execution_status"] = self.execute_result.get("status", "unknown")
        
        # A. Execution success
        results["metrics"]["execution_success"] = len(accepted) + len(rejected) > 0
        
        # B. Decision correctness - check if decisions match evidence
        valid_decisions = 0
        for entry in history:
            evidence = entry.get("evidence", {})
            action = entry.get("action", "")
            confidence = evidence.get("confidence", 0)
            
            # Check if decision matches evidence thresholds
            if action == "accept" and confidence >= 0.5:
                valid_decisions += 1
            elif action == "reject" and confidence < 0.5:
                valid_decisions += 1
            elif action == "refine":
                valid_decisions += 1
        
        results["metrics"]["valid_decisions"] = valid_decisions
        results["metrics"]["decision_accuracy"] = valid_decisions / max(len(history), 1) if history else 0
        
        # C. Loop stability
        results["metrics"]["loop_stable"] = iteration < 10  # Should converge quickly
        
        # D. Convergence rate
        results["metrics"]["convergence_rate"] = iteration / max(len(history), 1) if history else 0
        
        return results


class Phase4Evaluator:
    """Evaluate Insight + Plot Quality (Phase 4)"""
    
    def __init__(self, session_id, phase4_result):
        self.session_id = session_id
        self.phase4_result = phase4_result
        self.client = httpx.Client(base_url=BASE_URL, timeout=30.0)
    
    def evaluate(self):
        results = {"phase": "Phase 4: Insight + Plot Quality", "metrics": {}}
        
        insights = self.phase4_result.get("insights", [])
        plots = self.phase4_result.get("plots", [])
        
        results["metrics"]["insight_count"] = len(insights)
        results["metrics"]["plot_count"] = len(plots)
        
        # A. Insight evaluation
        grounded_insights = 0
        for ins in insights:
            # Check if insight has hypothesis anchor
            if ins.get("hypothesis_id"):
                grounded_insights += 1
        
        results["metrics"]["grounded_insights"] = grounded_insights
        results["metrics"]["faithfulness_score"] = grounded_insights / max(len(insights), 1)
        
        # Check confidence alignment
        misaligned = 0
        for ins in insights:
            conf = ins.get("confidence", 0)
            text = ins.get("insight", "").lower()
            if conf < 0.5 and ("certain" in text or "definitely" in text):
                misaligned += 1
        
        results["metrics"]["misaligned_insights"] = misaligned
        results["metrics"]["alignment_score"] = 1 - (misaligned / max(len(insights), 1))
        
        # B. Plot evaluation
        valid_plots = 0
        for plot in plots:
            # Check if plot has required fields
            if plot.get("type") and plot.get("plot_id"):
                valid_plots += 1
        
        results["metrics"]["valid_plots"] = valid_plots
        results["metrics"]["plot_validity_rate"] = valid_plots / max(len(plots), 1)
        
        # Check plot grounding
        linked_plots = sum(1 for p in plots if p.get("linked_hypothesis"))
        results["metrics"]["grounded_plots"] = linked_plots
        results["metrics"]["plot_grounding_score"] = linked_plots / max(len(plots), 1)
        
        # C. Traceability
        traceable = 0
        for ins in insights:
            if ins.get("hypothesis_id"):
                traceable += 1
        results["metrics"]["traceable_insights"] = traceable
        results["metrics"]["traceability_score"] = traceable / max(len(insights), 1)
        
        return results


class CrossPhaseEvaluator:
    """Evaluate end-to-end consistency across phases"""
    
    def __init__(self, phase_results):
        self.phase_results = phase_results
    
    def evaluate(self):
        results = {"phase": "Cross-Phase Evaluation", "metrics": {}}
        
        # 1. End-to-end factual consistency
        phase0 = self.phase_results.get("Phase 0: Data Integrity", {})
        phase1 = self.phase_results.get("Phase 1: Signal Discovery", {})
        phase2 = self.phase_results.get("Phase 2: Hypothesis Quality", {})
        phase3 = self.phase_results.get("Phase 3: Execution Quality", {})
        phase4 = self.phase_results.get("Phase 4: Insight + Plot Quality", {})
        
        # Check data flow consistency
        data_rows = phase0.get("metrics", {}).get("total_rows", 0)
        signals = phase1.get("metrics", {}).get("total_signals", 0)
        hypotheses = phase2.get("metrics", {}).get("total_hypotheses", 0)
        executed = phase3.get("metrics", {}).get("accepted_count", 0) + phase3.get("metrics", {}).get("rejected_count", 0)
        
        results["metrics"]["data_to_signals"] = signals > 0 and data_rows > 0
        results["metrics"]["signals_to_hypotheses"] = hypotheses > 0 and signals > 0
        results["metrics"]["hypotheses_executed"] = executed > 0 and hypotheses > 0
        
        # 2. Traceability
        traceability = phase4.get("metrics", {}).get("traceability_score", 0)
        results["metrics"]["traceability_score"] = traceability
        
        # 3. Decision coherence
        decision_acc = phase3.get("metrics", {}).get("decision_accuracy", 0)
        results["metrics"]["decision_coherence"] = decision_acc > 0.5
        
        # 4. Hallucination indicators
        misaligned = phase4.get("metrics", {}).get("misaligned_insights", 0)
        results["metrics"]["hallucination_indicators"] = misaligned
        
        return results


def run_evaluation():
    print("=" * 70)
    print("DATA ANALYST AGENT - COMPREHENSIVE EVALUATION")
    print("=" * 70)
    
    # Initialize and run all phases
    evaluator = PhaseEvaluator()
    client = evaluator.upload_and_initialize()
    
    all_results = {}
    
    # Phase 0 Evaluation
    print("\n--- PHASE 0 EVALUATION ---")
    p0 = Phase0Evaluator(evaluator.session_id)
    p0_results = p0.evaluate()
    all_results["Phase 0: Data Integrity"] = p0_results
    print(f"Schema Accuracy: {p0_results['metrics']['schema_accuracy']:.1%}")
    print(f"Outlier Detected: {p0_results['metrics']['outlier_detection']}")
    
    # Phase 1 Evaluation
    print("\n--- PHASE 1 EVALUATION ---")
    p1 = Phase1Evaluator(evaluator.session_id, evaluator.explore_result)
    p1_results = p1.evaluate()
    all_results["Phase 1: Signal Discovery"] = p1_results
    print(f"Total Signals: {p1_results['metrics']['total_signals']}")
    print(f"Correlation Precision: {p1_results['metrics']['correlation_precision']:.1%}")
    print(f"Signal Stability: {p1_results['metrics']['signal_stability_ratio']:.1%}")
    
    # Phase 2 Evaluation
    print("\n--- PHASE 2 EVALUATION ---")
    p2 = Phase2Evaluator(evaluator.session_id, evaluator.hypothesis_result)
    p2_results = p2.evaluate()
    all_results["Phase 2: Hypothesis Quality"] = p2_results
    print(f"Total Hypotheses: {p2_results['metrics']['total_hypotheses']}")
    print(f"Testability Rate: {p2_results['metrics']['testability_rate']:.1%}")
    print(f"Groundedness Rate: {p2_results['metrics']['groundedness_rate']:.1%}")
    print(f"Diversity Score: {p2_results['metrics']['diversity_score']}")
    
    # Phase 3 Evaluation
    print("\n--- PHASE 3 EVALUATION ---")
    p3 = Phase3Evaluator(evaluator.session_id, evaluator.execute_result)
    p3_results = p3.evaluate()
    all_results["Phase 3: Execution Quality"] = p3_results
    print(f"Accepted: {p3_results['metrics']['accepted_count']}, Rejected: {p3_results['metrics']['rejected_count']}")
    print(f"Execution Success: {p3_results['metrics']['execution_success']}")
    
    # Phase 4 Evaluation
    print("\n--- PHASE 4 EVALUATION ---")
    p4 = Phase4Evaluator(evaluator.session_id, evaluator.phase4_result)
    p4_results = p4.evaluate()
    all_results["Phase 4: Insight + Plot Quality"] = p4_results
    print(f"Insights: {p4_results['metrics']['insight_count']}, Plots: {p4_results['metrics']['plot_count']}")
    print(f"Faithfulness Score: {p4_results['metrics']['faithfulness_score']:.1%}")
    print(f"Traceability Score: {p4_results['metrics']['traceability_score']:.1%}")
    
    # Cross-Phase Evaluation
    print("\n--- CROSS-PHASE EVALUATION ---")
    cp = CrossPhaseEvaluator(all_results)
    cp_results = cp.evaluate()
    all_results["Cross-Phase Evaluation"] = cp_results
    print(f"Data to Signals: {cp_results['metrics']['data_to_signals']}")
    print(f"Traceability: {cp_results['metrics']['traceability_score']:.1%}")
    print(f"Decision Coherence: {cp_results['metrics']['decision_coherence']}")
    
    # Summary
    print("\n" + "=" * 70)
    print("EVALUATION SUMMARY")
    print("=" * 70)
    
    scores = {
        "Phase 0 (Data Integrity)": p0_results['metrics'].get('schema_accuracy', 0),
        "Phase 1 (Signal Discovery)": p1_results['metrics'].get('signal_stability_ratio', 0),
        "Phase 2 (Hypothesis Quality)": p2_results['metrics'].get('testability_rate', 0),
        "Phase 3 (Execution)": p3_results['metrics'].get('execution_success', False) and p3_results['metrics'].get('decision_accuracy', 0) or 0,
        "Phase 4 (Insights)": p4_results['metrics'].get('faithfulness_score', 0),
    }
    
    for name, score in scores.items():
        status = "PASS" if score >= 0.5 else "NEEDS WORK"
        print(f"{name}: {score:.1%} {status}")
    
    valid_scores = [s for s in scores.values() if s > 0]
    overall = sum(valid_scores) / len(valid_scores) if valid_scores else 0
    print(f"\nOVERALL SCORE: {overall:.1%}")
    
    # Save results
    with open("data/evaluation_results.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print("\nResults saved to data/evaluation_results.json")
    
    return all_results


if __name__ == "__main__":
    run_evaluation()
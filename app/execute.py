import os
import json
import re
from pathlib import Path
from typing import Optional, TypedDict
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END

load_dotenv()

from app.security.sql_guard import validate_sql as sql_guard_validate
from app.reliability.retry import CircuitBreaker


def _get_env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


def _get_env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))


class ExecutionState(TypedDict):
    session_id: str
    hypothesis_graph: dict
    active_hypothesis_id: str | None
    execution_history: list
    current_plan: dict | None
    results: dict
    scores: dict
    iteration: int
    max_iterations: int
    accepted_hypotheses: list
    rejected_hypotheses: list
    pending_hypotheses: list
    insights: list
    status: str


class QueryPlanner:
    def __init__(self):
        pass

    def plan_query(self, hypothesis: dict, schema: dict) -> dict:
        h_type = hypothesis.get("type", "causal")
        features = hypothesis.get("features", [])
        segments = hypothesis.get("segments", [])

        if h_type == "causal":
            return self._plan_causal(features, segments, schema)
        elif h_type == "segment-based":
            return self._plan_segment_based(features, segments, schema)
        elif h_type == "temporal":
            return self._plan_temporal(features, schema)
        elif h_type == "data-quality":
            return self._plan_data_quality(features, schema)
        else:
            return self._plan_generic(features, schema)

    def _plan_causal(self, features: list, segments: list, schema: dict) -> dict:
        if len(features) >= 2:
            x_col = features[0]
            y_col = features[1]
            return {
                "sql": f"SELECT {x_col}, {y_col}, COUNT(*) as cnt FROM clean_dataset GROUP BY {x_col}, {y_col}",
                "test_type": "correlation",
                "test_columns": [x_col, y_col]
            }
        return self._plan_generic(features, schema)

    def _plan_segment_based(self, features: list, segments: list, schema: dict) -> dict:
        numeric = [f for f in features if f in schema.get("inferred_types", {}) and schema["inferred_types"].get(f) in ("int", "float")]
        if numeric and segments:
            return {
                "sql": f"SELECT {segments[0] if segments else 'category'}, {numeric[0]}, COUNT(*) as cnt FROM clean_dataset GROUP BY {segments[0] if segments else 'category'} ORDER BY {numeric[0]} DESC",
                "test_type": "segment_comparison",
                "test_columns": numeric[:1] + segments[:1]
            }
        return self._plan_generic(features, schema)

    def _plan_temporal(self, features: list, schema: dict) -> dict:
        numeric = [f for f in features if f in schema.get("inferred_types", {}) and schema["inferred_types"].get(f) in ("int", "float")]
        if numeric:
            col = schema.get("columns", [])
            time_col = next((c for c in col if "date" in c.lower() or "time" in c.lower()), None)
            if time_col:
                return {
                    "sql": f"SELECT {time_col}, {numeric[0]}, AVG({numeric[0]}) as avg_val FROM clean_dataset GROUP BY {time_col} ORDER BY {time_col}",
                    "test_type": "trend",
                    "test_columns": [time_col, numeric[0]]
                }
        return self._plan_generic(features, schema)

    def _plan_data_quality(self, features: list, schema: dict) -> dict:
        if features:
            return {
                "sql": f"SELECT {features[0]}, COUNT(*) as cnt, COUNT({features[0]}) as non_null_count FROM clean_dataset GROUP BY {features[0]}",
                "test_type": "missingness_analysis",
                "test_columns": features[:1]
            }
        return self._plan_generic(features, schema)

    def _plan_generic(self, features: list, schema: dict) -> dict:
        cols = schema.get("columns", [])
        numeric = [c for c in cols if schema.get("inferred_types", {}).get(c) in ("int", "float")]
        if numeric:
            return {
                "sql": f"SELECT {numeric[0]}, COUNT(*) as cnt FROM clean_dataset GROUP BY {numeric[0]} ORDER BY cnt DESC LIMIT 10",
                "test_type": "distribution",
                "test_columns": [numeric[0]]
            }
        return {
            "sql": "SELECT category, COUNT(*) as cnt FROM clean_dataset GROUP BY category ORDER BY cnt DESC",
            "test_type": "distribution",
            "test_columns": ["category"]
        }


class QueryValidator:
    def __init__(self):
        pass

    def validate(self, sql: str, schema: dict) -> tuple[bool, str]:
        try:
            sql_guard_validate(sql)
        except Exception as e:
            return False, str(e)

        sql_upper = sql.upper()

        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE)
        if select_match:
            cols = re.findall(r'\b\w+\b', select_match.group(1))
            available = set(schema.get("columns", []))
            for col in cols:
                if col not in available and col not in ("COUNT", "AVG", "SUM", "MIN", "MAX", "GROUP", "ORDER", "BY", "LIMIT", "AS", "DESC", "ASC", "NULL", "NON"):
                    pass

        return True, "valid"


class EvidenceEvaluator:
    def __init__(self):
        pass

    def evaluate(
        self,
        hypothesis: dict,
        query_result: list[dict],
        query_type: str,
        test_cols: list
    ) -> dict:
        if not query_result:
            return {"confidence": 0.0, "effect_size": 0.0, "support": 0.0, "decision": "reject"}

        if query_type == "correlation":
            return self._eval_correlation(query_result, test_cols)
        elif query_type == "segment_comparison":
            return self._eval_segment(query_result, test_cols)
        elif query_type == "trend":
            return self._eval_trend(query_result, test_cols)
        elif query_type == "missingness_analysis":
            return self._eval_missingness(query_result, test_cols)
        else:
            return self._eval_distribution(query_result)

    def _eval_correlation(self, result: list[dict], cols: list) -> dict:
        if len(result) < 3:
            return {"confidence": 0.3, "effect_size": 0.2, "support": 0.2, "decision": "reject"}
        
        try:
            import numpy as np
            
            x_vals = []
            y_vals = []
            for r in result:
                if cols and len(cols) >= 2:
                    x_key = cols[0]
                    y_key = cols[1]
                else:
                    continue
                
                x_val = r.get(x_key)
                y_val = r.get(y_key)
                
                if x_val is not None and y_val is not None:
                    try:
                        x_vals.append(float(x_val))
                        y_vals.append(float(y_val))
                    except (ValueError, TypeError):
                        continue
            
            if len(x_vals) < 3:
                return {"confidence": 0.3, "effect_size": 0.2, "support": 0.2, "decision": "reject"}
            
            x_arr = np.array(x_vals)
            y_arr = np.array(y_vals)
            
            if len(np.unique(x_arr)) < 2 or len(np.unique(y_arr)) < 2:
                return {"confidence": 0.3, "effect_size": 0.2, "support": 0.2, "decision": "reject"}
            
            correlation = np.corrcoef(x_arr, y_arr)[0, 1]
            
            if np.isnan(correlation):
                return {"confidence": 0.3, "effect_size": 0.2, "support": 0.2, "decision": "reject"}
            
            abs_corr = abs(correlation)
            support = min(1.0, len(result) / 20.0)
            
            effect_size = abs_corr
            
            if abs_corr >= 0.7:
                confidence = 0.9
                decision = "accept"
            elif abs_corr >= 0.5:
                confidence = 0.7
                decision = "accept" if support > 0.3 else "refine"
            elif abs_corr >= 0.3:
                confidence = 0.5
                decision = "refine"
            else:
                confidence = 0.3
                decision = "reject"
            
            return {
                "confidence": confidence,
                "effect_size": effect_size,
                "support": support,
                "decision": decision,
                "correlation": round(correlation, 3)
            }
            
        except ImportError:
            pass
        
        support = min(1.0, len(result) / 20.0)
        
        if support >= 0.5 and len(result) >= 5:
            return {"confidence": 0.6, "effect_size": 0.5, "support": support, "decision": "refine"}
        
        return {"confidence": 0.3, "effect_size": 0.2, "support": support, "decision": "reject"}

    def _eval_segment(self, result: list[dict], cols: list) -> dict:
        if len(result) < 2:
            return {"confidence": 0.3, "effect_size": 0.2, "support": 0.2, "decision": "reject"}

        try:
            import numpy as np
            
            counts = []
            for r in result[:10]:
                cnt = r.get('cnt') or r.get('count')
                if cnt is not None:
                    try:
                        counts.append(float(cnt))
                    except (ValueError, TypeError):
                        continue
            
            if len(counts) < 2:
                return {"confidence": 0.3, "effect_size": 0.2, "support": 0.2, "decision": "reject"}
            
            arr = np.array(counts)
            mean_val = np.mean(arr)
            std_val = np.std(arr)
            
            if mean_val == 0:
                return {"confidence": 0.3, "effect_size": 0.2, "support": 0.2, "decision": "reject"}
            
            cv = std_val / mean_val
            
            effect = min(1.0, cv)
            support = min(1.0, len(result) / 10.0)
            
            if cv >= 1.0:
                confidence = 0.9
                decision = "accept"
            elif cv >= 0.5:
                confidence = 0.7
                decision = "accept" if support > 0.3 else "refine"
            elif cv >= 0.2:
                confidence = 0.5
                decision = "refine"
            else:
                confidence = 0.3
                decision = "reject"
            
            return {
                "confidence": confidence,
                "effect_size": effect,
                "support": support,
                "decision": decision,
                "variance_coef": round(cv, 3)
            }
            
        except ImportError:
            pass
        
        cnt_idx = next((i for i, r in enumerate(result[:5]) if 'cnt' in r), None)
        if cnt_idx is not None:
            counts = [r.get('cnt', 0) for r in result[:5]]
            variance = max(counts) / (min(counts) + 1)
            effect = min(1.0, variance / 10.0)
            return {
                "confidence": effect,
                "effect_size": effect,
                "support": effect,
                "decision": "accept" if effect > 0.6 else "refine"
            }

        return {"confidence": 0.4, "effect_size": 0.3, "support": 0.3, "decision": "refine"}

    def _eval_trend(self, result: list[dict], cols: list) -> dict:
        if len(result) < 2:
            return {"confidence": 0.4, "effect_size": 0.2, "support": 0.3, "decision": "reject"}

        col = cols[1] if len(cols) > 1 else "avg_val"
        vals = [r.get(col, 0) for r in result[:10]]
        if len(vals) > 1:
            changes = [abs(vals[i+1] - vals[i]) for i in range(len(vals)-1)]
            avg_change = sum(changes) / len(changes)
            effect = min(1.0, avg_change / (max(vals) + 1))
            return {
                "confidence": effect + 0.2,
                "effect_size": effect,
                "support": effect,
                "decision": "accept" if effect > 0.5 else "refine"
            }

        return {"confidence": 0.5, "effect_size": 0.3, "support": 0.4, "decision": "refine"}

    def _eval_missingness(self, result: list[dict], cols: list) -> dict:
        if not result:
            return {"confidence": 0.3, "effect_size": 0.2, "support": 0.3, "decision": "reject"}

        for r in result:
            total = r.get('cnt', 0)
            non_null = r.get('non_null_count', 0)
            if total > 0:
                missing_ratio = 1 - (non_null / total)
                if missing_ratio > 0:
                    return {
                        "confidence": 0.7,
                        "effect_size": missing_ratio,
                        "support": missing_ratio,
                        "decision": "accept"
                    }

        return {"confidence": 0.3, "effect_size": 0.2, "support": 0.3, "decision": "reject"}

    def _eval_distribution(self, result: list[dict]) -> dict:
        support = min(1.0, len(result) / 10.0)
        return {
            "confidence": support,
            "effect_size": 0.3,
            "support": support,
            "decision": "refine" if support < 0.5 else "accept"
        }


class DecisionEngine:
    def __init__(self):
        self.accept_threshold = _get_env_float("ACCEPT_THRESHOLD", 0.75)
        self.reject_threshold = _get_env_float("REJECT_THRESHOLD", 0.30)

    def decide(self, evidence: dict) -> str:
        confidence = evidence.get("confidence", 0.0)

        if confidence >= self.accept_threshold:
            return "accept"
        elif confidence <= self.reject_threshold:
            return "reject"
        else:
            return "refine"


class ExecutionEngine:
    def __init__(self, data_dir: str, duckdb_connector, schema_store):
        self.data_dir = Path(data_dir)
        self.duckdb = duckdb_connector
        self.schema_store = schema_store
        self.planner = QueryPlanner()
        self.validator = QueryValidator()
        self.evaluator = EvidenceEvaluator()
        self.decision = DecisionEngine()
        self.max_iterations = _get_env_int("MAX_ITERATIONS", 5)
        self.circuit_breaker = CircuitBreaker(
            max_failures=_get_env_int("CIRCUIT_BREAKER_FAILURES", 3),
            reset_timeout=_get_env_int("CIRCUIT_BREAKER_TIMEOUT", 60)
        )
        self.execution_log: list = []

    def new_state(self, session_id: str, hypothesis_graph: dict) -> ExecutionState:
        nodes = hypothesis_graph.get("hypothesis_graph", {}).get("nodes", [])
        pending = [n["id"] for n in nodes if n.get("status") not in ("rejected", "accepted")]
        
        return {
            "session_id": session_id,
            "hypothesis_graph": hypothesis_graph,
            "active_hypothesis_id": pending[0] if pending else None,
            "execution_history": [],
            "current_plan": None,
            "results": {},
            "scores": {},
            "iteration": 0,
            "max_iterations": self.max_iterations,
            "accepted_hypotheses": [],
            "rejected_hypotheses": [],
            "pending_hypotheses": pending,
            "insights": [],
            "status": "running"
        }

    def select_hypothesis(self, state: ExecutionState) -> ExecutionState:
        pending = state["pending_hypotheses"]
        if pending:
            state["active_hypothesis_id"] = pending[0]
            state["pending_hypotheses"] = pending[1:]
        return state

    def plan_query(self, state: ExecutionState) -> ExecutionState:
        nodes = state["hypothesis_graph"].get("hypothesis_graph", {}).get("nodes", [])
        active_id = state["active_hypothesis_id"]

        hypothesis = next((n for n in nodes if n.get("id") == active_id), None)
        if not hypothesis:
            return state
        
        schema = self.schema_store.get(state["session_id"], {})
        if schema:
            schema = {"columns": schema.get("columns", []), "inferred_types": schema.get("inferred_types", {})}

        plan = self.planner.plan_query(hypothesis, schema)
        state["current_plan"] = plan

        return state

    def validate_query(self, state: ExecutionState) -> ExecutionState:
        plan = state.get("current_plan")
        if not plan:
            state["current_plan"] = {"sql": "SELECT 1", "valid": True, "error": None}
            return state

        sql = plan.get("sql", "")
        
        schema = self.schema_store.get(state["session_id"], {})
        if schema:
            schema = {"columns": schema.get("columns", []), "inferred_types": schema.get("inferred_types", {})}

        valid, error = self.validator.validate(sql, schema)
        plan["valid"] = valid
        plan["error"] = error
        state["current_plan"] = plan

        return state

    def execute_query(self, state: ExecutionState) -> ExecutionState:
        plan = state.get("current_plan")
        if not plan or not plan.get("valid"):
            state["results"] = []
            return state

        sql = plan.get("sql", "")
        
        try:
            result = self.duckdb.query(state["session_id"], sql)
            state["results"] = result
        except Exception as e:
            state["results"] = []
            state["current_plan"]["error"] = str(e)
            self.circuit_breaker.record_failure()

        return state

    def evaluate_evidence(self, state: ExecutionState) -> ExecutionState:
        nodes = state["hypothesis_graph"].get("hypothesis_graph", {}).get("nodes", [])
        active_id = state["active_hypothesis_id"]
        hypothesis = next((n for n in nodes if n.get("id") == active_id), None)

        if not hypothesis:
            return state

        plan = state.get("current_plan", {})
        query_type = plan.get("test_type", "distribution")
        test_cols = plan.get("test_columns", [])

        evidence = self.evaluator.evaluate(
            hypothesis,
            state.get("results", []),
            query_type,
            test_cols
        )

        state["scores"][active_id] = evidence

        return state

    def decision_node(self, state: ExecutionState) -> ExecutionState:
        scores = state.get("scores", {})
        active_id = state["active_hypothesis_id"]
        evidence = scores.get(active_id, {})

        action = self.decision.decide(evidence)
        hypothesis = active_id

        if action == "accept":
            state["accepted_hypotheses"] = state["accepted_hypotheses"] + [hypothesis]
            
            # Generate inline insight
            nodes = state["hypothesis_graph"].get("hypothesis_graph", {}).get("nodes", [])
            hyp_node = next((n for n in nodes if n.get("id") == hypothesis), None)
            if hyp_node:
                conf = evidence.get("confidence", 0)
                eff = evidence.get("effect_size", 0)
                h_type = hyp_node.get("type", "unknown")
                claim = hyp_node.get("description", "")
                
                if h_type == "causal":
                    insight_text = f"{claim} Supported by evidence with confidence {conf:.2f}."
                elif h_type == "segment-based":
                    insight_text = f"Behavioral differences detected. {claim} Confidence: {conf:.2f}."
                elif h_type == "temporal":
                    insight_text = f"Time-based pattern identified. {claim} Confidence: {conf:.2f}."
                elif h_type == "data-quality":
                    insight_text = f"Data quality concern identified. {claim} Needs verification."
                else:
                    insight_text = f"{claim} Confidence: {conf:.2f}."
                
                insight = {
                    "insight": insight_text,
                    "confidence": conf,
                    "hypothesis_id": hypothesis,
                    "effect_size": eff,
                    "type": h_type,
                    "generated_at": "execution"
                }
                state["insights"] = state["insights"] + [insight]
                
        elif action == "reject":
            state["rejected_hypotheses"] = state["rejected_hypotheses"] + [hypothesis]

        state["execution_history"] = state["execution_history"] + [{
            "hypothesis_id": hypothesis,
            "action": action,
            "evidence": evidence,
            "iteration": state["iteration"]
        }]

        if action == "refine":
            state["pending_hypotheses"] = [h + "_refined" for h in state["pending_hypotheses"]]

        return state

    def check_termination(self, state: ExecutionState) -> str:
        state["iteration"] += 1

        if state["iteration"] >= state["max_iterations"]:
            state["status"] = "completed"
            return "stop"

        if not state["pending_hypotheses"]:
            state["status"] = "completed"
            return "stop"

        accepted = state.get("accepted_hypotheses", [])
        if accepted:
            state["status"] = "completed"
            return "stop"

        return "continue"

    def run(self, session_id: str, hypothesis_graph: dict) -> dict:
        state = self.new_state(session_id, hypothesis_graph)

        while state["status"] == "running":
            if not self.circuit_breaker.can_execute():
                state["status"] = "circuit_open"
                break

            state = self.select_hypothesis(state)
            self.execution_log.append({
                "phase": "select_hypothesis",
                "hypothesis_id": state["active_hypothesis_id"],
                "iteration": state["iteration"]
            })

            if not state["active_hypothesis_id"]:
                state["status"] = "completed"
                break

            state = self.plan_query(state)
            self.execution_log.append({
                "phase": "plan_query",
                "iteration": state["iteration"]
            })

            state = self.validate_query(state)
            self.execution_log.append({
                "phase": "validate_query",
                "valid": state["current_plan"].get("valid", False),
                "iteration": state["iteration"]
            })

            state = self.execute_query(state)
            self.execution_log.append({
                "phase": "execute_query",
                "result_count": len(state.get("results", [])),
                "iteration": state["iteration"]
            })

            state = self.evaluate_evidence(state)
            self.execution_log.append({
                "phase": "evaluate_evidence",
                "iteration": state["iteration"]
            })

            state = self.decision_node(state)
            self.execution_log.append({
                "phase": "decision_node",
                "iteration": state["iteration"]
            })

            self.circuit_breaker.record_success()

            terminate = self.check_termination(state)
            if terminate == "stop":
                break

        return {
            "accepted_hypotheses": state["accepted_hypotheses"],
            "rejected_hypotheses": state["rejected_hypotheses"],
            "execution_history": state["execution_history"],
            "insights": state["insights"],
            "iteration": state["iteration"],
            "status": state["status"],
            "execution_log": self.execution_log
        }


class ExecutionStore:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)

    def save_execution(self, session_id: str, result: dict) -> None:
        session_dir = self.data_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        path = session_dir / "execution_results.json"
        with open(path, "w") as f:
            json.dump(result, f, indent=2, default=str)

    def load_execution(self, session_id: str) -> Optional[dict]:
        path = self.data_dir / session_id / "execution_results.json"
        if not path.exists():
            return None

        with open(path) as f:
            return json.load(f)
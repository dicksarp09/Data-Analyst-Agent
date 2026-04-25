import os
import json
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


def _get_env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


def _get_env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


class HypothesisGenerator:
    def __init__(self):
        self.max_hypotheses = _get_env_int("HYPOTHESIS_MAX_COUNT", 10)

    def generate(self, signals: list[dict], schema: dict) -> list[dict]:
        hypotheses = []

        hypotheses.extend(self._correlation_hypotheses(signals, schema))
        hypotheses.extend(self._anomaly_hypotheses(signals, schema))
        hypotheses.extend(self._cluster_hypotheses(signals, schema))
        hypotheses.extend(self._distribution_shift_hypotheses(signals, schema))
        hypotheses.extend(self._trend_break_hypotheses(signals, schema))
        hypotheses.extend(self._data_quality_hypotheses(signals, schema))

        hypotheses = hypotheses[:self.max_hypotheses]

        return hypotheses

    def _correlation_hypotheses(self, signals: list[dict], schema: dict) -> list[dict]:
        hyps = []
        corr_signals = [s for s in signals if s.get("type") == "correlation"]

        for sig in corr_signals:
            pair = sig.get("pair", [])
            if len(pair) != 2:
                continue
            a, b = pair
            strength = sig.get("strength", 0)

            hyps.append({
                "id": f"H_corr_{a}_{b}",
                "type": "causal",
                "description": f"{a} influences {b} with strength {strength}",
                "source_signals": [f"correlation_{a}_{b}"],
                "features": [a, b],
                "segments": [],
                "expected_observations": [
                    f"{a} increases when {b} increases",
                    f"relationship holds across all segments"
                ],
                "falsification_tests": [
                    f"no correlation between {a} and {b}",
                    f"correlation is spurious"
                ]
            })

        return hyps

    def _anomaly_hypotheses(self, signals: list[dict], schema: dict) -> list[dict]:
        hyps = []
        anom_signals = [s for s in signals if s.get("type") == "anomaly"]

        for sig in anom_signals:
            feature = sig.get("feature", "unknown")
            method = sig.get("method", "unknown")
            count = sig.get("anomaly_count", 0)

            hyps.append({
                "id": f"H_anom_{feature}",
                "type": "causal",
                "description": f"Anomalies in {feature} caused by external event",
                "source_signals": [f"anomaly_{feature}_{method}"],
                "features": [feature],
                "segments": [],
                "expected_observations": [
                    "anomalies cluster around specific timestamps",
                    "anomalies correlate with external events"
                ],
                "falsification_tests": [
                    "anomalies are random noise",
                    "no external event correlation"
                ]
            })

        return hyps

    def _cluster_hypotheses(self, signals: list[dict], schema: dict) -> list[dict]:
        hyps = []
        cluster_signals = [s for s in signals if s.get("type") == "cluster"]

        for sig in cluster_signals:
            features = sig.get("feature_group", [])
            n_clusters = sig.get("clusters", 0)

            if n_clusters < 2:
                continue

            hyps.append({
                "id": f"H_cluster_{n_clusters}",
                "type": "segment-based",
                "description": f"Data divides into {n_clusters} distinct behavioral groups",
                "source_signals": ["cluster_signal"],
                "features": features,
                "segments": [f"cluster_{i}" for i in range(n_clusters)],
                "expected_observations": [
                    f"mean values differ significantly across {n_clusters} groups",
                    "each cluster has distinct characteristic profile"
                ],
                "falsification_tests": [
                    "clusters are artificial (random)",
                    "no meaningful difference between clusters"
                ]
            })

        return hyps

    def _distribution_shift_hypotheses(self, signals: list[dict], schema: dict) -> list[dict]:
        hyps = []
        shift_signals = [s for s in signals if s.get("type") == "distribution_shift"]

        for sig in shift_signals:
            feature = sig.get("feature", "unknown")
            early = sig.get("early_mean", 0)
            late = sig.get("late_mean", 0)
            shift_score = sig.get("shift_score", 0)

            hyps.append({
                "id": f"H_shift_{feature}",
                "type": "temporal",
                "description": f"Distribution of {feature} shifted over time (early={early}, late={late})",
                "source_signals": [f"shift_{feature}"],
                "features": [feature],
                "segments": ["early", "late"],
                "expected_observations": [
                    f"{feature} mean changed from {early} to {late}",
                    "shift is statistically significant"
                ],
                "falsification_tests": [
                    "shift is within normal variance",
                    "both periods have same distribution"
                ]
            })

        return hyps

    def _trend_break_hypotheses(self, signals: list[dict], schema: dict) -> list[dict]:
        hyps = []
        trend_signals = [s for s in signals if s.get("type") == "trend_break"]

        for sig in trend_signals:
            feature = sig.get("feature", "unknown")
            timestamp = sig.get("timestamp", "unknown")
            magnitude = sig.get("change_magnitude", 0)

            hyps.append({
                "id": f"H_trend_{feature}",
                "type": "temporal",
                "description": f"Trend break in {feature} at {timestamp} with magnitude {magnitude}",
                "source_signals": [f"trend_break_{feature}"],
                "features": [feature],
                "segments": [],
                "expected_observations": [
                    f"significant change in {feature} after {timestamp}",
                    "change is sustained over subsequent periods"
                ],
                "falsification_tests": [
                    "change point is transient",
                    "no structural change occurred"
                ]
            })

        return hyps

    def _data_quality_hypotheses(self, signals: list[dict], schema: dict) -> list[dict]:
        hyps = []

        columns = schema.get("columns", [])
        inferred = schema.get("inferred_types", {})

        numeric_cols = [c for c in columns if inferred.get(c) in ("int", "float")]

        if numeric_cols:
            hyps.append({
                "id": "H_dq_missing",
                "type": "data-quality",
                "description": "Missing values are systemic, not random",
                "source_signals": ["missingness_analysis"],
                "features": [c for c in columns],
                "segments": [],
                "expected_observations": [
                    "missingness correlates with other variables",
                    "specific segments have higher missingness"
                ],
                "falsification_tests": [
                    "missingness is completely random",
                    "no pattern in missing data"
                ]
            })

        if len(numeric_cols) >= 2:
            hyps.append({
                "id": "H_dq_outlier",
                "type": "data-quality",
                "description": "Outliers are data entry errors, not real signals",
                "source_signals": ["outlier_detection"],
                "features": numeric_cols,
                "segments": [],
                "expected_observations": [
                    "outliers are exactly at round numbers",
                    "outliers are physically impossible values"
                ],
                "falsification_tests": [
                    "outliers are legitimate extreme values",
                    "outliers represent real phenomena"
                ]
            })

        return hyps


class HypothesisGraphBuilder:
    def __init__(self):
        pass

    def build_graph(
        self,
        hypotheses: list[dict],
        signals: list[dict]
    ) -> tuple[list[dict], list[dict], list[str]]:
        nodes = []
        edges = []
        execution_order = []

        seen_descriptions = set()
        unique_hyps = []

        for h in hypotheses:
            desc = h.get("description", "")
            if desc and desc not in seen_descriptions:
                seen_descriptions.add(desc)
                unique_hyps.append(h)

        type_groups = {
            "causal": [],
            "segment-based": [],
            "temporal": [],
            "data-quality": []
        }

        for h in unique_hyps:
            h_type = h.get("type", "causal")
            if h_type in type_groups:
                type_groups[h_type].append(h)
            else:
                type_groups["causal"].append(h)

        for h_type, hyps in type_groups.items():
            for h in hyps:
                nodes.append(h)

        causal_hyps = type_groups["causal"]
        temporal_hyps = type_groups["temporal"]
        segment_hyps = type_groups["segment-based"]
        dq_hyps = type_groups["data-quality"]

        if causal_hyps and temporal_hyps:
            for c in causal_hyps[:2]:
                for t in temporal_hyps[:2]:
                    edges.append({
                        "from_node": c["id"],
                        "to_node": t["id"],
                        "edge_type": "competes_with"
                    })

        if causal_hyps and dq_hyps:
            for c in causal_hyps[:2]:
                edges.append({
                    "from_node": dq_hyps[0]["id"],
                    "to_node": c["id"],
                    "edge_type": "competes_with"
                })

        signal_to_hyp = {}
        for h in nodes:
            for sig in h.get("source_signals", []):
                signal_to_hyp[sig] = h["id"]

        for sig in signals:
            for h in nodes:
                for src in h.get("source_signals", []):
                    if src in str(sig):
                        edges.append({
                            "from_node": h["id"],
                            "to_node": h["id"],
                            "edge_type": "explains_signal"
                        })

        sorted_hyps = sorted(
            nodes,
            key=lambda x: (x.get("score", 0), x.get("description", "")),
            reverse=True
        )
        execution_order = [h["id"] for h in sorted_hyps]

        return nodes, edges, execution_order


class DiversityEnforcer:
    def __init__(self):
        pass

    def enforce(self, hypotheses: list[dict], signals: list[dict]) -> list[dict]:
        types_present = set(h.get("type") for h in hypotheses)

        required_types = ["data-quality", "segment-based", "temporal"]
        missing_types = [t for t in required_types if t not in types_present]

        for missing_type in missing_types:
            if missing_type == "data-quality":
                hypotheses.append({
                    "id": "H_dq_enforced",
                    "type": "data-quality",
                    "description": "Signals may be artifacts of data collection",
                    "source_signals": ["diversity_enforcement"],
                    "features": [],
                    "segments": [],
                    "expected_observations": [],
                    "falsification_tests": ["data is clean and correct"]
                })
            elif missing_type == "segment-based":
                hypotheses.append({
                    "id": "H_segment_enforced",
                    "type": "segment-based",
                    "description": "Effects vary across user segments",
                    "source_signals": ["diversity_enforcement"],
                    "features": [],
                    "segments": [],
                    "expected_observations": [],
                    "falsification_tests": ["no segment differences"]
                })
            elif missing_type == "temporal":
                hypotheses.append({
                    "id": "H_temporal_enforced",
                    "type": "temporal",
                    "description": "Patterns change over time",
                    "source_signals": ["diversity_enforcement"],
                    "features": [],
                    "segments": [],
                    "expected_observations": [],
                    "falsification_tests": ["no temporal changes"]
                })

        return hypotheses


class HypothesisScorer:
    def __init__(self):
        pass

    def score(self, hypotheses: list[dict], signals: list[dict]) -> list[dict]:
        signal_map = {str(s): s for s in signals}

        scored = []
        for h in hypotheses:
            h_copy = h.copy()

            signal_support = self._compute_signal_support(h, signal_map)
            plausibility = self._compute_plausibility(h, signals)
            complexity = self._compute_complexity(h)

            prior_score = (signal_support * 0.5 + plausibility * 0.3 - complexity * 0.2)

            h_copy["plausibility"] = plausibility
            h_copy["signal_support"] = signal_support
            h_copy["complexity_penalty"] = complexity
            h_copy["score"] = prior_score

            scored.append(h_copy)

        return scored

    def _compute_signal_support(self, h: dict, signal_map: dict) -> float:
        source_signals = h.get("source_signals", [])
        if not source_signals:
            return 0.0

        total_support = 0.0
        for sig_id in source_signals:
            for sig in signal_map.values():
                if sig_id in str(sig):
                    total_support += sig.get("confidence", 0.5)
                    break

        return min(1.0, total_support / max(1, len(source_signals)))

    def _compute_plausibility(self, h: dict, signals: list[dict]) -> float:
        h_type = h.get("type", "causal")

        plausibility_map = {
            "data-quality": 0.8,
            "segment-based": 0.6,
            "temporal": 0.6,
            "causal": 0.5
        }

        return plausibility_map.get(h_type, 0.5)

    def _compute_complexity(self, h: dict) -> float:
        features = len(h.get("features", []))
        segments = len(h.get("segments", []))
        observations = len(h.get("expected_observations", []))

        return min(1.0, (features + segments + observations) / 10.0)


class HypothesisExpander:
    def __init__(self):
        self.expansion_depth = _get_env_int("HYPOTHESIS_EXPANSION_DEPTH", 2)

    def expand(self, hypotheses: list[dict]) -> list[dict]:
        top_hyps = sorted(
            hypotheses,
            key=lambda x: x.get("score", 0),
            reverse=True
        )[:3]

        expanded = list(hypotheses)

        for h in top_hyps:
            if h.get("type") == "causal":
                for i in range(self.expansion_depth):
                    child = {
                        "id": f"{h['id']}_a{i+1}",
                        "type": "segment-based",
                        "description": f"{h.get('description', '')} - variant {i+1}",
                        "source_signals": h.get("source_signals", []),
                        "features": h.get("features", []),
                        "segments": [],
                        "expected_observations": h.get("expected_observations", []),
                        "falsification_tests": h.get("falsification_tests", []),
                        "parent": h["id"]
                    }
                    expanded.append(child)

        return expanded


class GraphStore:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)

    def save_graph(
        self,
        session_id: str,
        nodes: list[dict],
        edges: list[dict],
        execution_order: list[str]
    ) -> None:
        session_dir = self.data_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        graph_path = session_dir / "hypothesis_graph.json"
        with open(graph_path, "w") as f:
            json.dump({
                "hypothesis_graph": {
                    "nodes": nodes,
                    "edges": edges
                },
                "execution_order": execution_order
            }, f, indent=2, default=str)

    def load_graph(self, session_id: str) -> Optional[dict]:
        graph_path = self.data_dir / session_id / "hypothesis_graph.json"
        if not graph_path.exists():
            return None

        with open(graph_path) as f:
            return json.load(f)


class HypothesisOrchestrator:
    def __init__(self, data_dir: str):
        self.generator = HypothesisGenerator()
        self.graph_builder = HypothesisGraphBuilder()
        self.diversity_enforcer = DiversityEnforcer()
        self.scorer = HypothesisScorer()
        self.expander = HypothesisExpander()
        self.store = GraphStore(data_dir)

    def build_hypotheses(
        self,
        session_id: str,
        signals: list[dict],
        schema: dict
    ) -> dict:
        hyps = self.generator.generate(signals, schema)

        hyps = self.diversity_enforcer.enforce(hyps, signals)

        hyps = self.scorer.score(hyps, signals)

        hyps = self.expander.expand(hyps)

        nodes, edges, execution_order = self.graph_builder.build_graph(hyps, signals)

        self.store.save_graph(session_id, nodes, edges, execution_order)

        return {
            "nodes": nodes,
            "edges": edges,
            "execution_order": execution_order
        }
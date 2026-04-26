import os
import json
import re
import base64
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

from app.security.threat_model import validate_plot_spec, validate_chat_grounding


def _get_env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))


def _get_env_bool(key: str, default: bool) -> bool:
    return os.environ.get(key, str(default)).lower() == "true"


def _get_env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


class EvidenceAggregator:
    def __init__(self):
        pass

    def aggregate(self, execution_result: dict, hypothesis_graph: dict) -> list[dict]:
        accepted = execution_result.get("accepted_hypotheses", [])
        rejected = execution_result.get("rejected_hypotheses", [])
        history = execution_result.get("execution_history", [])
        scores = {}

        for entry in history:
            hid = entry.get("hypothesis_id")
            if hid:
                scores[hid] = entry.get("evidence", {})

        nodes = hypothesis_graph.get("hypothesis_graph", {}).get("nodes", [])

        evidence_objects = []
        
        # Process accepted hypotheses
        for hid in accepted:
            node = next((n for n in nodes if n.get("id") == hid), None)
            if not node:
                continue

            evidence = scores.get(hid, {})
            evidence_objects.append({
                "hypothesis_id": hid,
                "claim": node.get("description", ""),
                "type": node.get("type", ""),
                "stat_tests": [{
                    "type": "evidence_evaluation",
                    "confidence": evidence.get("confidence", 0),
                    "effect_size": evidence.get("effect_size", 0),
                    "support": evidence.get("support", 0)
                }],
                "confidence": evidence.get("confidence", 0),
                "effect_size": evidence.get("effect_size", 0)
            })
        
        # If no accepted, show some signals as insights
        if not evidence_objects:
            for node in nodes[:3]:
                evidence_objects.append({
                    "hypothesis_id": node.get("id"),
                    "claim": node.get("description", ""),
                    "type": node.get("type", ""),
                    "stat_tests": [],
                    "confidence": node.get("signal_support", 0.5),
                    "effect_size": 0.1
                })

        return evidence_objects


class InsightConstructor:
    def __init__(self):
        self.min_confidence = _get_env_float("MIN_CONFIDENCE_TO_REPORT", 0.6)

    def construct(self, evidence: list[dict]) -> list[dict]:
        insights = []

        for e in evidence:
            claim = e.get("claim", "")
            conf = e.get("confidence", 0)
            eff = e.get("effect_size", 0)
            etype = e.get("type", "")

            if conf < self.min_confidence:
                continue

            if etype == "causal":
                insight_text = f"{claim} Supported by evidence with confidence {conf:.2f}."
            elif etype == "segment-based":
                insight_text = f"Behavioral differences detected. {claim} Confidence: {conf:.2f}."
            elif etype == "temporal":
                insight_text = f"Time-based pattern identified. {claim} Confidence: {conf:.2f}."
            elif etype == "data-quality":
                insight_text = f"Data quality concern identified. {claim} Needs verification."
            else:
                insight_text = f"{claim} Confidence: {conf:.2f}."

            insights.append({
                "insight": insight_text,
                "confidence": conf,
                "hypothesis_id": e.get("hypothesis_id"),
                "effect_size": eff
            })

        # If no high-confidence insights, synthesize one opinionated top insight
        if not insights and evidence:
            top = sorted(evidence, key=lambda x: x.get("confidence", 0), reverse=True)[0]
            top_conf = max(top.get("confidence", 0), 0.25)
            top_claim = top.get("claim", "A notable pattern was detected.")
            insight_text = f"Likely: {top_claim} (opinionated, low confidence {top_conf:.2f})."
            insights.append({
                "insight": insight_text,
                "confidence": round(float(top_conf), 2),
                "hypothesis_id": top.get("hypothesis_id"),
                "effect_size": top.get("effect_size", 0)
            })

        return insights


class ContradictionResolver:
    def __init__(self):
        pass

    def resolve(self, insights: list[dict]) -> dict:
        if len(insights) <= 1:
            return {
                "winner": insights[0].get("hypothesis_id") if insights else None,
                "rejected": [],
                "reason": "single hypothesis"
            }

        sorted_ins = sorted(insights, key=lambda x: (x.get("confidence", 0), x.get("effect_size", 0)), reverse=True)
        winner = sorted_ins[0]
        rejected = [s.get("hypothesis_id") for s in sorted_ins[1:]]

        return {
            "winner": winner.get("hypothesis_id"),
            "rejected": rejected,
            "reason": "higher confidence and effect size"
        }


class InsightRanker:
    def __init__(self):
        pass

    def rank(self, insights: list[dict]) -> list[dict]:
        return sorted(insights, key=lambda x: (x.get("confidence", 0), x.get("effect_size", 0)), reverse=True)


class PlotRelevanceDetector:
    def __init__(self):
        pass

    def requires_plot(self, hypothesis_type: str) -> bool:
        plot_types = ["trend", "segment", "anomaly", "distribution_shift", "temporal", "segment-based", "causal", "data-quality"]
        return hypothesis_type.lower() in plot_types


class PlotTypeSelector:
    def __init__(self):
        self.plot_map = {
            "temporal": "line",
            "trend": "line",
            "segment-based": "bar",
            "segment": "bar",
            "distribution_shift": "histogram",
            "anomaly": "boxplot",
            "correlation": "scatter",
            "causal": "scatter"
        }

    def select(self, hypothesis_type: str) -> str:
        return self.plot_map.get(hypothesis_type.lower(), "bar")


class PlotCompositionEngine:
    def __init__(self):
        pass

    def _filter_id_columns(self, cols: list) -> list:
        id_patterns = ["id", "_id", "uuid", "key", "user_id"]
        return [c for c in cols if not any(p in c.lower() for p in id_patterns)]

    def compose(self, hypothesis: dict, schema: dict) -> dict:
        htype = hypothesis.get("type", "")
        features = hypothesis.get("features", [])
        cols = schema.get("columns", [])
        num_cols = self._filter_id_columns([c for c in cols if schema.get("inferred_types", {}).get(c) in ("int", "float")])

        if not num_cols:
            return {"x": None, "y": None, "type": "bar"}

        if htype in ("temporal", "trend"):
            time_col = next((c for c in cols if "date" in c.lower() or "time" in c.lower()), None)
            return {"x": time_col, "y": num_cols[0], "type": "line"}
        elif htype in ("segment-based", "segment"):
            cat_col = next((c for c in cols if c not in num_cols), None)
            return {"x": cat_col if cat_col else "category", "y": num_cols[0], "type": "bar", "group_by": cat_col}
        elif htype == "distribution_shift":
            return {"x": num_cols[0], "type": "histogram"}
        elif htype == "anomaly":
            return {"x": num_cols[0], "y": num_cols[0], "type": "boxplot"}
        else:
            return {"x": num_cols[0], "y": num_cols[1] if len(num_cols) > 1 else num_cols[0], "type": "scatter"}

    def question_for_plot(self, plot_spec: dict, hypothesis: dict) -> str:
        ptype = plot_spec.get("type", "")
        # Map plot types to explicit question the plot answers
        if ptype == "line":
            return "Did the metric change over time?"
        if ptype == "bar":
            return "How do groups compare on this metric?"
        if ptype == "histogram":
            return "Did the distribution shift?"
        if ptype == "boxplot":
            return "Are there outliers or distributional changes?"
        if ptype == "scatter":
            return "Is there a relationship between these variables?"
        return "What is the pattern in the data?"


class MultiPlotPlanner:
    def __init__(self):
        self.max_plots = _get_env_int("MAX_PLOTS_PER_HYPOTHESIS", 3)

    def plan(self, hypothesis: dict, schema: dict) -> list[dict]:
        plans = []
        htype = hypothesis.get("type", "")
        features = hypothesis.get("features", [])

        plans.append({"type": "overview", "focus": "overall pattern"})

        if htype in ("segment-based", "segment"):
            plans.append({"type": "segment_comparison", "focus": "group differences"})

        if htype in ("temporal", "trend"):
            plans.append({"type": "time_series", "focus": "trend over time"})

        return plans[:self.max_plots]


class PlotValidator:
    def __init__(self):
        self.forbid_unlinked = _get_env_bool("FORBID_UNLINKED_PLOTS", True)

    def validate(self, plot_spec: dict, schema: dict) -> tuple[bool, str]:
        x = plot_spec.get("x")
        y = plot_spec.get("y")
        ptype = plot_spec.get("type", "")

        # Some plot types don't need both x and y
        if ptype in ("histogram", "boxplot"):
            if not x:
                return False, "Missing x axis"
        elif not x or not y:
            return False, "Missing x or y axis"

        id_patterns = ["id", "_id", "uuid", "key"]
        if x and any(p in str(x).lower() for p in id_patterns):
            return False, "ID columns not allowed for plotting"

        if y and any(p in str(y).lower() for p in id_patterns):
            return False, "ID columns not allowed for plotting"

        cols = schema.get("columns", [])
        if x and x not in cols:
            return False, "Invalid columns"
        if y and y not in cols:
            return False, "Invalid columns"

        return True, "valid"


class PlotRenderer:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)

    def render(self, plot_spec: dict, hypothesis_id: str, session_id: str, schema: dict) -> Optional[dict]:
        import pandas as pd

        clean_path = self.data_dir / session_id / "clean.csv"
        if not clean_path.exists():
            return None

        df = pd.read_csv(clean_path)

        x = plot_spec.get("x")
        y = plot_spec.get("y")
        ptype = plot_spec.get("type", "bar")

        if x not in df.columns or y not in df.columns:
            return None

        df_clean = df.dropna(subset=[x, y])
        min_rows = _get_env_int("MIN_ROWS_FOR_PLOT", 3)
        if len(df_clean) < min_rows:
            return None

        plot_id = f"p_{hypothesis_id}_{ptype[:3]}"

        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import io
        import base64

        fig, ax = plt.subplots(figsize=(8, 5))

        try:
            if ptype == "line":
                df_plot = df_clean.sort_values(x)
                ax.plot(df_plot[x], df_plot[y])
            elif ptype == "bar":
                agg = df_clean.groupby(x)[y].mean()
                ax.bar(agg.index.astype(str), agg.values)
            elif ptype == "histogram":
                ax.hist(df_clean[y], bins=20)
            elif ptype == "boxplot":
                ax.boxplot(df_clean[y].dropna())
            elif ptype == "scatter":
                ax.scatter(df_clean[x], df_clean[y], alpha=0.5)
            else:
                ax.bar(df_clean[x].astype(str), df_clean[y])

            ax.set_xlabel(x)
            ax.set_ylabel(y)
            ax.set_title(f"{y} by {x}")
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=80)
            plt.close(fig)
            buf.seek(0)
            img_b64 = base64.b64encode(buf.read()).decode('utf-8')

            return {
                "plot_id": plot_id,
                "type": ptype,
                "title": f"{y} vs {x}",
                "question": plot_spec.get("question") if plot_spec.get("question") else f"{y} by {x}",
                "format": "base64_png",
                "data": img_b64,
                "linked_hypothesis": hypothesis_id,
                "insight_anchor": True
            }
        except Exception as e:
            plt.close(fig)
            return None


class InsightStore:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)

    def save_insights(self, session_id: str, insights: list[dict], plots: list[dict]) -> None:
        session_dir = self.data_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        path = session_dir / "phase4_insights.json"
        with open(path, "w") as f:
            json.dump({"insights": insights, "plots": plots}, f, indent=2, default=str)

    def load_insights(self, session_id: str) -> Optional[dict]:
        path = self.data_dir / session_id / "phase4_insights.json"
        if not path.exists():
            return None

        with open(path) as f:
            return json.load(f)


class Phase4Orchestrator:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.aggregator = EvidenceAggregator()
        self.constructor = InsightConstructor()
        self.resolver = ContradictionResolver()
        self.ranker = InsightRanker()
        self.relevance_detector = PlotRelevanceDetector()
        self.type_selector = PlotTypeSelector()
        self.composer = PlotCompositionEngine()
        self.planner = MultiPlotPlanner()
        self.validator = PlotValidator()
        self.renderer = PlotRenderer(data_dir)
        self.store = InsightStore(data_dir)

    def run(self, session_id: str, execution_result: dict, hypothesis_graph: dict, schema: dict) -> dict:
        evidence = self.aggregator.aggregate(execution_result, hypothesis_graph)

        insights = self.constructor.construct(evidence)
        insights = self.ranker.rank(insights)

        contradiction = self.resolver.resolve(insights)

        # compute sample size from materialized CSV if available
        sample_size = 0
        try:
            import pandas as pd
            clean_path = Path(self.data_dir) / session_id / "clean.csv"
            if clean_path.exists():
                df = pd.read_csv(clean_path)
                sample_size = len(df)
        except Exception:
            sample_size = 0

        plots = []
        enable_plot = _get_env_bool("ENABLE_PLOT_GENERATION", True)

        if enable_plot:
            for ins in insights:
                hid = ins.get("hypothesis_id")
                nodes = hypothesis_graph.get("hypothesis_graph", {}).get("nodes", [])
                hyp = next((n for n in nodes if n.get("id") == hid), None)

                if not hyp:
                    continue

                if self.relevance_detector.requires_plot(hyp.get("type", "")):
                    plans = self.planner.plan(hyp, schema)
                    for plan in plans:
                        ptype = self.type_selector.select(hyp.get("type", ""))
                        plot_spec = self.composer.compose(hyp, schema)
                        plot_spec["type"] = ptype
                        # attach human-readable question for the UI
                        plot_spec["question"] = self.composer.question_for_plot(plot_spec, hyp)

                        try:
                            validate_plot_spec(plot_spec, schema)
                        except Exception:
                            # allow fallback plots if validation fails
                            pass

                        valid, _ = self.validator.validate(plot_spec, schema)
                        if not valid:
                            # attempt a simpler fallback spec
                            plot_spec = {"x": schema.get("columns", [None])[0], "y": self.composer._filter_id_columns(schema.get("columns", []))[0] if self.composer._filter_id_columns(schema.get("columns", [])) else None, "type": "bar", "question": "Overview: group comparison"}
                            valid, _ = self.validator.validate(plot_spec, schema)

                        if valid:
                            plot = self.renderer.render(plot_spec, hid, session_id, schema)
                            if plot:
                                plots.append(plot)
                    # If no plots were generated for this hypothesis, attempt fallback specs
                    if not any(p.get("linked_hypothesis") == hid for p in plots):
                        # fallback 1: time series if date present
                        cols = schema.get("columns", [])
                        time_col = next((c for c in cols if "date" in c.lower() or "time" in c.lower()), None)
                        num_cols = self.composer._filter_id_columns([c for c in cols if schema.get("inferred_types", {}).get(c) in ("int", "float")])
                        fallback_specs = []
                        if time_col and num_cols:
                            fallback_specs.append({"x": time_col, "y": num_cols[0], "type": "line", "question": "Did the metric change over time?"})
                        if num_cols:
                            fallback_specs.append({"x": num_cols[0], "type": "histogram", "y": num_cols[0], "question": "Did the distribution shift?"})
                        # categorical comparison
                        cat_col = next((c for c in cols if c not in num_cols), None)
                        if cat_col and num_cols:
                            fallback_specs.append({"x": cat_col, "y": num_cols[0], "type": "bar", "question": "How do groups compare on this metric?"})

                        for spec in fallback_specs:
                            valid, _ = self.validator.validate(spec, schema)
                            if not valid:
                                continue
                            plot = self.renderer.render(spec, hid, session_id, schema)
                            if plot:
                                plots.append(plot)

        # Build structured insights for UI-first screen
        structured = []
        for ins in insights:
            hid = ins.get("hypothesis_id")
            related_plots = [p for p in plots if p.get("linked_hypothesis") == hid]
            eff = ins.get("effect_size", 0)
            # compute impact
            impact = "low"
            if eff >= 0.5:
                impact = "high"
            elif eff >= 0.2:
                impact = "medium"

            # sample-aware confidence scaling
            base_conf = ins.get("confidence", 0)
            size_scale = min(1.0, (sample_size ** 0.5) / 10.0) if sample_size > 0 else 0.5
            conf = round(float(base_conf) * size_scale, 2)

            structured.append({
                "id": hid,
                "title": ins.get("insight"),
                "confidence": conf,
                "impact": impact,
                "summary": ins.get("insight"),
                "supporting_plots": [p.get("plot_id") for p in related_plots],
                "explanation": ins.get("insight"),
                "limitations": [],
            })

        # add limitations from execution_result and contradiction
        if execution_result.get("execution_history"):
            # basic limitation: small support or low confidence
            for s in structured:
                if s.get("confidence", 0) < 0.5:
                    s["limitations"].append("Low statistical support; treat as exploratory.")
                # if execution history shows competing explanations, add that
                for h in execution_result.get("execution_history", []):
                    ev = h.get("evidence", {})
                    if ev.get("decision") == "refine" or ev.get("confidence", 0) < 0.4:
                        if "Possible competing explanations present" not in s["limitations"]:
                            s["limitations"].append("Possible competing explanations present; verify with targeted tests.")

        self.store.save_insights(session_id, structured, plots)

        # suggested next questions (derive from top insight words)
        suggested = []
        if structured:
            top_text = structured[0].get("title", "")
            keywords = ["mobile","desktop","conversion","latency","churn","revenue","trend","anomaly","segment"]
            for k in keywords:
                if k in top_text.lower():
                    suggested.append(f"Explore {k} drivers")
        if not suggested:
            suggested = ["Compare mobile vs desktop behavior","Analyze churn drivers","Show anomalies"]

        return {
            "insights": structured,
            "plots": plots,
            "contradiction": contradiction,
            "insight_count": len(structured),
            "plot_count": len(plots),
            "suggested_next_questions": suggested
        }


class ChatInterface:
    def __init__(self, data_dir: str):
        self.store = InsightStore(data_dir)
        self.groq_client = None
        self.groq_model = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
        api_key = os.environ.get("GROQ_API_KEY")
        if api_key:
            try:
                from groq import Groq
                self.groq_client = Groq(api_key=api_key)
            except Exception:
                self.groq_client = None

    def _generate_with_llm(self, matched_insights: list, user_query: str) -> str:
        if not self.groq_client or not matched_insights:
            return None

        insights_text = "\n".join([
            f"- {i.get('insight', '')} (confidence: {i.get('confidence', 0):.2f})"
            for i in matched_insights[:3]
        ])

        prompt = f"""You are an AI business intelligence analyst. Based on the following validated insights from data analysis, answer the user's question in a natural, conversational way.

IMPORTANT RULES:
- Only use information from the provided insights
- Do not make up new facts
- Include confidence scores when available
- Keep it concise (2-3 sentences)

Validated Insights:
{insights_text}

User Question: {user_query}

Your answer:"""

        try:
            chat_completion = self.groq_client.chat.completions.create(
                model=self.groq_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=200,
            )
            return chat_completion.choices[0].message.content
        except Exception as e:
            print(f"GROQ API error: {e}")
            return None

    def query(self, session_id: str, user_query: str) -> dict:
        allow_chat_only = _get_env_bool("ALLOW_CHAT_ONLY_FROM_INSIGHTS", True)

        data = self.store.load_insights(session_id)
        if not data:
            return {
                "answer": "No insights available. Please run Phase 3 execution first.",
                "confidence": 0,
                "insights": [],
                "plots": []
            }

        insights = data.get("insights", [])
        plots = data.get("plots", [])

        if allow_chat_only and not insights:
            return {
                "answer": "No validated insights found.",
                "confidence": 0,
                "insights": [],
                "plots": []
            }

        query_lower = user_query.lower()
        matched_insights = []
        matched_plots = []

        keywords = ["revenue", "conversion", "mobile", "desktop", "latency", "session", "trend", "anomaly", "cluster"]
        for kw in keywords:
            if kw in query_lower:
                for ins in insights:
                    if kw in ins.get("insight", "").lower():
                        matched_insights.append(ins)
                        ins_id = ins.get("hypothesis_id")
                        for p in plots:
                            if p.get("linked_hypothesis") == ins_id:
                                matched_plots.append(p)

        if not matched_insights and insights:
            matched_insights = insights[:2]
            matched_plots = [p for p in plots if p.get("linked_hypothesis") == matched_insights[0].get("hypothesis_id")]

        if matched_insights:
            top = matched_insights[0]
            llm_answer = self._generate_with_llm(matched_insights, user_query)
            if llm_answer:
                answer = llm_answer
            else:
                answer = top.get("insight", "Based on validated evidence.")
            confidence = top.get("confidence", 0)
        else:
            answer = "No relevant insights found for your query."
            confidence = 0

        response = {
            "answer": answer,
            "confidence": confidence,
            "insights": [i.get("hypothesis_id") for i in matched_insights],
            "plots": [p.get("plot_id") for p in matched_plots],
            "sources": [i.get("insight", "") for i in matched_insights]
        }

        try:
            validate_chat_grounding(response, matched_insights)
        except Exception:
            response["answer"] = top.get("insight", "Based on validated evidence.") if matched_insights else "No relevant insights found."

        return response
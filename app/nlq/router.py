from typing import Optional
from app.nlq.intent_model import NLQIntent, NLQResponse
from app.phase4 import InsightStore
from app.core.tracing import trace


class NLQRouter:
    def __init__(self, data_dir: str, duckdb_connector, schema_store):
        self.data_dir = data_dir
        self.duckdb = duckdb_connector
        self.schema_store = schema_store if schema_store else {}
        self.insight_store = InsightStore(data_dir)

    def route(self, intent: NLQIntent, session_id: str) -> NLQResponse:
        trace("nlq_route_start", {
            "intent_type": intent.intent_type,
            "requires_full_pipeline": intent.requires_full_pipeline,
            "session": session_id
        })

        try:
            return self._route_internal(intent, session_id)
        except Exception as e:
            trace("nlq_route_error", {"error": str(e)})
            return NLQResponse(
                answer=f"Error processing request: {str(e)}",
                confidence=0.0,
                sources=[],
                plots=[],
                intent_type=intent.intent_type
            )

    def _route_internal(self, intent: NLQIntent, session_id: str) -> NLQResponse:
        trace("nlq_route_internal", {"intent": intent.intent_type, "session": session_id})
        
        intent_type = intent.intent_type
        if intent_type in ("predict", "simulation"):
            return self._handle_prediction(intent, session_id)
        elif intent_type in ("metric_lookup", "compare"):
            return self._handle_safe_query(intent, session_id)
        elif intent_type == "explain":
            return self._handle_insights(intent, session_id)
        elif intent_type == "trend":
            return self._handle_insights(intent, session_id)
        elif intent_type == "anomaly":
            return self._handle_insights(intent, session_id)
        else:
            # Default to insights for any unknown intent
            return self._handle_insights(intent, session_id)
            return self._handle_insights(intent, session_id)

    def _handle_insights(self, intent: NLQIntent, session_id: str) -> NLQResponse:
        try:
            data = self.insight_store.load_insights(session_id)
        except Exception as e:
            return NLQResponse(
                answer=f"Could not load insights: {str(e)}",
                confidence=0.0,
                sources=[],
                plots=[],
                intent_type=intent.intent_type
            )
            
        if not data:
            return NLQResponse(
                answer="No insights available. Please run analysis first.",
                confidence=0.0,
                sources=[],
                plots=[],
                intent_type=intent.intent_type
            )

        insights = data.get("insights") if data else []
        if isinstance(insights, dict):
            # Handle case where it's wrapped in a dict
            insights = insights.get("insights", [])
        if not isinstance(insights, list):
            insights = []
            
        plots = data.get("plots") if data else []
        if isinstance(plots, dict):
            plots = plots.get("plots", [])
        if not isinstance(plots, list):
            plots = []

        if not insights:
            return NLQResponse(
                answer="No validated insights found.",
                confidence=0.0,
                sources=[],
                plots=[],
                intent_type=intent.intent_type
            )

        matched_insights = self._match_insights(intent, insights or [])
        matched_plots = self._match_plots(intent, plots or [])

        if matched_insights:
            top_insight = matched_insights[0]
            answer = top_insight.get("insight", "Based on validated evidence.")
            confidence = top_insight.get("confidence", 0.5)
            sources = [i.get("hypothesis_id", "") for i in matched_insights]
            plot_ids = [p.get("plot_id", "") for p in matched_plots]
        else:
            answer = insights[0].get("insight", "Based on data analysis.")
            confidence = insights[0].get("confidence", 0.5)
            sources = [insights[0].get("hypothesis_id", "")]
            plot_ids = [plots[0].get("plot_id", "")] if plots else []

        trace("nlq_route_insights", {
            "matched": len(matched_insights),
            "confidence": confidence
        })

        return NLQResponse(
            answer=answer,
            confidence=confidence,
            sources=sources,
            plots=plot_ids,
            intent_type=intent.intent_type
        )

    def _handle_safe_query(self, intent: NLQIntent, session_id: str) -> NLQResponse:
        if not intent.target_metric:
            return NLQResponse(
                answer="No target metric specified.",
                confidence=0.0,
                sources=[],
                plots=[],
                intent_type=intent.intent_type
            )

        try:
            schema = self.schema_store.get(session_id, {}) if self.schema_store else {}
            columns = schema.get("columns", [])
        except Exception as e:
            columns = []

        if intent.target_metric not in columns:
            return NLQResponse(
                answer=f"Metric '{intent.target_metric}' not found in dataset.",
                confidence=0.0,
                sources=[],
                plots=[],
                intent_type=intent.intent_type
            )

        safe_sql = f"SELECT {intent.target_metric}, COUNT(*) as cnt FROM clean_dataset GROUP BY {intent.target_metric} ORDER BY cnt DESC LIMIT 10"

        try:
            if not self.duckdb:
                return NLQResponse(
                    answer="Database not available",
                    confidence=0.0,
                    sources=[],
                    plots=[],
                    intent_type=intent.intent_type
                )
            result = self.duckdb.query(session_id, safe_sql)
            if result:
                top_val = result[0].get(intent.target_metric, "N/A")
                answer = f"Most common {intent.target_metric}: {top_val}"
                confidence = 0.8
            else:
                answer = f"No data found for {intent.target_metric}"
                confidence = 0.5
        except Exception as e:
            answer = f"Query error: {str(e)}"
            confidence = 0.0

        return NLQResponse(
            answer=answer,
            confidence=confidence,
            sources=["sql_query"],
            plots=[],
            intent_type=intent.intent_type
        )

    def _handle_prediction(self, intent: NLQIntent, session_id: str) -> NLQResponse:
        trace("nlq_route_prediction", {"session": session_id})

        return NLQResponse(
            answer="Prediction requires running predictive analysis. Use /predict endpoint.",
            confidence=0.0,
            sources=[],
            plots=[],
            intent_type=intent.intent_type
        )

    def _match_insights(self, intent: NLQIntent, insights: list) -> list:
        if not intent.target_metric:
            return insights[:3]

        matched = []
        question_lower = (intent.target_metric or "").lower()

        for ins in insights:
            ins_text = ins.get("insight", "").lower()
            if question_lower in ins_text:
                matched.append(ins)

        return matched[:3] if matched else insights[:3]

    def _match_plots(self, intent: NLQIntent, plots: list) -> list:
        if not intent.dimensions:
            return plots[:2]

        matched = []
        for plot in plots:
            plot_title = plot.get("title", "").lower()
            for dim in intent.dimensions:
                if dim in plot_title:
                    matched.append(plot)

        return matched[:2] if matched else plots[:2]
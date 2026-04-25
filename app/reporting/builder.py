from typing import Optional
from app.reporting.schema import Report, ReportMetadata, ReportSection
from app.phase4 import InsightStore
from app.core.tracing import trace


class ReportBuilder:
    def __init__(self, data_dir: str, schema_store):
        self.data_dir = data_dir
        self.schema_store = schema_store
        self.insight_store = InsightStore(data_dir)

    def build(self, session_id: str, title: Optional[str] = None, include_predictions: bool = False) -> Report:
        trace("report_build_start", {"session": session_id})

        insights_data = self.insight_store.load_insights(session_id)
        schema = self.schema_store.get(session_id, {})

        insights = insights_data.get("insights", []) if insights_data else []
        plots = insights_data.get("plots", []) if insights_data else []

        ranked_insights = self._rank_insights(insights)

        summary = self._generate_summary(ranked_insights)

        report_title = title or f"Data Analysis Report - {session_id[:8]}"

        metadata = ReportMetadata(
            session_id=session_id,
            generated_at=Report().generated_at,
            total_insights=len(ranked_insights),
            total_plots=len(plots)
        )

        report = Report(
            title=report_title,
            summary=summary,
            insights=ranked_insights,
            plots=plots,
            predictions=[],
            stats=self._build_stats(schema, insights_data),
            format="json"
        )

        trace("report_build_complete", {
            "session": session_id,
            "insights": len(ranked_insights),
            "plots": len(plots)
        })

        return report

    def _rank_insights(self, insights: list) -> list:
        if not insights:
            return []

        ranked = sorted(
            insights,
            key=lambda x: (x.get("confidence", 0), x.get("effect_size", 0)),
            reverse=True
        )

        return ranked[:10]

    def _generate_summary(self, insights: list) -> str:
        if not insights:
            return "No insights available. Please run the analysis pipeline first."

        total = len(insights)
        high_conf = sum(1 for i in insights if i.get("confidence", 0) >= 0.75)
        medium_conf = sum(1 for i in insights if 0.5 <= i.get("confidence", 0) < 0.75)

        summary_parts = [
            f"Analysis generated {total} insights.",
            f"{high_conf} high-confidence ({high_conf/total*100:.0f}%),",
            f"{medium_conf} medium-confidence ({medium_conf/total*100:.0f}%)."
        ]

        if insights:
            top = insights[0]
            summary_parts.append(
                f"Top insight: {top.get('insight', 'N/A')[:100]}..."
            )

        return " ".join(summary_parts)

    def _build_stats(self, schema: dict, insights_data: Optional[dict]) -> dict:
        stats = {
            "columns": len(schema.get("columns", [])),
            "column_types": schema.get("inferred_types", {}),
            "insights_count": len(insights_data.get("insights", [])) if insights_data else 0,
            "plots_count": len(insights_data.get("plots", [])) if insights_data else 0
        }

        return stats

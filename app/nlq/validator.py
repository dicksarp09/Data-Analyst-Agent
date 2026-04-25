from typing import Optional
from app.nlq.intent_model import NLQIntent
from app.core.tracing import trace


class NLQValidationError(Exception):
    pass


class NLQValidator:
    def __init__(self, schema_store):
        self.schema_store = schema_store

    def validate(self, intent: NLQIntent, session_id: str) -> tuple[bool, Optional[str]]:
        trace("nlq_validate_start", {"intent": intent.intent_type, "session": session_id})

        if not self.schema_store:
            return True, None
            
        schema = self.schema_store.get(session_id, {})
        if not schema:
            return True, None  # Allow without validation

        columns = schema.get("columns", [])
        inferred_types = schema.get("inferred_types", {})

        if intent.target_metric:
            valid, msg = self._validate_metric(intent.target_metric, columns, inferred_types)
            if not valid:
                trace("nlq_validate_metric_fail", {"metric": intent.target_metric})
                return False, msg

        if intent.dimensions:
            valid, msg = self._validate_dimensions(intent.dimensions, columns)
            if not valid:
                trace("nlq_validate_dimension_fail", {"dimensions": intent.dimensions})
                return False, msg

        if intent.time_range:
            valid, msg = self._validate_time_range(intent.time_range, columns)
            if not valid:
                trace("nlq_validate_timerange_fail", {"time_range": intent.time_range})
                return False, msg

        trace("nlq_validate_success", {"intent": intent.intent_type})
        return True, None

    def _validate_metric(self, metric: str, columns: list, types: dict) -> tuple[bool, Optional[str]]:
        metric_lower = metric.lower()

        for col in columns:
            if metric_lower in col.lower():
                col_type = types.get(col, "").lower()
                if col_type in ("int", "float", "bigint", "double"):
                    return True, None

        return False, f"Metric '{metric}' not found or not numeric in schema"

    def _validate_dimensions(self, dimensions: list, columns: list) -> tuple[bool, Optional[str]]:
        columns_lower = [c.lower() for c in columns]

        for dim in dimensions:
            found = any(dim in col for col in columns_lower)
            if not found:
                return False, f"Dimension '{dim}' not found in schema"

        return True, None

    def _validate_time_range(self, time_range: str, columns: list) -> tuple[bool, Optional[str]]:
        valid_ranges = ("7d", "30d", "90d", "365d", "ytd", "q1", "q2", "q3", "q4")

        if time_range not in valid_ranges:
            return False, f"Invalid time range '{time_range}'"

        time_columns = [c for c in columns if any(t in c.lower() for t in ("date", "time", "timestamp"))]
        if not time_columns:
            return False, "No time column found in schema"

        return True, None

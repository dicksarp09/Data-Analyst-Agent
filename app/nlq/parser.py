import os
import re
from typing import Optional
from app.nlq.intent_model import NLQIntent
from app.core.tracing import trace


class NLQParser:
    def __init__(self):
        self.groq_client = None
        api_key = os.environ.get("GROQ_API_KEY")
        if api_key:
            try:
                from groq import Groq
                self.groq_client = Groq(api_key=api_key)
            except Exception:
                pass

    def parse(self, question: str) -> NLQIntent:
        trace("nlq_parse_start", {"question": question[:50]})

        try:
            question_lower = question.lower()

            intent_type = self._detect_intent_type(question_lower)
            requires_full_pipeline = intent_type in ("predict", "simulation")
            target_metric = self._extract_metric(question_lower)
            dimensions = self._extract_dimensions(question_lower)
            time_range = self._extract_time_range(question_lower)

            intent = NLQIntent(
                intent_type=intent_type,
                target_metric=target_metric,
                dimensions=dimensions,
                time_range=time_range,
                requires_full_pipeline=requires_full_pipeline,
                raw_question=question
            )

            trace("nlq_parse_complete", {"intent_type": intent.intent_type})

            return intent

        except Exception as e:
            trace("nlq_parse_exception", {"error": str(e), "question": question[:50]})
            return NLQIntent(
                intent_type="explain",
                target_metric=None,
                dimensions=None,
                time_range=None,
                requires_full_pipeline=False,
                raw_question=question
            )

    def _detect_intent_type(self, question: str) -> str:
        patterns = {
            "explain": [r"why", r"explain", r"what.*caused", r"reason"],
            "compare": [r"compare", r"difference", r"vs|versus", r"better"],
            "trend": [r"trend", r"over time", r"change", r"growth", r"decline"],
            "anomaly": [r"unusual", r"anomaly", r"outlier", r"unexpected", r"strange"],
            "metric_lookup": [r"how many", r"total", r"average", r"sum", r"count"],
            "predict": [r"predict", r"forecast", r"future", r"will"],
            "simulation": [r"what if", r"simulate", r"change.*by", r"impact"]
        }

        for intent, regex_list in patterns.items():
            for regex in regex_list:
                if re.search(regex, question):
                    return intent

        return "explain"

    def _extract_metric(self, question: str) -> Optional[str]:
        metric_keywords = ["revenue", "sales", "profit", "users", "conversion",
                          "cost", "price", "quantity", "count", "amount"]

        for metric in metric_keywords:
            if metric in question:
                return metric

        return None

    def _extract_dimensions(self, question: str) -> Optional[list]:
        dimension_keywords = ["region", "country", "city", "product", "category",
                            "segment", "channel", "source", "device"]

        found = []
        for dim in dimension_keywords:
            if dim in question:
                found.append(dim)

        return found if found else None

    def _extract_time_range(self, question: str) -> Optional[str]:
        time_keywords = {
            r"last week": "7d",
            r"last month": "30d",
            r"last quarter": "90d",
            r"last year": "365d",
            r"this year": "ytd",
            r"recent": "7d"
        }

        for pattern, value in time_keywords.items():
            if re.search(pattern, question):
                return value

        return None
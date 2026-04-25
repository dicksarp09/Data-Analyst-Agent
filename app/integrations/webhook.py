import os
from typing import Optional, Dict, Any
from app.core.tracing import trace


class WebhookClient:
    def __init__(self):
        self.timeout = int(os.environ.get("WEBHOOK_TIMEOUT", 10))

    def send(self, url: str, payload: Dict[str, Any]) -> bool:
        trace("webhook_send_start", {"url": url})

        if not url:
            trace("webhook_no_url", {})
            return False

        sanitized_payload = self._sanitize_payload(payload)

        try:
            import requests

            response = requests.post(
                url,
                json=sanitized_payload,
                timeout=self.timeout
            )

            if response.status_code < 400:
                trace("webhook_success", {"url": url, "status": response.status_code})
                return True
            else:
                trace("webhook_error", {
                    "url": url,
                    "status": response.status_code,
                    "body": response.text[:200]
                })
                return False

        except ImportError:
            trace("webhook_requests_missing", {})
            return False
        except Exception as e:
            trace("webhook_exception", {"error": str(e)})
            return False

    def _sanitize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = {}

        for key, value in payload.items():
            if key.lower() in ("password", "secret", "token", "api_key"):
                sanitized[key] = "***REDACTED***"
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_payload(value)
            elif key in ("data", "csv", "rows"):
                sanitized[key] = f"<{len(value) if hasattr(value, '__len__') else 'N/A'} rows>"
            else:
                sanitized[key] = value

        return sanitized

    def send_insight(self, url: str, insight: Dict[str, Any]) -> bool:
        payload = {
            "event": "insight_generated",
            "data": {
                "insight": insight.get("insight", ""),
                "confidence": insight.get("confidence", 0),
                "type": insight.get("type", ""),
                "hypothesis_id": insight.get("hypothesis_id", "")
            }
        }

        return self.send(url, payload)

    def send_report(self, url: str, report_summary: Dict[str, Any]) -> bool:
        payload = {
            "event": "report_ready",
            "data": {
                "title": report_summary.get("title", ""),
                "insight_count": report_summary.get("insights_count", 0),
                "plot_count": report_summary.get("plots_count", 0),
                "summary": report_summary.get("summary", "")[:500]
            }
        }

        return self.send(url, payload)
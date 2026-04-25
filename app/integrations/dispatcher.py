import os
from typing import Optional
from app.integrations.events import Event, EventType
from app.integrations.slack import SlackNotifier
from app.integrations.webhook import WebhookClient
from app.core.tracing import trace


class EventDispatcher:
    def __init__(self):
        self.slack = SlackNotifier() if os.environ.get("SLACK_WEBHOOK_URL") else None
        self.webhook = WebhookClient()
        self.rate_limit_calls = {}
        self.rate_limit_window = 60
        self.max_calls_per_window = 10

    def dispatch(self, event: Event) -> bool:
        trace("dispatch_start", {"event_type": event.type, "session": event.session_id})

        if self._check_rate_limit(event.type):
            trace("dispatch_rate_limited", {"event_type": event.type})
            return False

        success = False

        if event.type in (EventType.INSIGHT_GENERATED, EventType.REPORT_READY):
            success = self._dispatch_insight_event(event)
        elif event.type == EventType.PIPELINE_PROGRESS:
            success = self._dispatch_progress_event(event)
        elif event.type == EventType.ERROR:
            success = self._dispatch_error_event(event)

        if success:
            self._record_call(event.type)

        trace("dispatch_complete", {"success": success, "event_type": event.type})
        return success

    def _dispatch_insight_event(self, event: Event) -> bool:
        if not event.payload:
            return False

        message = self._format_insight_message(event.payload)

        if self.slack:
            try:
                self.slack.send(message)
            except Exception as e:
                trace("dispatch_slack_error", {"error": str(e)})

        webhook_urls = os.environ.get("WEBHOOK_URLS", "").split(",")
        for url in webhook_urls:
            if url.strip():
                try:
                    self.webhook.send(url.strip(), event.payload)
                except Exception as e:
                    trace("dispatch_webhook_error", {"error": str(e)})

        return True

    def _dispatch_progress_event(self, event: Event) -> bool:
        payload = event.payload
        message = f"Analysis progress: {payload.get('progress_percent', 0)}% - {payload.get('current_phase', 'unknown')}"

        if self.slack:
            try:
                self.slack.send(message)
            except Exception:
                pass

        return True

    def _dispatch_error_event(self, event: Event) -> bool:
        payload = event.payload
        message = f"Error in session {event.session_id}: {payload.get('error_message', 'Unknown error')}"

        if self.slack:
            try:
                self.slack.send(message)
            except Exception:
                pass

        return True

    def _format_insight_message(self, payload: dict) -> str:
        parts = []

        if payload.get("insight_text"):
            text = payload["insight_text"]
            parts.append(text[:200] + "..." if len(text) > 200 else text)

        if payload.get("confidence"):
            conf = payload["confidence"] * 100
            parts.append(f"Confidence: {conf:.0f}%")

        if payload.get("plot_count", 0) > 0:
            parts.append(f"Plots: {payload['plot_count']}")

        return " | ".join(parts) if parts else "New analysis complete"

    def _check_rate_limit(self, event_type: str) -> bool:
        import time
        now = time.time()

        if event_type not in self.rate_limit_calls:
            self.rate_limit_calls[event_type] = []

        calls = self.rate_limit_calls[event_type]
        recent_calls = [t for t in calls if now - t < self.rate_limit_window]

        if len(recent_calls) >= self.max_calls_per_window:
            return True

        self.rate_limit_calls[event_type] = recent_calls
        return False

    def _record_call(self, event_type: str) -> None:
        import time
        if event_type not in self.rate_limit_calls:
            self.rate_limit_calls[event_type] = []
        self.rate_limit_calls[event_type].append(time.time())
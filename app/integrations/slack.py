import os
from typing import Optional
from app.core.tracing import trace


class SlackNotifier:
    def __init__(self):
        self.webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
        self.enabled = bool(self.webhook_url)

    def send(self, message: str, blocks: Optional[list] = None) -> bool:
        if not self.enabled:
            trace("slack_disabled", {})
            return False

        if not message:
            message = "Data Analyst Agent notification"

        payload = {
            "text": message
        }

        if blocks:
            payload["blocks"] = blocks

        try:
            import requests

            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )

            if response.status_code == 200:
                trace("slack_sent", {"message": message[:50]})
                return True
            else:
                trace("slack_error", {
                    "status": response.status_code,
                    "body": response.text
                })
                return False

        except ImportError:
            trace("slack_requests_missing", {})
            return False
        except Exception as e:
            trace("slack_exception", {"error": str(e)})
            return False

    def send_insight(self, insight_text: str, confidence: float, session_id: str) -> bool:
        confidence_pct = confidence * 100

        if confidence >= 0.75:
            emoji = "🎯"
            color = "#6bd8cb"
        elif confidence >= 0.5:
            emoji = "📊"
            color = "#ffb95f"
        else:
            emoji = "⚠️"
            color = "#ffb4ab"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} New Insight Generated"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{insight_text[:200]}*"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Confidence: {confidence_pct:.0f}% | Session: `{session_id[:8]}`"
                    }
                ]
            }
        ]

        return self.send(insight_text, blocks)

    def send_report_ready(self, report_path: str, session_id: str, insight_count: int) -> bool:
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📋 Report Ready"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Analysis complete with *{insight_count}* insights"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Session: `{session_id[:8]}`"
                    }
                ]
            }
        ]

        return self.send(f"Report ready with {insight_count} insights", blocks)
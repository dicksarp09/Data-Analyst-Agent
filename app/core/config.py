import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class Config:
    MIN_DATA_POINTS: int = 50
    ENABLE_PREDICTION: bool = True
    ENABLE_REPORT_EXPORT: bool = True
    ENABLE_TRACING: bool = True
    SLACK_WEBHOOK_URL: Optional[str] = None
    WEBHOOK_URLS: str = ""
    WEBHOOK_TIMEOUT: int = 10
    RATE_LIMIT_CALLS: int = 10
    RATE_LIMIT_WINDOW: int = 60
    AUDIT_LOG_FILE: str = "audit.log"


_config: Optional[Config] = None


def get_config() -> Config:
    global _config

    if _config is None:
        _config = Config(
            MIN_DATA_POINTS=int(os.environ.get("MIN_DATA_POINTS", 50)),
            ENABLE_PREDICTION=os.environ.get("ENABLE_PREDICTION", "true").lower() == "true",
            ENABLE_REPORT_EXPORT=os.environ.get("ENABLE_REPORT_EXPORT", "true").lower() == "true",
            ENABLE_TRACING=os.environ.get("ENABLE_TRACING", "true").lower() == "true",
            SLACK_WEBHOOK_URL=os.environ.get("SLACK_WEBHOOK_URL"),
            WEBHOOK_URLS=os.environ.get("WEBHOOK_URLS", ""),
            WEBHOOK_TIMEOUT=int(os.environ.get("WEBHOOK_TIMEOUT", 10)),
            RATE_LIMIT_CALLS=int(os.environ.get("RATE_LIMIT_CALLS", 10)),
            RATE_LIMIT_WINDOW=int(os.environ.get("RATE_LIMIT_WINDOW", 60)),
            AUDIT_LOG_FILE=os.environ.get("AUDIT_LOG_FILE", "audit.log")
        )

    return _config
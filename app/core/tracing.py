import os
import json
from datetime import datetime
from typing import Any


def trace(event: str, data: dict) -> None:
    """Trace all events across the system."""
    trace_enabled = os.environ.get("ENABLE_TRACING", "true").lower() == "true"
    if not trace_enabled:
        return

    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "event": event,
        "data": _sanitize_data(data)
    }

    print(f"[TRACE] {json.dumps(log_entry)}")


def _sanitize_data(data: dict) -> dict:
    """Remove sensitive data from traces."""
    sensitive_keys = ("password", "api_key", "secret", "token", "session_id")

    sanitized = {}
    for key, value in data.items():
        if any(s in key.lower() for s in sensitive_keys):
            sanitized[key] = "***REDACTED***"
        elif isinstance(value, dict):
            sanitized[key] = _sanitize_data(value)
        else:
            sanitized[key] = value

    return sanitized


def audit(event: str, data: dict) -> None:
    """Audit log for compliance."""
    audit_log = os.environ.get("AUDIT_LOG_FILE", "audit.log")

    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "event": event,
        "data": _sanitize_data(data)
    }

    try:
        with open(audit_log, "a") as f:
            f.write(json.dumps(log_entry) + "\n")
    except Exception:
        pass
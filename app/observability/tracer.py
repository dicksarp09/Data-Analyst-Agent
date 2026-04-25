import json
import time
import functools
from pathlib import Path
from typing import Optional, Callable, Any
from datetime import datetime
from contextlib import contextmanager


class TraceEvent:
    def __init__(
        self,
        event_type: str,
        phase: str,
        session_id: str,
        details: dict = None,
        timestamp: str = None
    ):
        self.event_type = event_type
        self.phase = phase
        self.session_id = session_id
        self.details = details or {}
        self.timestamp = timestamp or datetime.utcnow().isoformat()

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "phase": self.phase,
            "session_id": self.session_id,
            "timestamp": self.timestamp,
            "details": self.details
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class Tracer:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.current_session_id: Optional[str] = None

    def _get_trace_path(self, session_id: str) -> Path:
        session_dir = self.data_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        return session_dir / "trace.json"

    def start_session(self, session_id: str) -> None:
        self.current_session_id = session_id

    def emit(
        self,
        event_type: str,
        phase: str,
        details: dict = None
    ) -> TraceEvent:
        if not self.current_session_id:
            return None

        event = TraceEvent(
            event_type=event_type,
            phase=phase,
            session_id=self.current_session_id,
            details=details or {}
        )

        trace_path = self._get_trace_path(self.current_session_id)
        
        existing = []
        if trace_path.exists():
            try:
                with open(trace_path) as f:
                    existing = json.load(f)
            except:
                existing = []

        existing.append(event.to_dict())
        
        with open(trace_path, "w") as f:
            json.dump(existing, f, indent=2)

        return event

    def emit_phase_start(self, phase: str, details: dict = None) -> TraceEvent:
        return self.emit("phase_start", phase, details)

    def emit_phase_end(self, phase: str, details: dict = None) -> TraceEvent:
        return self.emit("phase_end", phase, details)

    def emit_decision(self, phase: str, decision: str, details: dict = None) -> TraceEvent:
        return self.emit("decision", phase, {"decision": decision, **(details or {})})

    def emit_action(self, phase: str, action: str, details: dict = None) -> TraceEvent:
        return self.emit("action", phase, {"action": action, **(details or {})})

    def emit_error(self, phase: str, error: str, details: dict = None) -> TraceEvent:
        return self.emit("error", phase, {"error": error, **(details or {})})

    def load_trace(self, session_id: str) -> list:
        trace_path = self._get_trace_path(session_id)
        if not trace_path.exists():
            return []

        with open(trace_path) as f:
            return json.load(f)


def trace(phase: str, include_args: bool = False):
    """
    Decorator to trace function execution.
    
    Usage:
        @trace("phase1")
        def my_function(x, y):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            result = func(*args, **kwargs)
            
            duration = time.time() - start_time
            
            details = {"duration_ms": round(duration * 1000, 2)}
            if include_args:
                details["args"] = str(args[:3]) if args else ""
                details["kwargs"] = str(list(kwargs.keys())[:3])
            
            return result
        return wrapper
    return decorator


@contextmanager
def trace_context(tracer: Tracer, phase: str, session_id: str, details: dict = None):
    """
    Context manager for tracing a block of code.
    
    Usage:
        with trace_context(tracer, "phase3", session_id, {"hypothesis_count": 5}):
            # code to trace
            ...
    """
    tracer.start_session(session_id)
    tracer.emit_phase_start(phase, details)
    
    start_time = time.time()
    error = None
    result = None
    
    try:
        yield
    except Exception as e:
        error = str(e)
        raise
    finally:
        duration = time.time() - start_time
        end_details = {"duration_ms": round(duration * 1000, 2)}
        if error:
            end_details["error"] = error
        
        tracer.emit_phase_end(phase, end_details)


class PhaseTransitionTracker:
    def __init__(self, tracer: Tracer):
        self.tracer = tracer
        self.phase_order = ["phase0", "phase1", "phase2", "phase3", "phase4"]
        self.current_phase: Optional[str] = None

    def transition_to(self, phase: str, session_id: str, details: dict = None) -> None:
        if self.current_phase:
            self.tracer.emit_phase_end(
                self.current_phase,
                {"transition_to": phase}
            )
        
        self.current_phase = phase
        self.tracer.emit_phase_start(phase, details or {})

    def record_decision(
        self,
        phase: str,
        decision_type: str,
        decision_value: Any,
        details: dict = None
    ) -> None:
        self.tracer.emit_decision(
            phase,
            decision_type,
            {"value": str(decision_value), **(details or {})}
        )

    def record_action(
        self,
        phase: str,
        action_name: str,
        details: dict = None
    ) -> None:
        self.tracer.emit_action(phase, action_name, details or {})
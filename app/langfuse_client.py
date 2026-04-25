"""
Langfuse Observability Integration (v2 API)

Provides tracing for the Data Analyst Agent following best practices:
- Proper span hierarchy for multi-step operations
- Session-based tracing for conversation flows
- Flush handling for script execution
"""
import os
from functools import wraps
from typing import Optional, Any
from contextlib import contextmanager

_langfuse_client: Optional[Any] = None
_is_initialized: bool = False


def get_langfuse_client() -> Optional[Any]:
    """Get or initialize the Langfuse client."""
    global _langfuse_client, _is_initialized
    
    if _is_initialized:
        return _langfuse_client
    
    public_key = os.environ.get("LANGFUSE_PUBLIC_KEY")
    secret_key = os.environ.get("LANGFUSE_SECRET_KEY")
    host = os.environ.get("LANGFUSE_HOST", os.environ.get("LANGFUSE_BASE_URL", "https://cloud.langfuse.com"))
    
    if not public_key or not secret_key:
        return None
    
    try:
        from langfuse import Langfuse
        
        _langfuse_client = Langfuse(
            public_key=public_key,
            secret_key=secret_key,
            host=host
        )
        _is_initialized = True
        return _langfuse_client
    except Exception:
        return None


def is_enabled() -> bool:
    """Check if Langfuse is enabled (keys configured)."""
    return get_langfuse_client() is not None


def flush():
    """Flush pending traces to Langfuse. Call before script exit."""
    if _langfuse_client:
        try:
            _langfuse_client.flush()
        except Exception:
            pass


def observe(
    name: str,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
    tags: Optional[list] = None,
    metadata: Optional[dict] = None
):
    """
    Decorator to trace a function with Langfuse.
    
    Usage:
        @observe("phase1-signal-detection", session_id="abc123")
        def run_signal_detection(session_id):
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            client = get_langfuse_client()
            
            if not client:
                return func(*args, **kwargs)
            
            with client.start_as_current_span(
                name=name,
                session_id=session_id,
                user_id=user_id,
                tags=tags,
                metadata=metadata
            ) as span:
                try:
                    result = func(*args, **kwargs)
                    span.output = str(result)[:500]
                    return result
                except Exception as e:
                    span.add_event(f"error: {str(e)}")
                    raise
        
        return wrapper
    return decorator


def span(
    name: str,
    metadata: Optional[dict] = None
):
    """
    Decorator to create a nested span within a trace.
    
    Usage:
        @observe("process-data")
        def process_data():
            @span("validate-schema")
            def validate():
                ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            client = get_langfuse_client()
            
            if not client:
                return func(*args, **kwargs)
            
            with client.start_as_current_span(name=name, metadata=metadata) as span_obj:
                try:
                    result = func(*args, **kwargs)
                    span_obj.output = str(result)[:500]
                    return result
                except Exception as e:
                    span_obj.add_event(f"error: {str(e)}")
                    raise
        
        return wrapper
    return decorator


@contextmanager
def trace_context(
    name: str,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
    tags: Optional[list] = None,
    metadata: Optional[dict] = None
):
    """
    Context manager for tracing a block of code.
    
    Usage:
        with trace_context("data-upload", session_id="abc123") as trace:
            # code to trace
            trace.add_event("file received")
    """
    client = get_langfuse_client()
    
    if not client:
        yield None
        return
    
    meta = metadata or {}
    if session_id:
        meta["session_id"] = session_id
    if tags:
        meta["tags"] = tags
    
    with client.start_as_current_span(
        name=name,
        metadata=meta
    ) as span:
        yield span


@contextmanager
def span_context(
    name: str,
    metadata: Optional[dict] = None
):
    """Context manager for nested spans."""
    client = get_langfuse_client()
    
    if not client:
        yield None
        return
    
    with client.start_as_current_span(name=name, metadata=metadata) as span_obj:
        yield span_obj


def generation(
    name: str,
    model: str,
    messages: Optional[list] = None,
    metadata: Optional[dict] = None
):
    """
    Decorator for LLM generation traces.
    
    Usage:
        @generation("hypothesis-generation", model="llama-3.3-70b-versatile")
        def generate_hypotheses():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            client = get_langfuse_client()
            
            if not client:
                return func(*args, **kwargs)
            
            with client.start_as_current_generation(
                name=name,
                model=model,
                messages=messages,
                metadata=metadata
            ) as gen:
                try:
                    result = func(*args, **kwargs)
                    if result:
                        gen.response = result[:500] if isinstance(result, str) else str(result)
                    return result
                except Exception as e:
                    gen.add_event(f"error: {str(e)}")
                    raise
        
        return wrapper
    return decorator


class LangfuseTracing:
    """Helper class for manual tracing operations."""
    
    def __init__(self, session_id: Optional[str] = None, user_id: Optional[str] = None):
        self.session_id = session_id
        self.user_id = user_id
        self.client = get_langfuse_client()
    
    def start_trace(self, name: str, tags: Optional[list] = None, metadata: Optional[dict] = None):
        """Start a new trace."""
        if not self.client:
            return None
        
        return self.client.start_as_current_span(
            name=name,
            session_id=self.session_id,
            user_id=self.user_id,
            tags=tags,
            metadata=metadata
        )
    
    def start_span(self, name: str, metadata: Optional[dict] = None):
        """Start a nested span."""
        if not self.client:
            return None
        
        return self.client.start_as_current_span(name=name, metadata=metadata)
    
    def add_event(self, span, event_name: str, metadata: Optional[dict] = None):
        """Add an event to a span."""
        if span:
            span.add_event(name=event_name, metadata=metadata)
    
    def set_input(self, span, input_data: Any):
        """Set input on a span."""
        if span:
            span.input = str(input_data)[:500]
    
    def set_output(self, span, output_data: Any):
        """Set output on a span."""
        if span:
            span.output = str(output_data)[:500]
    
    def set_score(self, span, score_name: str, value: float):
        """Set a score on a span for evaluation."""
        if span:
            span.score(
                name=score_name,
                value=value
            )
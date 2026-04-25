"""
Reliability Layer - Retry logic with exponential backoff
"""
import time
import random
import logging
from functools import wraps
from typing import Callable, Any, Optional, TypeVar, List

logger = logging.getLogger(__name__)

T = TypeVar('T')


class RetryExhausted(Exception):
    """Raised when all retry attempts are exhausted."""
    pass


class CircuitBreakerOpen(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """
    Simple circuit breaker implementation.
    
    Opens circuit after max_failures consecutive failures.
    Resets after reset_timeout seconds.
    """
    def __init__(self, max_failures: int = 3, reset_timeout: int = 60):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"  # closed, open, half-open
    
    def record_success(self):
        """Record a successful operation."""
        self.failures = 0
        self.state = "closed"
    
    def record_failure(self):
        """Record a failed operation."""
        self.failures += 1
        self.last_failure_time = time.time()
        
        if self.failures >= self.max_failures:
            self.state = "open"
    
    def can_execute(self) -> bool:
        """Check if operation can be executed."""
        if self.state == "closed":
            return True
        
        if self.state == "open" and self.last_failure_time:
            elapsed = time.time() - self.last_failure_time
            if elapsed >= self.reset_timeout:
                self.state = "half-open"
                return True
            return False
        
        if self.state == "half-open":
            return True
        
        return False
    
    def reset(self):
        """Reset the circuit breaker."""
        self.failures = 0
        self.state = "closed"
        self.last_failure_time = None


def retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential: bool = True,
    jitter: bool = True,
    retry_on: tuple = (Exception,)
) -> Callable:
    """
    Decorator for retry logic with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exponential: Use exponential backoff
        jitter: Add random jitter
        retry_on: Tuple of exceptions to retry on
        
    Returns:
        Decorated function
        
    Usage:
        @retry(max_retries=3, base_delay=2.0)
        def risky_operation():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"All {max_retries} retries exhausted for {func.__name__}")
                        raise RetryExhausted(f"Max retries ({max_retries}) exceeded: {str(e)}") from e
                    
                    if exponential:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                    else:
                        delay = base_delay
                    
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    logger.warning(
                        f"Retry {attempt + 1}/{max_retries} for {func.__name__}: "
                        f"{str(e)}. Waiting {delay:.2f}s"
                    )
                    time.sleep(delay)
            
            raise last_exception
        
        return wrapper
    return decorator


def retry_with_fallback(
    fallback_return: Any = None,
    max_retries: int = 3,
    base_delay: float = 1.0
) -> Callable:
    """
    Decorator that returns fallback value on exhaustion.
    
    Args:
        fallback_return: Value to return when retries exhausted
        max_retries: Maximum retries
        base_delay: Base delay
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        @retry(max_retries=max_retries, base_delay=base_delay)
        def wrapper(*args, **kwargs) -> T:
            return func(*args, **kwargs)
        
        def wrapper_with_fallback(*args, **kwargs) -> T:
            try:
                return wrapper(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Returning fallback for {func.__name__}: {str(e)}")
                return fallback_return
        
        return wrapper_with_fallback
    return decorator


def with_timeout(timeout_seconds: float) -> Callable:
    """
    Decorator to add timeout to operations.
    
    Args:
        timeout_seconds: Maximum execution time
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError(f"Operation {func.__name__} timed out after {timeout_seconds}s")
            
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(int(timeout_seconds))
            
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
            
            return result
        
        return wrapper
    return decorator


class RetryContext:
    """Context manager for retry operations with state tracking."""
    
    def __init__(self, operation_name: str, max_retries: int = 3):
        self.operation_name = operation_name
        self.max_retries = max_retries
        self.attempts = 0
        self.errors: List[str] = []
    
    def __enter__(self):
        self.attempts += 1
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            return True
        
        self.errors.append(str(exc_val))
        
        if self.attempts >= self.max_retries:
            logger.error(f"Operation {self.operation_name} failed after {self.attempts} attempts: {self.errors}")
            return False
        
        return True
    
    @property
    def should_retry(self) -> bool:
        return self.attempts < self.max_retries
    
    @property
    def is_exhausted(self) -> bool:
        return self.attempts >= self.max_retries
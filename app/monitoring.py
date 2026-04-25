"""
Production Monitoring, Logging, and Rate Limiting

Provides:
- Structured logging to file with rotation
- Request/response monitoring
- Rate limiting per IP
- Health checks and metrics
"""
import os
import time
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Optional
from contextlib import contextmanager
from functools import wraps

from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


class FileLogHandler(logging.Handler):
    """Custom handler that writes logs to a rotating file."""
    
    def __init__(self, log_dir: str, max_bytes: int = 10 * 1024 * 1024, backup_count: int = 5):
        super().__init__()
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / "app.log"
        self.max_bytes = max_bytes
        self.backup_count = backup_count
    
    def emit(self, record):
        try:
            msg = self.format(record)
            
            # Rotate if needed
            if self.log_file.exists() and self.log_file.stat().st_size >= self.max_bytes:
                self._rotate()
            
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            self.handleError(record)
    
    def _rotate(self):
        """Rotate log files."""
        for i in range(self.backup_count - 1, 0, -1):
            src = self.log_dir / f"app.{i}.log"
            dst = self.log_dir / f"app.{i + 1}.log"
            if src.exists():
                src.rename(dst)
        
        if self.log_file.exists():
            self.log_file.rename(self.log_dir / "app.1.log")


def setup_logging(log_dir: str = "logs", level: str = "INFO") -> logging.Logger:
    """Setup structured logging to file."""
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger("data-analyst-agent")
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # File handler
    file_handler = FileLogHandler(str(log_dir))
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S"
    ))
    logger.addHandler(console_handler)
    
    return logger


class RateLimiter(BaseHTTPMiddleware):
    """Rate limiting middleware - limits requests per IP."""
    
    def __init__(self, app, requests_per_minute: int = 60, burst: int = 10):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.burst = burst
        self.requests: dict = defaultdict(list)
        self.blocked_ips: dict = {}
    
    def _get_client_ip(self, request: Request) -> str:
        """Get client IP, considering X-Forwarded-For header."""
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"
    
    def _clean_old_requests(self, ip: str):
        """Remove requests older than 1 minute."""
        now = time.time()
        self.requests[ip] = [t for t in self.requests[ip] if now - t < 60]
    
    def _is_blocked(self, ip: str) -> bool:
        """Check if IP is temporarily blocked."""
        if ip in self.blocked_ips:
            if time.time() < self.blocked_ips[ip]:
                return True
            del self.blocked_ips[ip]
        return False
    
    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for health check
        if request.url.path in ["/health", "/docs", "/openapi.json"]:
            return await call_next(request)
        
        ip = self._get_client_ip(request)
        
        # Check if blocked
        if self._is_blocked(ip):
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Too many requests. Please try again later."}
            )
        
        # Clean old requests
        self._clean_old_requests(ip)
        
        now = time.time()
        request_times = self.requests[ip]
        
        # Check burst limit (requests in last 5 seconds)
        recent = [t for t in request_times if now - t < 5]
        if len(recent) >= self.burst:
            # Block for 30 seconds
            self.blocked_ips[ip] = now + 30
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Rate limit exceeded. Blocked for 30 seconds."}
            )
        
        # Check per-minute limit
        if len(request_times) >= self.requests_per_minute:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Rate limit exceeded. Max 60 requests per minute."}
            )
        
        # Record request
        request_times.append(now)
        
        return await call_next(request)


class RequestMonitor:
    """Track request metrics."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.total_requests = 0
        self.total_errors = 0
        self.total_latency = 0.0
        self.endpoint_counts = defaultdict(int)
        self.start_time = time.time()
    
    def record_request(self, method: str, path: str, status: int, latency: float):
        """Record request metrics."""
        self.total_requests += 1
        self.total_latency += latency
        self.endpoint_counts[f"{method} {path}"] += 1
        
        if status >= 400:
            self.total_errors += 1
        
        if latency > 1.0:
            self.logger.warning(f"Slow request: {method} {path} took {latency:.2f}s")
    
    def get_stats(self) -> dict:
        """Get current statistics."""
        uptime = time.time() - self.start_time
        return {
            "uptime_seconds": round(uptime, 2),
            "total_requests": self.total_requests,
            "total_errors": self.total_errors,
            "error_rate": round(self.total_errors / max(self.total_requests, 1) * 100, 2),
            "avg_latency_ms": round(self.total_latency / max(self.total_requests, 1) * 1000, 2),
            "requests_per_minute": round(self.total_requests / max(uptime / 60, 1), 2),
            "endpoints": dict(self.endpoint_counts)
        }


@contextmanager
def timed_operation(logger: logging.Logger, operation: str):
    """Context manager for timing operations."""
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        logger.info(f"{operation} completed in {duration:.2f}s")


def log_request(logger: logging.Logger):
    """Decorator to log function calls."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"Calling {func.__name__}")
            try:
                result = func(*args, **kwargs)
                logger.info(f"{func.__name__} completed successfully")
                return result
            except Exception as e:
                logger.error(f"{func.__name__} failed: {str(e)}")
                raise
        return wrapper
    return decorator
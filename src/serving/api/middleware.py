"""
API Middleware

Production middleware for:
- Request logging
- Rate limiting
- Security headers
"""

import time
from collections import defaultdict
from typing import Callable, Dict
import asyncio

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
import structlog

logger = structlog.get_logger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log all requests with timing information"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.perf_counter()
        
        # Generate request ID
        request_id = request.headers.get("X-Request-ID", str(time.time_ns()))
        
        # Log request
        logger.info(
            "Request started",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            client=request.client.host if request.client else None,
        )
        
        # Process request
        response = await call_next(request)
        
        # Calculate duration
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        # Log response
        logger.info(
            "Request completed",
            request_id=request_id,
            status_code=response.status_code,
            duration_ms=round(duration_ms, 2),
        )
        
        # Add timing header
        response.headers["X-Response-Time"] = f"{duration_ms:.2f}ms"
        response.headers["X-Request-ID"] = request_id
        
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Simple in-memory rate limiter.
    
    For production, use Redis-based rate limiting.
    """
    
    def __init__(
        self,
        app,
        max_requests: int = 100,
        window_seconds: int = 60,
    ):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: Dict[str, list] = defaultdict(list)
        self._lock = asyncio.Lock()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Get client identifier
        client_id = request.client.host if request.client else "unknown"
        
        # Check rate limit
        current_time = time.time()
        
        async with self._lock:
            # Remove old requests outside window
            self._requests[client_id] = [
                t for t in self._requests[client_id]
                if current_time - t < self.window_seconds
            ]
            
            # Check if over limit
            if len(self._requests[client_id]) >= self.max_requests:
                logger.warning(
                    "Rate limit exceeded",
                    client=client_id,
                    requests=len(self._requests[client_id]),
                )
                return Response(
                    content='{"error": "Rate limit exceeded"}',
                    status_code=429,
                    media_type="application/json",
                    headers={
                        "Retry-After": str(self.window_seconds),
                        "X-RateLimit-Limit": str(self.max_requests),
                        "X-RateLimit-Remaining": "0",
                    },
                )
            
            # Record request
            self._requests[client_id].append(current_time)
            remaining = self.max_requests - len(self._requests[client_id])
        
        # Process request
        response = await call_next(request)
        
        # Add rate limit headers
        response.headers["X-RateLimit-Limit"] = str(self.max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        
        # Security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Content-Security-Policy"] = "default-src 'self'"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        return response

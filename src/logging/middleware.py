"""
FastAPI Logging Middleware

Automatic request/response logging with correlation tracking
and performance monitoring.
"""

import time
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from .correlation import (
    CorrelationContext,
    extract_correlation_from_headers,
    extract_trace_from_headers,
)
from .structured_logger import get_logger


class LoggingMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for request/response logging."""

    def __init__(
        self,
        app,
        logger_name: str = "ecap.http",
        exclude_paths: Optional[list] = None,
        log_request_body: bool = False,
        log_response_body: bool = False,
        max_body_size: int = 1024,
    ):
        super().__init__(app)
        self.logger = get_logger(logger_name)
        self.exclude_paths = exclude_paths or ["/health", "/metrics", "/favicon.ico"]
        self.log_request_body = log_request_body
        self.log_response_body = log_response_body
        self.max_body_size = max_body_size

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Process request and response with logging."""
        # Skip logging for excluded paths
        if request.url.path in self.exclude_paths:
            return await call_next(request)

        start_time = time.time()

        # Extract or generate correlation ID
        correlation_id = extract_correlation_from_headers(dict(request.headers))
        trace_context = extract_trace_from_headers(dict(request.headers))

        # Create correlation context
        with CorrelationContext(
            correlation_id=correlation_id,
            trace_id=trace_context.trace_id if trace_context else None,
            span_id=trace_context.span_id if trace_context else None,
            service_name="ecap-api",
            operation_name=f"{request.method} {request.url.path}",
        ) as ctx:
            # Log request
            await self._log_request(request, ctx)

            # Process request
            try:
                response = await call_next(request)

                # Calculate duration
                duration_ms = (time.time() - start_time) * 1000

                # Add correlation headers to response
                for header, value in ctx.get_trace_headers().items():
                    response.headers[header] = value

                # Log response
                await self._log_response(request, response, duration_ms, ctx)

                return response

            except Exception as exc:
                duration_ms = (time.time() - start_time) * 1000

                # Log exception
                self.logger.error(
                    f"Request failed: {exc}",
                    exception_type=type(exc).__name__,
                    exception_message=str(exc),
                    http_method=request.method,
                    http_path=str(request.url.path),
                    http_query=str(request.url.query),
                    duration_ms=duration_ms,
                    user_agent=request.headers.get("user-agent"),
                    client_ip=self._get_client_ip(request),
                )

                raise  # Re-raise the exception

    async def _log_request(self, request: Request, ctx: CorrelationContext):
        """Log incoming request details."""
        # Base request info
        log_data = {
            "http_method": request.method,
            "http_path": str(request.url.path),
            "http_query": str(request.url.query) if request.url.query else None,
            "http_scheme": request.url.scheme,
            "http_version": request.scope.get("http_version"),
            "user_agent": request.headers.get("user-agent"),
            "client_ip": self._get_client_ip(request),
            "content_type": request.headers.get("content-type"),
            "content_length": request.headers.get("content-length"),
        }

        # Add authentication info if available
        if "authorization" in request.headers:
            auth_type = request.headers["authorization"].split(" ")[0]
            log_data["auth_type"] = auth_type

        # Log request body if enabled and reasonable size
        if self.log_request_body and request.headers.get("content-length"):
            content_length = int(request.headers["content-length"])
            if content_length <= self.max_body_size:
                try:
                    body = await request.body()
                    if body:
                        log_data["request_body"] = body.decode(
                            "utf-8", errors="ignore"
                        )[: self.max_body_size]
                except Exception:
                    log_data["request_body"] = "<failed to read>"

        self.logger.info(f"→ {request.method} {request.url.path}", **log_data)

    async def _log_response(
        self,
        request: Request,
        response: Response,
        duration_ms: float,
        ctx: CorrelationContext,
    ):
        """Log outgoing response details."""
        # Determine log level based on status code
        if response.status_code >= 500:
            log_level = "error"
        elif response.status_code >= 400:
            log_level = "warning"
        else:
            log_level = "info"

        log_data = {
            "http_status_code": response.status_code,
            "duration_ms": round(duration_ms, 2),
            "response_size": response.headers.get("content-length"),
            "cache_status": response.headers.get("x-cache-status"),
        }

        # Add performance classifications
        if duration_ms > 5000:
            log_data["performance"] = "very_slow"
        elif duration_ms > 2000:
            log_data["performance"] = "slow"
        elif duration_ms > 1000:
            log_data["performance"] = "acceptable"
        else:
            log_data["performance"] = "fast"

        # Log response body if enabled (only for errors or small responses)
        if self.log_response_body and (
            response.status_code >= 400
            or (
                response.headers.get("content-length")
                and int(response.headers["content-length"]) <= self.max_body_size
            )
        ):
            # Note: Can't easily read response body without intercepting it
            # This would require a more complex implementation
            pass

        message = f"← {request.method} {request.url.path} {response.status_code} {duration_ms:.1f}ms"

        getattr(self.logger, log_level)(message, **log_data)

    def _get_client_ip(self, request: Request) -> str:
        """Get client IP address, handling proxies."""
        # Check for forwarded headers (proxy/load balancer)
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            # Take the first IP in the chain
            return forwarded_for.split(",")[0].strip()

        # Check for real IP header
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        # Fall back to direct connection
        client_host = request.client.host if request.client else "unknown"
        return client_host


class PerformanceLoggingMiddleware(BaseHTTPMiddleware):
    """Specialized middleware for performance monitoring."""

    def __init__(
        self,
        app,
        slow_request_threshold: float = 1000.0,  # milliseconds
        very_slow_threshold: float = 5000.0,
    ):
        super().__init__(app)
        self.logger = get_logger("ecap.performance")
        self.slow_threshold = slow_request_threshold
        self.very_slow_threshold = very_slow_threshold

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Monitor request performance."""
        start_time = time.time()

        try:
            response = await call_next(request)
            duration_ms = (time.time() - start_time) * 1000

            # Log slow requests
            if duration_ms > self.slow_threshold:
                severity = (
                    "critical" if duration_ms > self.very_slow_threshold else "warning"
                )

                getattr(self.logger, severity)(
                    f"Slow request detected: {request.method} {request.url.path}",
                    duration_ms=duration_ms,
                    threshold_exceeded=self.very_slow_threshold
                    if duration_ms > self.very_slow_threshold
                    else self.slow_threshold,
                    http_method=request.method,
                    http_path=str(request.url.path),
                    http_status=response.status_code,
                    client_ip=request.client.host if request.client else "unknown",
                )

            return response

        except Exception as exc:
            duration_ms = (time.time() - start_time) * 1000

            self.logger.error(
                f"Request failed after {duration_ms:.1f}ms",
                duration_ms=duration_ms,
                exception_type=type(exc).__name__,
                exception_message=str(exc),
                http_method=request.method,
                http_path=str(request.url.path),
            )

            raise


class SecurityLoggingMiddleware(BaseHTTPMiddleware):
    """Security-focused logging middleware."""

    def __init__(self, app):
        super().__init__(app)
        self.logger = get_logger("ecap.security")

        # Suspicious patterns
        self.suspicious_patterns = [
            "script>",
            "javascript:",
            "eval(",
            "union select",
            "drop table",
            "../",
            "..\\",
            "cmd.exe",
            "/etc/passwd",
        ]

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Monitor for security issues."""
        # Check for suspicious patterns in URL
        url_str = str(request.url)
        for pattern in self.suspicious_patterns:
            if pattern in url_str.lower():
                self.logger.warning(
                    f"Suspicious request pattern detected: {pattern}",
                    http_method=request.method,
                    http_path=str(request.url.path),
                    http_query=str(request.url.query),
                    suspicious_pattern=pattern,
                    client_ip=request.client.host if request.client else "unknown",
                    user_agent=request.headers.get("user-agent"),
                )
                break

        # Log authentication failures
        response = await call_next(request)

        if response.status_code == 401:
            self.logger.warning(
                "Authentication failure",
                http_method=request.method,
                http_path=str(request.url.path),
                client_ip=request.client.host if request.client else "unknown",
                user_agent=request.headers.get("user-agent"),
            )
        elif response.status_code == 403:
            self.logger.warning(
                "Authorization failure",
                http_method=request.method,
                http_path=str(request.url.path),
                client_ip=request.client.host if request.client else "unknown",
                user_agent=request.headers.get("user-agent"),
            )

        return response

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from src.custom_logging.correlation import (
    CorrelationContext,
    extract_trace_from_headers,
    generate_correlation_id,
    set_correlation_id,
    set_trace_context,
)
from src.custom_logging.structured_logger import get_logger

logger = get_logger(__name__)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        correlation_id = (
            request.headers.get("X-Correlation-ID") or generate_correlation_id()
        )
        set_correlation_id(correlation_id)

        trace_context = extract_trace_from_headers(request.headers)
        if trace_context:
            set_trace_context(trace_context)
        else:
            # If no trace context in headers, create a new one based on correlation_id
            with CorrelationContext(
                correlation_id=correlation_id,
                service_name="api-service",
                operation_name=f"{request.method} {request.url.path}",
            ) as ctx:
                set_trace_context(ctx.trace_context)

        response = await call_next(request)

        # Add correlation ID to response headers
        response.headers["X-Correlation-ID"] = correlation_id
        if trace_context:
            response.headers["X-Trace-ID"] = trace_context.trace_id
            response.headers["X-Span-ID"] = trace_context.span_id

        return response

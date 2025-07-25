"""
Request Correlation and Tracing

Provides correlation ID tracking across service boundaries
for distributed request tracing.
"""

import uuid
from typing import Optional
from contextvars import ContextVar
import threading
from dataclasses import dataclass
import time


# Context variable for correlation ID (async-safe)
_correlation_context: ContextVar[Optional[str]] = ContextVar(
    'correlation_id', 
    default=None
)

# Thread-local storage for non-async contexts
_thread_local = threading.local()


@dataclass
class TraceContext:
    """Extended tracing context with timing and metadata."""
    
    correlation_id: str
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    start_time: float = None
    service_name: str = "unknown"
    operation_name: str = "unknown"
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = time.time()


class CorrelationContext:
    """Context manager for correlation ID tracking."""
    
    def __init__(self, correlation_id: Optional[str] = None, 
                 trace_id: Optional[str] = None,
                 span_id: Optional[str] = None,
                 service_name: str = "ecap-service",
                 operation_name: str = "operation"):
        
        self.correlation_id = correlation_id or generate_correlation_id()
        self.trace_id = trace_id or self.correlation_id
        self.span_id = span_id or generate_span_id()
        self.service_name = service_name
        self.operation_name = operation_name
        
        self.trace_context = TraceContext(
            correlation_id=self.correlation_id,
            trace_id=self.trace_id,
            span_id=self.span_id,
            service_name=service_name,
            operation_name=operation_name
        )
        
        self.previous_correlation_id = None
        self.previous_trace_context = None
    
    def __enter__(self):
        # Store previous values
        self.previous_correlation_id = get_correlation_id()
        self.previous_trace_context = get_trace_context()
        
        # Set new values
        set_correlation_id(self.correlation_id)
        set_trace_context(self.trace_context)
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore previous values
        if self.previous_correlation_id:
            set_correlation_id(self.previous_correlation_id)
        else:
            clear_correlation_id()
            
        if self.previous_trace_context:
            set_trace_context(self.previous_trace_context)
        else:
            clear_trace_context()
    
    def create_child_span(self, operation_name: str) -> 'CorrelationContext':
        """Create a child span for nested operations."""
        return CorrelationContext(
            correlation_id=self.correlation_id,
            trace_id=self.trace_id,
            span_id=generate_span_id(),
            service_name=self.service_name,
            operation_name=operation_name
        )
    
    def get_duration_ms(self) -> float:
        """Get duration of current span in milliseconds."""
        return (time.time() - self.trace_context.start_time) * 1000
    
    def get_trace_headers(self) -> dict:
        """Get HTTP headers for trace propagation."""
        return {
            "X-Correlation-ID": self.correlation_id,
            "X-Trace-ID": self.trace_id,
            "X-Span-ID": self.span_id,
            "X-Parent-Span-ID": self.trace_context.parent_span_id or "",
        }


def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())


def generate_span_id() -> str:
    """Generate a new span ID."""
    return str(uuid.uuid4())[:8]


def generate_trace_id() -> str:
    """Generate a new trace ID."""
    return str(uuid.uuid4())


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID."""
    try:
        # Try async context first
        return _correlation_context.get()
    except LookupError:
        # Fall back to thread-local storage
        return getattr(_thread_local, 'correlation_id', None)


def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID for the current context."""
    # Set in async context
    _correlation_context.set(correlation_id)
    
    # Set in thread-local storage as backup
    _thread_local.correlation_id = correlation_id


def clear_correlation_id() -> None:
    """Clear the correlation ID from the current context."""
    _correlation_context.set(None)
    
    if hasattr(_thread_local, 'correlation_id'):
        delattr(_thread_local, 'correlation_id')


def get_trace_context() -> Optional[TraceContext]:
    """Get the current trace context."""
    return getattr(_thread_local, 'trace_context', None)


def set_trace_context(trace_context: TraceContext) -> None:
    """Set the trace context for the current context."""
    _thread_local.trace_context = trace_context


def clear_trace_context() -> None:
    """Clear the trace context from the current context."""
    if hasattr(_thread_local, 'trace_context'):
        delattr(_thread_local, 'trace_context')


def extract_correlation_from_headers(headers: dict) -> Optional[str]:
    """Extract correlation ID from HTTP headers."""
    # Try various header names (case-insensitive)
    possible_headers = [
        "X-Correlation-ID",
        "X-Correlation-Id", 
        "X-Request-ID",
        "X-Request-Id",
        "Correlation-ID",
        "Request-ID"
    ]
    
    # Convert headers to lowercase for case-insensitive lookup
    lower_headers = {k.lower(): v for k, v in headers.items()}
    
    for header in possible_headers:
        value = lower_headers.get(header.lower())
        if value:
            return value
    
    return None


def extract_trace_from_headers(headers: dict) -> Optional[TraceContext]:
    """Extract full trace context from HTTP headers."""
    correlation_id = extract_correlation_from_headers(headers)
    if not correlation_id:
        return None
    
    lower_headers = {k.lower(): v for k, v in headers.items()}
    
    trace_id = lower_headers.get("x-trace-id", correlation_id)
    span_id = lower_headers.get("x-span-id", generate_span_id())
    parent_span_id = lower_headers.get("x-parent-span-id")
    
    return TraceContext(
        correlation_id=correlation_id,
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id if parent_span_id else None
    )


def ensure_correlation_id() -> str:
    """Ensure a correlation ID exists, creating one if needed."""
    correlation_id = get_correlation_id()
    if not correlation_id:
        correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)
    return correlation_id


def with_correlation(correlation_id: Optional[str] = None):
    """Decorator to ensure correlation ID is set for a function."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with CorrelationContext(correlation_id):
                return func(*args, **kwargs)
        return wrapper
    return decorator
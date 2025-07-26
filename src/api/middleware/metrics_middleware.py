"""
Prometheus metrics middleware for FastAPI applications.

This middleware collects and exposes application performance metrics
for monitoring and alerting purposes.
"""

import time
from typing import Callable

from fastapi import Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

from src.logging.structured_logger import get_logger

logger = get_logger(__name__)

# Prometheus metrics
http_requests_total = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"]
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint", "status"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

http_requests_in_progress = Gauge(
    "http_requests_in_progress",
    "Number of HTTP requests currently being processed",
    ["method", "endpoint"],
)

# Application-specific metrics
database_connections_active = Gauge(
    "database_connections_active", "Number of active database connections"
)

cache_hits_total = Counter("cache_hits_total", "Total cache hits", ["cache_type"])

cache_misses_total = Counter("cache_misses_total", "Total cache misses", ["cache_type"])

kafka_messages_produced_total = Counter(
    "kafka_messages_produced_total", "Total Kafka messages produced", ["topic"]
)

kafka_messages_consumed_total = Counter(
    "kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    ["topic", "consumer_group"],
)

analytics_jobs_total = Counter(
    "analytics_jobs_total", "Total analytics jobs executed", ["job_type", "status"]
)

analytics_job_duration_seconds = Histogram(
    "analytics_job_duration_seconds",
    "Analytics job duration in seconds",
    ["job_type"],
    buckets=[1, 5, 10, 30, 60, 300, 600, 1800],
)

# Custom business metrics
user_registrations_total = Counter(
    "user_registrations_total", "Total user registrations"
)

transactions_total = Counter(
    "transactions_total", "Total transactions processed", ["status", "payment_method"]
)

fraud_alerts_total = Counter(
    "fraud_alerts_total", "Total fraud alerts generated", ["alert_type", "severity"]
)


class PrometheusMetricsMiddleware(BaseHTTPMiddleware):
    """Middleware to collect Prometheus metrics for HTTP requests."""

    def __init__(self, app, **kwargs):
        super().__init__(app, **kwargs)
        self.exclude_paths = {"/metrics", "/health", "/docs", "/redoc", "/openapi.json"}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and collect metrics."""
        # Skip metrics collection for certain paths
        if request.url.path in self.exclude_paths:
            return await call_next(request)

        method = request.method
        path = request.url.path

        # Normalize path for metrics (remove IDs and query params)
        endpoint = self._normalize_path(path)

        # Track request in progress
        http_requests_in_progress.labels(method=method, endpoint=endpoint).inc()

        start_time = time.time()

        try:
            # Process request
            response = await call_next(request)
            status = str(response.status_code)

        except Exception as e:
            logger.error(f"Request failed: {e}", exc_info=True)
            status = "500"
            response = StarletteResponse(status_code=500)

        finally:
            # Calculate duration
            duration = time.time() - start_time

            # Update metrics
            http_requests_total.labels(
                method=method, endpoint=endpoint, status=status
            ).inc()

            http_request_duration_seconds.labels(
                method=method, endpoint=endpoint, status=status
            ).observe(duration)

            http_requests_in_progress.labels(method=method, endpoint=endpoint).dec()

            # Log performance metrics
            logger.info(
                "Request processed",
                extra={
                    "method": method,
                    "endpoint": endpoint,
                    "status": status,
                    "duration": duration,
                    "metrics_collected": True,
                },
            )

        return response

    def _normalize_path(self, path: str) -> str:
        """Normalize URL path for metrics to avoid high cardinality."""
        # Remove UUIDs and IDs from paths
        import re

        # Replace UUIDs
        path = re.sub(
            r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            "/{uuid}",
            path,
        )

        # Replace numeric IDs
        path = re.sub(r"/\d+(?=/|$)", "/{id}", path)

        # Replace other common ID patterns
        path = re.sub(r"/[a-zA-Z0-9_-]{20,}", "/{id}", path)

        return path


def get_metrics_response() -> StarletteResponse:
    """Generate Prometheus metrics response."""
    metrics_data = generate_latest()
    return StarletteResponse(content=metrics_data, media_type=CONTENT_TYPE_LATEST)


# Helper functions for updating application metrics
def record_database_connection(active_connections: int):
    """Record current active database connections."""
    database_connections_active.set(active_connections)


def record_cache_hit(cache_type: str):
    """Record a cache hit."""
    cache_hits_total.labels(cache_type=cache_type).inc()


def record_cache_miss(cache_type: str):
    """Record a cache miss."""
    cache_misses_total.labels(cache_type=cache_type).inc()


def record_kafka_message_produced(topic: str):
    """Record a Kafka message production."""
    kafka_messages_produced_total.labels(topic=topic).inc()


def record_kafka_message_consumed(topic: str, consumer_group: str):
    """Record a Kafka message consumption."""
    kafka_messages_consumed_total.labels(
        topic=topic, consumer_group=consumer_group
    ).inc()


def record_analytics_job(job_type: str, status: str, duration: float):
    """Record analytics job execution."""
    analytics_jobs_total.labels(job_type=job_type, status=status).inc()
    analytics_job_duration_seconds.labels(job_type=job_type).observe(duration)


def record_user_registration():
    """Record a user registration."""
    user_registrations_total.inc()


def record_transaction(status: str, payment_method: str):
    """Record a transaction."""
    transactions_total.labels(status=status, payment_method=payment_method).inc()


def record_fraud_alert(alert_type: str, severity: str):
    """Record a fraud alert."""
    fraud_alerts_total.labels(alert_type=alert_type, severity=severity).inc()

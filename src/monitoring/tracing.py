"""
Distributed tracing configuration using OpenTelemetry and Jaeger.

This module provides comprehensive tracing capabilities for the ECAP platform,
enabling end-to-end request tracking across all services.
"""

import os
from typing import Optional

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from src.custom_logging.structured_logger import get_logger

logger = get_logger(__name__)


class ECAPTracing:
    """ECAP distributed tracing manager."""

    def __init__(
        self,
        service_name: str = "ecap-api",
        service_version: str = "1.0.0",
        jaeger_endpoint: Optional[str] = None,
        environment: str = "development",
    ):
        self.service_name = service_name
        self.service_version = service_version
        self.environment = environment
        self.jaeger_endpoint = jaeger_endpoint or os.getenv(
            "JAEGER_ENDPOINT", "http://localhost:14268/api/traces"
        )
        self._tracer = None

    def initialize_tracing(self) -> None:
        """Initialize OpenTelemetry tracing with Jaeger exporter."""
        try:
            # Create resource with service information
            resource = Resource.create(
                {
                    "service.name": self.service_name,
                    "service.version": self.service_version,
                    "service.instance.id": os.getenv("HOSTNAME", "unknown"),
                    "deployment.environment": self.environment,
                }
            )

            # Create tracer provider
            trace.set_tracer_provider(TracerProvider(resource=resource))

            # Create Jaeger exporter
            jaeger_exporter = JaegerExporter(
                agent_host_name=os.getenv("JAEGER_AGENT_HOST", "localhost"),
                agent_port=int(os.getenv("JAEGER_AGENT_PORT", "6831")),
                collector_endpoint=self.jaeger_endpoint,
            )

            # Create span processor and add to tracer provider
            span_processor = BatchSpanProcessor(jaeger_exporter)
            trace.get_tracer_provider().add_span_processor(span_processor)

            # Get tracer
            self._tracer = trace.get_tracer(__name__)

            logger.info(
                "Tracing initialized",
                extra={
                    "service_name": self.service_name,
                    "service_version": self.service_version,
                    "jaeger_endpoint": self.jaeger_endpoint,
                    "environment": self.environment,
                },
            )

        except Exception as e:
            logger.error(f"Failed to initialize tracing: {e}", exc_info=True)
            raise

    def instrument_fastapi(self, app) -> None:
        """Instrument FastAPI application for tracing."""
        try:
            FastAPIInstrumentor.instrument_app(
                app,
                tracer_provider=trace.get_tracer_provider(),
                excluded_urls="health,metrics,docs,redoc,openapi.json",
                request_hook=self._request_hook,
                response_hook=self._response_hook,
            )
            logger.info("FastAPI instrumentation enabled")

        except Exception as e:
            logger.error(f"Failed to instrument FastAPI: {e}", exc_info=True)

    def instrument_database(self) -> None:
        """Instrument database connections for tracing."""
        try:
            Psycopg2Instrumentor().instrument(
                tracer_provider=trace.get_tracer_provider(),
                enable_commenter=True,
                commenter_options={
                    "db_driver": True,
                    "dbapi_threadsafety": True,
                    "dbapi_level": True,
                    "libpq_version": True,
                    "driver_paramstyle": True,
                },
            )
            logger.info("Database instrumentation enabled")

        except Exception as e:
            logger.error(f"Failed to instrument database: {e}", exc_info=True)

    def instrument_redis(self) -> None:
        """Instrument Redis connections for tracing."""
        try:
            RedisInstrumentor().instrument(tracer_provider=trace.get_tracer_provider())
            logger.info("Redis instrumentation enabled")

        except Exception as e:
            logger.error(f"Failed to instrument Redis: {e}", exc_info=True)

    def instrument_kafka(self) -> None:
        """Instrument Kafka for tracing."""
        try:
            KafkaInstrumentor().instrument(tracer_provider=trace.get_tracer_provider())
            logger.info("Kafka instrumentation enabled")

        except Exception as e:
            logger.error(f"Failed to instrument Kafka: {e}", exc_info=True)

    def instrument_logging(self) -> None:
        """Instrument logging to include trace information."""
        try:
            LoggingInstrumentor().instrument(
                tracer_provider=trace.get_tracer_provider(), set_logging_format=True
            )
            logger.info("Logging instrumentation enabled")

        except Exception as e:
            logger.error(f"Failed to instrument logging: {e}", exc_info=True)

    def get_tracer(self) -> trace.Tracer:
        """Get the configured tracer instance."""
        if self._tracer is None:
            raise RuntimeError(
                "Tracing not initialized. Call initialize_tracing() first."
            )
        return self._tracer

    def _request_hook(self, span, scope):
        """Hook called for each incoming request."""
        if span and span.is_recording():
            # Add custom attributes to spans
            request = scope.get("fastapi_request")
            if request:
                span.set_attribute(
                    "http.client_ip",
                    request.client.host if request.client else "unknown",
                )
                span.set_attribute(
                    "http.user_agent", request.headers.get("user-agent", "unknown")
                )

                # Add correlation ID if available
                correlation_id = request.headers.get("x-correlation-id")
                if correlation_id:
                    span.set_attribute("correlation.id", correlation_id)

    def _response_hook(self, span, message):
        """Hook called for each outgoing response."""
        if span and span.is_recording():
            # Add response-specific attributes
            response = message.get("response")
            if response:
                span.set_attribute("http.response.size", len(response.get("body", b"")))


# Global tracing instance
_tracing_instance: Optional[ECAPTracing] = None


def initialize_tracing(
    service_name: str = "ecap-api",
    service_version: str = "1.0.0",
    jaeger_endpoint: Optional[str] = None,
    environment: str = "development",
) -> ECAPTracing:
    """Initialize global tracing instance."""
    global _tracing_instance

    if _tracing_instance is None:
        _tracing_instance = ECAPTracing(
            service_name=service_name,
            service_version=service_version,
            jaeger_endpoint=jaeger_endpoint,
            environment=environment,
        )
        _tracing_instance.initialize_tracing()

    return _tracing_instance


def get_tracing_instance() -> Optional[ECAPTracing]:
    """Get the global tracing instance."""
    return _tracing_instance


def instrument_all(
    app,
    enable_database: bool = True,
    enable_redis: bool = True,
    enable_kafka: bool = True,
):
    """Instrument all supported components for tracing."""
    tracing = get_tracing_instance()
    if not tracing:
        raise RuntimeError("Tracing not initialized. Call initialize_tracing() first.")

    # Instrument FastAPI
    tracing.instrument_fastapi(app)

    # Instrument other components based on availability
    if enable_database:
        tracing.instrument_database()

    if enable_redis:
        tracing.instrument_redis()

    if enable_kafka:
        tracing.instrument_kafka()

    # Always instrument logging
    tracing.instrument_logging()


# Convenience functions for manual tracing
def start_span(name: str, attributes: Optional[dict] = None):
    """Start a new span for manual tracing."""
    tracing = get_tracing_instance()
    if not tracing:
        return None

    tracer = tracing.get_tracer()
    span = tracer.start_span(name)

    if attributes and span.is_recording():
        for key, value in attributes.items():
            span.set_attribute(key, value)

    return span


def add_span_attributes(span, attributes: dict):
    """Add attributes to an existing span."""
    if span and span.is_recording():
        for key, value in attributes.items():
            span.set_attribute(key, value)


def record_exception(span, exception: Exception):
    """Record an exception in a span."""
    if span and span.is_recording():
        span.record_exception(exception)
        span.set_status(trace.Status(trace.StatusCode.ERROR, str(exception)))

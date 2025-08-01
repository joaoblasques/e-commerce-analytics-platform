"""
ECAP Centralized Logging System

Comprehensive logging infrastructure providing:
- Structured JSON logging
- Request correlation and tracing
- ELK stack integration
- Multi-environment configuration
- Performance monitoring
"""

from .correlation import (
    CorrelationContext,
    clear_correlation_id,
    get_correlation_id,
    set_correlation_id,
)
from .formatters import ECSFormatter, JSONFormatter
from .handlers import AsyncQueueHandler, ElasticsearchHandler, FileRotatingHandler
from .middleware import LoggingMiddleware
from .structured_logger import LogConfig, StructuredLogger, get_logger, setup_logging

__all__ = [
    # Core logging
    "get_logger",
    "setup_logging",
    "LogConfig",
    "StructuredLogger",
    # Correlation and tracing
    "CorrelationContext",
    "get_correlation_id",
    "set_correlation_id",
    "clear_correlation_id",
    # FastAPI integration
    "LoggingMiddleware",
    # Formatters
    "JSONFormatter",
    "ECSFormatter",
    # Handlers
    "ElasticsearchHandler",
    "FileRotatingHandler",
    "AsyncQueueHandler",
]

# Version info
__version__ = "1.0.0"
__author__ = "ECAP Engineering Team"

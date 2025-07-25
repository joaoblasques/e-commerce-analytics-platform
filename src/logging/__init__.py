"""
ECAP Centralized Logging System

Comprehensive logging infrastructure providing:
- Structured JSON logging
- Request correlation and tracing
- ELK stack integration
- Multi-environment configuration
- Performance monitoring
"""

from .structured_logger import get_logger, setup_logging, LogConfig
from .correlation import CorrelationContext, get_correlation_id, set_correlation_id
from .middleware import LoggingMiddleware
from .formatters import JSONFormatter, ECSFormatter
from .handlers import ElasticsearchHandler, FileRotatingHandler

__all__ = [
    # Core logging
    "get_logger",
    "setup_logging", 
    "LogConfig",
    
    # Correlation and tracing
    "CorrelationContext",
    "get_correlation_id",
    "set_correlation_id",
    
    # FastAPI integration
    "LoggingMiddleware",
    
    # Formatters
    "JSONFormatter",
    "ECSFormatter",
    
    # Handlers
    "ElasticsearchHandler", 
    "FileRotatingHandler",
]

# Version info
__version__ = "1.0.0"
__author__ = "ECAP Engineering Team"
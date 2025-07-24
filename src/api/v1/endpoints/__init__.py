"""API v1 endpoints package.

This package contains all API v1 endpoints including enhanced analytics,
customer management, fraud detection, and real-time monitoring endpoints.
"""

# Import all endpoint modules to make them available for router inclusion
from . import (
    analytics,
    analytics_enhanced,
    auth,
    customers,
    customers_enhanced,
    fraud,
    fraud_enhanced,
    health,
    realtime,
)

__all__ = [
    "analytics",
    "analytics_enhanced",
    "auth",
    "customers",
    "customers_enhanced",
    "fraud",
    "fraud_enhanced",
    "health",
    "realtime",
]

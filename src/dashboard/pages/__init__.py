"""
Dashboard pages package.

This package contains all dashboard page modules for the E-Commerce Analytics Platform.
"""

# Import all page modules to make them available for import
from . import (
    customer_analytics,
    executive_dashboard,
    fraud_detection,
    operational_dashboard,
    real_time_monitoring,
    revenue_analytics,
)

__all__ = [
    "customer_analytics",
    "executive_dashboard",
    "fraud_detection",
    "operational_dashboard",
    "real_time_monitoring",
    "revenue_analytics",
]

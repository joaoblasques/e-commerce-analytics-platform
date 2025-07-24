"""
Dashboard utilities package.

This package provides utility functions for API communication, authentication,
and data processing for the Streamlit dashboard.
"""

from .api_client import APIClient, APIError, cached_api_call, handle_api_error
from .auth import (
    check_authentication,
    get_current_user,
    login_form,
    logout,
    require_auth,
    verify_credentials,
)
from .data_processing import (
    cached_data_processing,
    calculate_growth_rate,
    calculate_kpi_metrics,
    format_currency,
    format_large_number,
    format_number,
    format_percentage,
    get_growth_indicator,
    prepare_alert_data,
    prepare_revenue_breakdown,
    prepare_segment_data,
    process_time_series_data,
)

__all__ = [
    # API Client
    "APIClient",
    "APIError",
    "cached_api_call",
    "handle_api_error",
    # Authentication
    "check_authentication",
    "login_form",
    "logout",
    "get_current_user",
    "require_auth",
    "verify_credentials",
    # Data Processing
    "format_currency",
    "format_percentage",
    "format_number",
    "format_large_number",
    "calculate_growth_rate",
    "get_growth_indicator",
    "process_time_series_data",
    "prepare_segment_data",
    "prepare_revenue_breakdown",
    "calculate_kpi_metrics",
    "prepare_alert_data",
    "cached_data_processing",
]

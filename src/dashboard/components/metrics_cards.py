"""
Metrics cards component for displaying KPIs.

This module provides reusable metric card components for displaying
key performance indicators with formatting and trend indicators.
"""

from typing import Any, Dict, List, Optional, Union

import streamlit as st

from dashboard.utils.data_processing import (
    format_currency,
    format_large_number,
    format_number,
    format_percentage,
    get_growth_indicator,
)


def render_metric_card(
    title: str,
    value: Union[int, float, str],
    delta: Optional[Union[int, float]] = None,
    delta_color: str = "normal",
    format_type: str = "number",
    help_text: Optional[str] = None,
    prefix: str = "",
    suffix: str = "",
) -> None:
    """
    Render a single metric card.

    Args:
        title: Metric title
        value: Metric value
        delta: Change from previous period
        delta_color: Color for delta ("normal", "inverse", "off")
        format_type: How to format the value ("number", "currency", "percentage", "large_number")
        help_text: Optional help text
        prefix: Text to show before the value
        suffix: Text to show after the value
    """
    # Format the value based on type
    if format_type == "currency":
        formatted_value = format_currency(value)
    elif format_type == "percentage":
        formatted_value = format_percentage(value)
    elif format_type == "large_number":
        formatted_value = format_large_number(value)
    elif format_type == "number":
        formatted_value = format_number(value)
    else:
        formatted_value = str(value)

    # Add prefix and suffix
    display_value = f"{prefix}{formatted_value}{suffix}"

    # Format delta if provided
    formatted_delta = None
    if delta is not None:
        if format_type == "percentage":
            formatted_delta = format_percentage(delta)
        else:
            formatted_delta = format_number(delta, decimals=1)

    # Render the metric
    st.metric(
        label=title,
        value=display_value,
        delta=formatted_delta,
        delta_color=delta_color,
        help=help_text,
    )


def render_kpi_row(metrics: List[Dict[str, Any]], columns: int = 4) -> None:
    """
    Render a row of KPI metrics.

    Args:
        metrics: List of metric dictionaries
        columns: Number of columns to display
    """
    if not metrics:
        return

    cols = st.columns(columns)

    for i, metric in enumerate(metrics[:columns]):
        with cols[i % columns]:
            render_metric_card(**metric)


def render_revenue_metrics(revenue_data: Dict[str, Any]) -> None:
    """
    Render revenue-specific metrics.

    Args:
        revenue_data: Revenue data from API
    """
    if not revenue_data or "summary" not in revenue_data:
        st.warning("No revenue data available")
        return

    summary = revenue_data["summary"]

    metrics = [
        {
            "title": "Total Revenue",
            "value": summary.get("total_revenue", 0),
            "format_type": "currency",
            "help_text": "Total revenue for the selected period",
        },
        {
            "title": "Total Orders",
            "value": summary.get("total_orders", 0),
            "format_type": "large_number",
            "help_text": "Number of orders placed",
        },
        {
            "title": "Average Order Value",
            "value": summary.get("average_order_value", 0),
            "format_type": "currency",
            "help_text": "Average value per order",
        },
        {
            "title": "Growth Rate",
            "value": summary.get("growth_rate", 0),
            "format_type": "percentage",
            "delta_color": "normal",
            "help_text": "Revenue growth compared to previous period",
        },
    ]

    render_kpi_row(metrics)


def render_customer_metrics(customer_data: Dict[str, Any]) -> None:
    """
    Render customer-specific metrics.

    Args:
        customer_data: Customer data from API
    """
    if not customer_data:
        st.warning("No customer data available")
        return

    # Extract metrics from customer data
    total_customers = customer_data.get("total_customers", 0)

    # Calculate metrics from segments if available
    avg_clv = 0
    high_value_customers = 0

    if "segments" in customer_data:
        segments = customer_data["segments"]
        total_clv = sum(
            seg.get("characteristics", {}).get("avg_customer_lifetime_value", 0)
            * seg.get("count", 0)
            for seg in segments
        )
        total_count = sum(seg.get("count", 0) for seg in segments)
        avg_clv = total_clv / total_count if total_count > 0 else 0

        # Count high-value customers (CLV > $2000)
        high_value_customers = sum(
            seg.get("count", 0)
            for seg in segments
            if seg.get("characteristics", {}).get("avg_customer_lifetime_value", 0)
            > 2000
        )

    metrics = [
        {
            "title": "Total Customers",
            "value": total_customers,
            "format_type": "large_number",
            "help_text": "Total number of customers",
        },
        {
            "title": "Average CLV",
            "value": avg_clv,
            "format_type": "currency",
            "help_text": "Average Customer Lifetime Value",
        },
        {
            "title": "High-Value Customers",
            "value": high_value_customers,
            "format_type": "large_number",
            "help_text": "Customers with CLV > $2,000",
        },
        {
            "title": "Retention Rate",
            "value": 0.85,  # This would come from actual data
            "format_type": "percentage",
            "help_text": "Customer retention rate",
        },
    ]

    render_kpi_row(metrics)


def render_fraud_metrics(fraud_data: Dict[str, Any]) -> None:
    """
    Render fraud detection metrics.

    Args:
        fraud_data: Fraud data from API
    """
    if not fraud_data or "overview" not in fraud_data:
        st.warning("No fraud data available")
        return

    overview = fraud_data["overview"]

    metrics = [
        {
            "title": "Total Alerts",
            "value": overview.get("total_alerts", 0),
            "format_type": "number",
            "help_text": "Total fraud alerts generated",
        },
        {
            "title": "Fraud Rate",
            "value": overview.get("fraud_rate", 0),
            "format_type": "percentage",
            "delta_color": "inverse",
            "help_text": "Percentage of transactions flagged as fraudulent",
        },
        {
            "title": "Amount Blocked",
            "value": overview.get("total_blocked_amount", 0),
            "format_type": "currency",
            "help_text": "Total amount blocked due to fraud",
        },
        {
            "title": "False Positive Rate",
            "value": overview.get("false_positive_rate", 0),
            "format_type": "percentage",
            "delta_color": "inverse",
            "help_text": "Rate of false positive alerts",
        },
    ]

    render_kpi_row(metrics)


def render_system_metrics(system_data: Dict[str, Any]) -> None:
    """
    Render system health metrics.

    Args:
        system_data: System health data from API
    """
    if not system_data:
        st.warning("No system data available")
        return

    metrics = [
        {
            "title": "API Response Time",
            "value": system_data.get("response_time_ms", 0),
            "format_type": "number",
            "suffix": "ms",
            "help_text": "Average API response time",
        },
        {
            "title": "Throughput",
            "value": system_data.get("throughput_rps", 0),
            "format_type": "large_number",
            "suffix": " RPS",
            "help_text": "Requests per second",
        },
        {
            "title": "Error Rate",
            "value": system_data.get("error_rate", 0),
            "format_type": "percentage",
            "delta_color": "inverse",
            "help_text": "System error rate",
        },
        {
            "title": "Uptime",
            "value": system_data.get("uptime_seconds", 0) / 86400,  # Convert to days
            "format_type": "number",
            "suffix": " days",
            "help_text": "System uptime in days",
        },
    ]

    render_kpi_row(metrics)


def render_business_metrics(business_data: Dict[str, Any]) -> None:
    """
    Render real-time business metrics.

    Args:
        business_data: Business metrics data from API
    """
    if not business_data:
        st.warning("No business data available")
        return

    # Extract sales metrics
    sales = business_data.get("sales", {})
    customer = business_data.get("customer", {})

    metrics = [
        {
            "title": "Current Revenue",
            "value": sales.get("revenue", {}).get("current", 0),
            "format_type": "currency",
            "delta": sales.get("revenue", {}).get("growth_rate", 0),
            "help_text": "Revenue in current period",
        },
        {
            "title": "Active Users",
            "value": customer.get("active_users", {}).get("current", 0),
            "format_type": "large_number",
            "help_text": "Currently active users",
        },
        {
            "title": "Conversion Rate",
            "value": sales.get("orders", {}).get("conversion_rate", 0),
            "format_type": "percentage",
            "help_text": "Current conversion rate",
        },
        {
            "title": "Avg Session Duration",
            "value": customer.get("engagement", {}).get(
                "avg_session_duration_minutes", 0
            ),
            "format_type": "number",
            "suffix": " min",
            "help_text": "Average session duration",
        },
    ]

    render_kpi_row(metrics)


def render_custom_metrics(
    metrics_data: List[Dict[str, Any]], title: str = "Key Metrics"
) -> None:
    """
    Render custom metrics with a title.

    Args:
        metrics_data: List of metric dictionaries
        title: Section title
    """
    if not metrics_data:
        return

    st.subheader(title)

    # Group metrics into rows of 4
    for i in range(0, len(metrics_data), 4):
        row_metrics = metrics_data[i : i + 4]
        render_kpi_row(row_metrics)


def render_comparison_metrics(
    current_data: Dict[str, Any],
    previous_data: Dict[str, Any],
    metric_configs: List[Dict[str, Any]],
) -> None:
    """
    Render metrics with comparison to previous period.

    Args:
        current_data: Current period data
        previous_data: Previous period data
        metric_configs: Configuration for each metric
    """
    metrics = []

    for config in metric_configs:
        key = config["key"]
        current_value = current_data.get(key, 0)
        previous_value = previous_data.get(key, 0)

        # Calculate delta
        delta = None
        if previous_value != 0:
            delta = (current_value - previous_value) / previous_value

        metric = {
            "title": config["title"],
            "value": current_value,
            "delta": delta,
            "format_type": config.get("format_type", "number"),
            "delta_color": config.get("delta_color", "normal"),
            "help_text": config.get("help_text", ""),
        }

        metrics.append(metric)

    render_kpi_row(metrics)


def render_trend_indicator(
    value: float, threshold_good: float = 0.05, threshold_bad: float = -0.05
) -> str:
    """
    Get trend indicator based on value and thresholds.

    Args:
        value: Value to evaluate
        threshold_good: Threshold for good performance
        threshold_bad: Threshold for bad performance

    Returns:
        str: Trend indicator emoji
    """
    if value >= threshold_good:
        return "📈 Excellent"
    elif value > 0:
        return "↗️ Good"
    elif value > threshold_bad:
        return "➡️ Stable"
    else:
        return "📉 Needs Attention"

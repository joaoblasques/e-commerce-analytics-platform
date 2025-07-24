"""
Data processing utilities for the dashboard.

This module provides functions for data transformation, formatting,
and preparation for visualization components.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import streamlit as st


def format_currency(value: Union[int, float], currency: str = "USD") -> str:
    """
    Format a numeric value as currency.

    Args:
        value: Numeric value to format
        currency: Currency code (default: USD)

    Returns:
        str: Formatted currency string
    """
    if pd.isna(value) or value is None:
        return "N/A"

    if currency == "USD":
        return f"${value:,.2f}"
    elif currency == "EUR":
        return f"€{value:,.2f}"
    else:
        return f"{value:,.2f} {currency}"


def format_percentage(value: Union[int, float], decimals: int = 2) -> str:
    """
    Format a numeric value as percentage.

    Args:
        value: Numeric value to format (0.15 = 15%)
        decimals: Number of decimal places

    Returns:
        str: Formatted percentage string
    """
    if pd.isna(value) or value is None:
        return "N/A"

    return f"{value * 100:.{decimals}f}%"


def format_number(value: Union[int, float], decimals: int = 0) -> str:
    """
    Format a numeric value with thousands separators.

    Args:
        value: Numeric value to format
        decimals: Number of decimal places

    Returns:
        str: Formatted number string
    """
    if pd.isna(value) or value is None:
        return "N/A"

    if decimals == 0:
        return f"{int(value):,}"
    else:
        return f"{value:,.{decimals}f}"


def format_large_number(value: Union[int, float]) -> str:
    """
    Format large numbers with K, M, B suffixes.

    Args:
        value: Numeric value to format

    Returns:
        str: Formatted number with suffix
    """
    if pd.isna(value) or value is None:
        return "N/A"

    abs_value = abs(value)
    sign = "-" if value < 0 else ""

    if abs_value >= 1_000_000_000:
        return f"{sign}{abs_value / 1_000_000_000:.1f}B"
    elif abs_value >= 1_000_000:
        return f"{sign}{abs_value / 1_000_000:.1f}M"
    elif abs_value >= 1_000:
        return f"{sign}{abs_value / 1_000:.1f}K"
    else:
        return f"{sign}{abs_value:.0f}"


def calculate_growth_rate(current: float, previous: float) -> float:
    """
    Calculate growth rate between two values.

    Args:
        current: Current period value
        previous: Previous period value

    Returns:
        float: Growth rate as decimal (0.15 = 15% growth)
    """
    if previous == 0 or pd.isna(previous) or pd.isna(current):
        return 0.0

    return (current - previous) / previous


def get_growth_indicator(growth_rate: float) -> str:
    """
    Get growth indicator emoji based on growth rate.

    Args:
        growth_rate: Growth rate as decimal

    Returns:
        str: Emoji indicator
    """
    if growth_rate > 0.05:  # > 5%
        return "📈"
    elif growth_rate > 0:
        return "↗️"
    elif growth_rate < -0.05:  # < -5%
        return "📉"
    elif growth_rate < 0:
        return "↘️"
    else:
        return "➡️"


def process_time_series_data(
    data: List[Dict], date_field: str = "date"
) -> pd.DataFrame:
    """
    Process time series data into a pandas DataFrame.

    Args:
        data: List of dictionaries containing time series data
        date_field: Name of the date field

    Returns:
        pd.DataFrame: Processed time series data
    """
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Convert date field to datetime
    if date_field in df.columns:
        df[date_field] = pd.to_datetime(df[date_field])
        df = df.sort_values(date_field)

    return df


def calculate_moving_average(
    df: pd.DataFrame, column: str, window: int = 7
) -> pd.Series:
    """
    Calculate moving average for a column.

    Args:
        df: DataFrame containing the data
        column: Column name to calculate moving average for
        window: Window size for moving average

    Returns:
        pd.Series: Moving average values
    """
    if column not in df.columns:
        return pd.Series()

    return df[column].rolling(window=window, min_periods=1).mean()


def detect_anomalies(
    df: pd.DataFrame, column: str, threshold: float = 2.0
) -> pd.Series:
    """
    Detect anomalies using z-score method.

    Args:
        df: DataFrame containing the data
        column: Column name to check for anomalies
        threshold: Z-score threshold for anomaly detection

    Returns:
        pd.Series: Boolean series indicating anomalies
    """
    if column not in df.columns or df[column].empty:
        return pd.Series(dtype=bool)

    z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
    return z_scores > threshold


def prepare_segment_data(segments_data: Dict) -> pd.DataFrame:
    """
    Prepare customer segment data for visualization.

    Args:
        segments_data: Dictionary containing segment information

    Returns:
        pd.DataFrame: Processed segment data
    """
    if not segments_data or "segments" not in segments_data:
        return pd.DataFrame()

    segments = []
    for segment in segments_data["segments"]:
        segment_info = {
            "segment": segment["name"],
            "count": segment["count"],
            "percentage": segment["percentage"],
            "avg_clv": segment["characteristics"]["avg_customer_lifetime_value"],
            "avg_orders": segment["characteristics"]["avg_orders_per_customer"],
            "revenue_contribution": segment["characteristics"]["revenue_contribution"],
        }
        segments.append(segment_info)

    return pd.DataFrame(segments)


def prepare_revenue_breakdown(revenue_data: Dict) -> Dict[str, pd.DataFrame]:
    """
    Prepare revenue breakdown data for visualization.

    Args:
        revenue_data: Dictionary containing revenue breakdown

    Returns:
        Dict[str, pd.DataFrame]: Breakdown data by category
    """
    breakdowns = {}

    if "breakdown" in revenue_data:
        breakdown = revenue_data["breakdown"]

        for category, data in breakdown.items():
            if isinstance(data, list):
                df = pd.DataFrame(data)
                breakdowns[category.replace("by_", "")] = df

    return breakdowns


def calculate_kpi_metrics(data: Dict) -> Dict[str, Any]:
    """
    Calculate KPI metrics from raw data.

    Args:
        data: Raw data dictionary

    Returns:
        Dict[str, Any]: Calculated KPI metrics
    """
    metrics = {}

    # Revenue metrics
    if "summary" in data:
        summary = data["summary"]
        metrics.update(
            {
                "total_revenue": summary.get("total_revenue", 0),
                "total_orders": summary.get("total_orders", 0),
                "avg_order_value": summary.get("average_order_value", 0),
                "growth_rate": summary.get("growth_rate", 0),
                "profit_margin": summary.get("profit_margin", 0),
            }
        )

    # Forecast metrics
    if "forecast" in data:
        forecast = data["forecast"]
        metrics.update(
            {
                "predicted_revenue": forecast.get("next_period_revenue", 0),
                "forecast_trend": forecast.get("trend", "stable"),
                "confidence_lower": forecast.get("confidence_interval", {}).get(
                    "lower", 0
                ),
                "confidence_upper": forecast.get("confidence_interval", {}).get(
                    "upper", 0
                ),
            }
        )

    return metrics


def prepare_alert_data(alerts_data: Dict) -> pd.DataFrame:
    """
    Prepare alert data for display.

    Args:
        alerts_data: Dictionary containing alert information

    Returns:
        pd.DataFrame: Processed alert data
    """
    if not alerts_data or "alerts" not in alerts_data:
        return pd.DataFrame()

    alerts = []
    for alert in alerts_data["alerts"]:
        alert_info = {
            "alert_id": alert["alert_id"],
            "type": alert["alert_type"],
            "status": alert["status"],
            "priority": alert["priority"],
            "risk_score": alert["risk_score"],
            "amount": alert["amount"],
            "created_at": alert["created_at"],
            "description": alert["description"],
        }
        alerts.append(alert_info)

    df = pd.DataFrame(alerts)
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"])

    return df


def get_color_scale(values: List[float], color_scheme: str = "RdYlGn_r") -> List[str]:
    """
    Get color scale for values.

    Args:
        values: List of numeric values
        color_scheme: Color scheme name

    Returns:
        List[str]: List of color codes
    """
    if not values:
        return []

    # Normalize values to 0-1 range
    min_val, max_val = min(values), max(values)
    if max_val == min_val:
        return ["#1f77b4"] * len(values)

    normalized = [(v - min_val) / (max_val - min_val) for v in values]

    # Simple color mapping (can be enhanced with actual color schemes)
    colors = []
    for norm_val in normalized:
        if color_scheme == "RdYlGn_r":  # Red-Yellow-Green reversed
            if norm_val < 0.33:
                colors.append("#d62728")  # Red
            elif norm_val < 0.66:
                colors.append("#ff7f0e")  # Orange
            else:
                colors.append("#2ca02c")  # Green
        else:  # Default blue scale
            intensity = int(255 * (1 - norm_val))
            colors.append(f"rgb({intensity}, {intensity}, 255)")

    return colors


def create_summary_stats(
    df: pd.DataFrame, numeric_columns: List[str]
) -> Dict[str, Dict]:
    """
    Create summary statistics for numeric columns.

    Args:
        df: DataFrame to analyze
        numeric_columns: List of numeric column names

    Returns:
        Dict[str, Dict]: Summary statistics by column
    """
    stats = {}

    for col in numeric_columns:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            stats[col] = {
                "mean": df[col].mean(),
                "median": df[col].median(),
                "std": df[col].std(),
                "min": df[col].min(),
                "max": df[col].max(),
                "count": df[col].count(),
                "null_count": df[col].isnull().sum(),
            }

    return stats


@st.cache_data(ttl=300)
def cached_data_processing(data: Dict, processing_type: str) -> Any:
    """
    Cached data processing function for Streamlit.

    Args:
        data: Raw data to process
        processing_type: Type of processing to perform

    Returns:
        Processed data
    """
    if processing_type == "segments":
        return prepare_segment_data(data)
    elif processing_type == "revenue_breakdown":
        return prepare_revenue_breakdown(data)
    elif processing_type == "kpi_metrics":
        return calculate_kpi_metrics(data)
    elif processing_type == "alerts":
        return prepare_alert_data(data)
    else:
        return data

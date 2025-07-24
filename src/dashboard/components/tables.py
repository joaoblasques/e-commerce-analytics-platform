"""
Table components for data display.

This module provides reusable table components for displaying
structured data with formatting, filtering, and pagination.
"""

from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import streamlit as st

from dashboard.utils.data_processing import (
    format_currency,
    format_large_number,
    format_number,
    format_percentage,
)


def format_table_data(
    df: pd.DataFrame,
    column_formats: Dict[str, str] = None,
    column_names: Dict[str, str] = None,
) -> pd.DataFrame:
    """
    Format table data for display.

    Args:
        df: DataFrame to format
        column_formats: Dictionary mapping column names to format types
        column_names: Dictionary mapping column names to display names

    Returns:
        pd.DataFrame: Formatted DataFrame
    """
    if df.empty:
        return df

    formatted_df = df.copy()

    # Apply column formatting
    if column_formats:
        for column, format_type in column_formats.items():
            if column in formatted_df.columns:
                if format_type == "currency":
                    formatted_df[column] = formatted_df[column].apply(
                        lambda x: format_currency(x) if pd.notna(x) else "N/A"
                    )
                elif format_type == "percentage":
                    formatted_df[column] = formatted_df[column].apply(
                        lambda x: format_percentage(x) if pd.notna(x) else "N/A"
                    )
                elif format_type == "number":
                    formatted_df[column] = formatted_df[column].apply(
                        lambda x: format_number(x) if pd.notna(x) else "N/A"
                    )
                elif format_type == "large_number":
                    formatted_df[column] = formatted_df[column].apply(
                        lambda x: format_large_number(x) if pd.notna(x) else "N/A"
                    )

    # Rename columns
    if column_names:
        formatted_df = formatted_df.rename(columns=column_names)

    return formatted_df


def render_data_table(
    data: Union[pd.DataFrame, List[Dict]],
    title: str = "",
    column_formats: Dict[str, str] = None,
    column_names: Dict[str, str] = None,
    height: int = 400,
    use_container_width: bool = True,
    hide_index: bool = True,
    key: str = None,
) -> None:
    """
    Render a data table with formatting.

    Args:
        data: Data to display
        title: Table title
        column_formats: Column formatting options
        column_names: Column display names
        height: Table height
        use_container_width: Whether to use container width
        hide_index: Whether to hide the index
        key: Unique key for the component
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available")
        return

    if title:
        st.subheader(title)

    # Format the data
    formatted_df = format_table_data(df, column_formats, column_names)

    # Display the table
    st.dataframe(
        formatted_df,
        height=height,
        use_container_width=use_container_width,
        hide_index=hide_index,
        key=key,
    )


def render_customer_table(customers_data: Dict[str, Any]) -> None:
    """
    Render customer data table.

    Args:
        customers_data: Customer data from API
    """
    if not customers_data or "items" not in customers_data:
        st.warning("No customer data available")
        return

    customers = customers_data["items"]

    # Prepare data for table
    table_data = []
    for customer in customers:
        analytics = customer.get("analytics", {})
        demographics = customer.get("demographics", {})
        location = demographics.get("location", {})

        row = {
            "Customer ID": customer.get("id", ""),
            "Email": customer.get("email", ""),
            "Segment": analytics.get("rfm_segment", ""),
            "Lifetime Value": analytics.get("lifetime_value", 0),
            "Total Orders": analytics.get("total_orders", 0),
            "Total Spent": analytics.get("total_spent", 0),
            "Churn Risk": analytics.get("churn_risk", 0),
            "Location": f"{location.get('city', '')}, {location.get('state', '')}",
            "Last Purchase": customer.get("last_purchase", ""),
        }
        table_data.append(row)

    df = pd.DataFrame(table_data)

    # Format columns
    column_formats = {
        "Lifetime Value": "currency",
        "Total Spent": "currency",
        "Churn Risk": "percentage",
        "Total Orders": "number",
    }

    render_data_table(
        df,
        title="Customer Overview",
        column_formats=column_formats,
        height=500,
        key="customer_table",
    )


def render_revenue_breakdown_table(
    revenue_data: Dict[str, Any], breakdown_type: str = "category"
) -> None:
    """
    Render revenue breakdown table.

    Args:
        revenue_data: Revenue data from API
        breakdown_type: Type of breakdown (category, region, channel)
    """
    if not revenue_data or "breakdown" not in revenue_data:
        st.warning("No revenue breakdown data available")
        return

    breakdown = revenue_data["breakdown"]
    breakdown_key = f"by_{breakdown_type}"

    if breakdown_key not in breakdown:
        st.warning(f"No {breakdown_type} breakdown available")
        return

    data = breakdown[breakdown_key]
    df = pd.DataFrame(data)

    if df.empty:
        st.warning("No data available")
        return

    # Format columns
    column_formats = {"revenue": "currency", "percentage": "percentage"}

    column_names = {
        breakdown_type: breakdown_type.title(),
        "revenue": "Revenue",
        "percentage": "Percentage",
    }

    render_data_table(
        df,
        title=f"Revenue by {breakdown_type.title()}",
        column_formats=column_formats,
        column_names=column_names,
        height=300,
        key=f"revenue_{breakdown_type}_table",
    )


def render_fraud_alerts_table(fraud_data: Dict[str, Any]) -> None:
    """
    Render fraud alerts table.

    Args:
        fraud_data: Fraud alerts data from API
    """
    if not fraud_data or "alerts" not in fraud_data:
        st.warning("No fraud alerts available")
        return

    alerts = fraud_data["alerts"]

    # Prepare data for table
    table_data = []
    for alert in alerts:
        investigation = alert.get("investigation", {})

        row = {
            "Alert ID": alert.get("alert_id", ""),
            "Type": alert.get("alert_type", ""),
            "Status": alert.get("status", ""),
            "Priority": alert.get("priority", ""),
            "Risk Score": alert.get("risk_score", 0),
            "Amount": alert.get("amount", 0),
            "Customer ID": alert.get("customer_id", ""),
            "Created": alert.get("created_at", ""),
            "Assigned To": investigation.get("assigned_to", "Unassigned"),
        }
        table_data.append(row)

    df = pd.DataFrame(table_data)

    # Format columns
    column_formats = {"Risk Score": "percentage", "Amount": "currency"}

    # Add status styling
    def style_status(val):
        if val == "open":
            return "background-color: #ffebee"
        elif val == "investigating":
            return "background-color: #fff3e0"
        elif val == "resolved":
            return "background-color: #e8f5e8"
        return ""

    render_data_table(
        df,
        title="Fraud Alerts",
        column_formats=column_formats,
        height=500,
        key="fraud_alerts_table",
    )


def render_product_performance_table(product_data: Dict[str, Any]) -> None:
    """
    Render product performance table.

    Args:
        product_data: Product performance data from API
    """
    if not product_data or "products" not in product_data:
        st.warning("No product data available")
        return

    products = product_data["products"]

    # Prepare data for table
    table_data = []
    for product in products:
        row = {
            "Product ID": product.get("product_id", ""),
            "Name": product.get("name", ""),
            "Category": product.get("category", ""),
            "Revenue": product.get("revenue", 0),
            "Units Sold": product.get("units_sold", 0),
            "Average Rating": product.get("average_rating", 0),
            "Margin": product.get("margin", 0),
            "Growth Rate": product.get("growth_rate", 0),
            "Inventory Status": product.get("inventory_status", ""),
        }
        table_data.append(row)

    df = pd.DataFrame(table_data)

    # Format columns
    column_formats = {
        "Revenue": "currency",
        "Units Sold": "number",
        "Margin": "percentage",
        "Growth Rate": "percentage",
    }

    render_data_table(
        df,
        title="Product Performance",
        column_formats=column_formats,
        height=500,
        key="product_performance_table",
    )


def render_system_health_table(system_data: Dict[str, Any]) -> None:
    """
    Render system health components table.

    Args:
        system_data: System health data from API
    """
    if not system_data or "core_metrics" not in system_data:
        st.warning("No system health data available")
        return

    core_metrics = system_data["core_metrics"]

    # Prepare data for table
    table_data = []
    for component, metrics in core_metrics.items():
        status = metrics.get("status", "unknown")

        # Get key metric based on component
        key_metric = ""
        if component == "api_server":
            key_metric = f"{metrics.get('response_time_p95_ms', 0)}ms"
        elif component == "database":
            key_metric = f"{metrics.get('query_time_p95_ms', 0)}ms"
        elif component == "kafka":
            key_metric = f"{metrics.get('consumer_lag', 0)} lag"
        elif component == "spark":
            key_metric = f"{metrics.get('active_jobs', 0)} jobs"
        elif component == "redis":
            key_metric = f"{metrics.get('cache_hit_rate', 0):.1%} hit rate"

        row = {
            "Component": component.replace("_", " ").title(),
            "Status": status.title(),
            "Key Metric": key_metric,
            "Details": str(metrics)[:50] + "..."
            if len(str(metrics)) > 50
            else str(metrics),
        }
        table_data.append(row)

    df = pd.DataFrame(table_data)

    render_data_table(
        df, title="System Health Components", height=300, key="system_health_table"
    )


def render_paginated_table(
    data: Union[pd.DataFrame, List[Dict]],
    title: str = "",
    page_size: int = 20,
    column_formats: Dict[str, str] = None,
    column_names: Dict[str, str] = None,
    key: str = None,
) -> None:
    """
    Render a paginated table.

    Args:
        data: Data to display
        title: Table title
        page_size: Number of rows per page
        column_formats: Column formatting options
        column_names: Column display names
        key: Unique key for the component
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available")
        return

    if title:
        st.subheader(title)

    # Calculate pagination
    total_rows = len(df)
    total_pages = (total_rows + page_size - 1) // page_size

    if total_pages > 1:
        # Page selector
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            page = st.selectbox(
                "Page",
                range(1, total_pages + 1),
                key=f"{key}_page_selector" if key else None,
            )

        # Calculate start and end indices
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_rows)

        # Show page info
        st.caption(f"Showing {start_idx + 1}-{end_idx} of {total_rows} rows")

        # Get page data
        page_df = df.iloc[start_idx:end_idx]
    else:
        page_df = df

    # Format and display the data
    formatted_df = format_table_data(page_df, column_formats, column_names)

    st.dataframe(
        formatted_df,
        height=400,
        use_container_width=True,
        hide_index=True,
        key=f"{key}_table" if key else None,
    )


def render_searchable_table(
    data: Union[pd.DataFrame, List[Dict]],
    title: str = "",
    search_columns: List[str] = None,
    column_formats: Dict[str, str] = None,
    column_names: Dict[str, str] = None,
    key: str = None,
) -> None:
    """
    Render a searchable table.

    Args:
        data: Data to display
        title: Table title
        search_columns: Columns to search in
        column_formats: Column formatting options
        column_names: Column display names
        key: Unique key for the component
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available")
        return

    if title:
        st.subheader(title)

    # Search functionality
    search_term = st.text_input(
        "Search",
        placeholder="Enter search term...",
        key=f"{key}_search" if key else None,
    )

    # Filter data based on search
    if search_term:
        if search_columns:
            # Search in specific columns
            mask = (
                df[search_columns]
                .astype(str)
                .apply(lambda x: x.str.contains(search_term, case=False, na=False))
                .any(axis=1)
            )
        else:
            # Search in all string columns
            string_columns = df.select_dtypes(include=["object"]).columns
            if len(string_columns) > 0:
                mask = (
                    df[string_columns]
                    .astype(str)
                    .apply(lambda x: x.str.contains(search_term, case=False, na=False))
                    .any(axis=1)
                )
            else:
                mask = pd.Series([True] * len(df))

        filtered_df = df[mask]

        if filtered_df.empty:
            st.warning(f"No results found for '{search_term}'")
            return
    else:
        filtered_df = df

    # Show result count
    if search_term:
        st.caption(f"Found {len(filtered_df)} results")

    # Format and display the data
    formatted_df = format_table_data(filtered_df, column_formats, column_names)

    st.dataframe(
        formatted_df,
        height=400,
        use_container_width=True,
        hide_index=True,
        key=f"{key}_table" if key else None,
    )


def render_sortable_table(
    data: Union[pd.DataFrame, List[Dict]],
    title: str = "",
    default_sort_column: str = None,
    default_sort_ascending: bool = True,
    column_formats: Dict[str, str] = None,
    column_names: Dict[str, str] = None,
    key: str = None,
) -> None:
    """
    Render a sortable table.

    Args:
        data: Data to display
        title: Table title
        default_sort_column: Default column to sort by
        default_sort_ascending: Default sort direction
        column_formats: Column formatting options
        column_names: Column display names
        key: Unique key for the component
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    if df.empty:
        st.warning("No data available")
        return

    if title:
        st.subheader(title)

    # Sort controls
    col1, col2 = st.columns([3, 1])

    with col1:
        sort_column = st.selectbox(
            "Sort by",
            options=df.columns.tolist(),
            index=df.columns.tolist().index(default_sort_column)
            if default_sort_column in df.columns
            else 0,
            key=f"{key}_sort_column" if key else None,
        )

    with col2:
        sort_ascending = st.checkbox(
            "Ascending",
            value=default_sort_ascending,
            key=f"{key}_sort_ascending" if key else None,
        )

    # Sort the data
    sorted_df = df.sort_values(by=sort_column, ascending=sort_ascending)

    # Format and display the data
    formatted_df = format_table_data(sorted_df, column_formats, column_names)

    st.dataframe(
        formatted_df,
        height=400,
        use_container_width=True,
        hide_index=True,
        key=f"{key}_table" if key else None,
    )

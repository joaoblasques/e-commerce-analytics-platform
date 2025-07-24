"""
Customer Analytics Dashboard page.

This module provides comprehensive customer analytics including RFM segmentation,
customer lifetime value analysis, churn prediction, and customer journey insights.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from dashboard.components import (
    render_bar_chart,
    render_cohort_heatmap,
    render_customer_metrics,
    render_customer_table,
    render_data_table,
    render_funnel_chart,
    render_gauge_chart,
    render_heatmap,
    render_kpi_row,
    render_metric_card,
    render_pie_chart,
    render_scatter_plot,
    render_segment_treemap,
)
from dashboard.utils.api_client import APIError, handle_api_error
from dashboard.utils.data_processing import (
    cached_data_processing,
    format_currency,
    format_percentage,
    prepare_segment_data,
)


@handle_api_error
def load_customer_data() -> Dict[str, Any]:
    """Load data for customer analytics dashboard."""
    api_client = st.session_state.get("api_client")
    if not api_client:
        st.error("API client not available")
        return {}

    # Get filter values from session state
    segment_filter = st.session_state.get("filter_segment")

    data = {}

    try:
        # Load customer segments
        data["segments"] = api_client.get_customer_segments(
            segment_type="rfm", include_predictions=True
        )

        # Load customer list
        customer_params = {
            "segment": segment_filter,
            "sort_by": "lifetime_value",
            "include_predictions": True,
            "page": 1,
            "size": 50,
        }
        data["customers"] = api_client.get_customers(**customer_params)

        # Load cohort analysis
        data["cohort"] = api_client.get_cohort_analysis(
            cohort_type="monthly", metric="retention"
        )

        # Load funnel analysis
        data["funnel"] = api_client.get_funnel_analysis(
            funnel_type="purchase", time_range="30d"
        )

    except APIError as e:
        st.error(f"Failed to load customer data: {e.message}")
        return {}

    return data


def render_customer_overview(segments_data: Dict[str, Any]) -> None:
    """Render customer overview section."""
    st.header("👥 Customer Overview")

    if not segments_data:
        st.warning("No customer segment data available")
        return

    # Customer metrics
    render_customer_metrics(segments_data)

    st.markdown("---")

    # Customer segments visualization
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("Customer Segments Distribution")

        if "segments" in segments_data:
            segments = segments_data["segments"]

            # Prepare data for pie chart
            chart_data = [
                {
                    "segment": seg["name"],
                    "count": seg["count"],
                    "percentage": seg["percentage"],
                }
                for seg in segments
            ]

            render_pie_chart(
                data=chart_data,
                values_column="count",
                names_column="segment",
                title="Customer Distribution by Segment",
                height=400,
            )

    with col2:
        st.subheader("Segment Value Analysis")

        if "segments" in segments_data:
            segments = segments_data["segments"]

            # Prepare data for CLV analysis
            clv_data = [
                {
                    "segment": seg["name"],
                    "avg_clv": seg["characteristics"]["avg_customer_lifetime_value"],
                    "count": seg["count"],
                    "revenue_contribution": seg["characteristics"][
                        "revenue_contribution"
                    ],
                }
                for seg in segments
            ]

            render_scatter_plot(
                data=clv_data,
                x_column="count",
                y_column="avg_clv",
                title="Segment Size vs Average CLV",
                size_column="revenue_contribution",
                color_column="segment",
                height=400,
            )


def render_rfm_analysis(segments_data: Dict[str, Any]) -> None:
    """Render RFM analysis section."""
    st.header("📊 RFM Analysis")

    if not segments_data or "segments" not in segments_data:
        st.warning("No RFM data available")
        return

    segments = segments_data["segments"]

    # RFM breakdown if available
    if "rfm_breakdown" in segments_data:
        rfm_breakdown = segments_data["rfm_breakdown"]

        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader("Recency Distribution")
            recency_data = rfm_breakdown.get("recency_distribution", {})
            if recency_data:
                chart_data = [
                    {"category": k.title(), "percentage": v}
                    for k, v in recency_data.items()
                ]
                render_bar_chart(
                    data=chart_data,
                    x_column="category",
                    y_column="percentage",
                    title="Recency Distribution",
                    height=300,
                )

        with col2:
            st.subheader("Frequency Distribution")
            frequency_data = rfm_breakdown.get("frequency_distribution", {})
            if frequency_data:
                chart_data = [
                    {"category": k.title(), "percentage": v}
                    for k, v in frequency_data.items()
                ]
                render_bar_chart(
                    data=chart_data,
                    x_column="category",
                    y_column="percentage",
                    title="Frequency Distribution",
                    height=300,
                )

        with col3:
            st.subheader("Monetary Distribution")
            monetary_data = rfm_breakdown.get("monetary_distribution", {})
            if monetary_data:
                chart_data = [
                    {"category": k.title(), "percentage": v}
                    for k, v in monetary_data.items()
                ]
                render_bar_chart(
                    data=chart_data,
                    x_column="category",
                    y_column="percentage",
                    title="Monetary Distribution",
                    height=300,
                )

    # Segment treemap
    st.subheader("Customer Segments Treemap")
    render_segment_treemap(segments_data, "RFM Customer Segments")

    # Detailed segment table
    st.subheader("Segment Details")

    # Prepare segment data for table
    segment_table_data = []
    for seg in segments:
        characteristics = seg["characteristics"]
        row = {
            "Segment": seg["name"],
            "Count": seg["count"],
            "Percentage": seg["percentage"],
            "Avg CLV": characteristics["avg_customer_lifetime_value"],
            "Avg Orders": characteristics["avg_orders_per_customer"],
            "Revenue Contribution": characteristics["revenue_contribution"],
        }
        segment_table_data.append(row)

    df = pd.DataFrame(segment_table_data)

    st.dataframe(
        df,
        column_config={
            "Percentage": st.column_config.NumberColumn("Percentage", format="%.1f%%"),
            "Avg CLV": st.column_config.NumberColumn("Avg CLV", format="$%.2f"),
            "Revenue Contribution": st.column_config.NumberColumn(
                "Revenue Contribution", format="$%.2f"
            ),
        },
        hide_index=True,
        use_container_width=True,
    )


def render_customer_lifetime_value(segments_data: Dict[str, Any]) -> None:
    """Render customer lifetime value analysis."""
    st.header("💰 Customer Lifetime Value Analysis")

    if not segments_data or "segments" not in segments_data:
        st.warning("No CLV data available")
        return

    segments = segments_data["segments"]

    # CLV metrics
    total_customers = sum(seg["count"] for seg in segments)
    total_clv = sum(
        seg["count"] * seg["characteristics"]["avg_customer_lifetime_value"]
        for seg in segments
    )
    avg_clv = total_clv / total_customers if total_customers > 0 else 0

    # High-value customers (CLV > $2000)
    high_value_customers = sum(
        seg["count"]
        for seg in segments
        if seg["characteristics"]["avg_customer_lifetime_value"] > 2000
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Average CLV",
            format_currency(avg_clv),
            help="Average customer lifetime value across all segments",
        )

    with col2:
        st.metric(
            "Total CLV",
            format_currency(total_clv),
            help="Total customer lifetime value",
        )

    with col3:
        st.metric(
            "High-Value Customers",
            f"{high_value_customers:,}",
            help="Customers with CLV > $2,000",
        )

    with col4:
        high_value_percentage = (
            (high_value_customers / total_customers) * 100 if total_customers > 0 else 0
        )
        st.metric(
            "High-Value %",
            f"{high_value_percentage:.1f}%",
            help="Percentage of high-value customers",
        )

    # CLV distribution chart
    col1, col2 = st.columns([2, 1])

    with col1:
        # CLV by segment bar chart
        clv_data = [
            {
                "segment": seg["name"],
                "avg_clv": seg["characteristics"]["avg_customer_lifetime_value"],
                "count": seg["count"],
            }
            for seg in segments
        ]
        clv_data.sort(key=lambda x: x["avg_clv"], reverse=True)

        render_bar_chart(
            data=clv_data,
            x_column="segment",
            y_column="avg_clv",
            title="Average CLV by Segment",
            height=400,
        )

    with col2:
        # CLV distribution gauge for top segment
        if clv_data:
            top_segment = clv_data[0]
            top_clv = top_segment["avg_clv"]

            render_gauge_chart(
                value=min(top_clv / 50, 100),  # Scale to 0-100
                title=f"Top Segment CLV\n{top_segment['segment']}",
                min_value=0,
                max_value=100,
                height=300,
            )


def render_churn_analysis(segments_data: Dict[str, Any]) -> None:
    """Render churn analysis section."""
    st.header("⚠️ Churn Risk Analysis")

    if not segments_data or "predictions" not in segments_data:
        st.warning("No churn prediction data available")
        return

    predictions = segments_data["predictions"]
    churn_risk = predictions.get("churn_risk", {})

    # Churn risk metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        high_risk = churn_risk.get("high_risk", 0)
        st.metric(
            "High Risk Customers",
            f"{high_risk:,}",
            help="Customers with high churn probability",
        )

    with col2:
        medium_risk = churn_risk.get("medium_risk", 0)
        st.metric(
            "Medium Risk Customers",
            f"{medium_risk:,}",
            help="Customers with medium churn probability",
        )

    with col3:
        low_risk = churn_risk.get("low_risk", 0)
        st.metric(
            "Low Risk Customers",
            f"{low_risk:,}",
            help="Customers with low churn probability",
        )

    # Churn risk distribution
    churn_data = [
        {"risk_level": "High Risk", "count": high_risk, "color": "#d62728"},
        {"risk_level": "Medium Risk", "count": medium_risk, "color": "#ff7f0e"},
        {"risk_level": "Low Risk", "count": low_risk, "color": "#2ca02c"},
    ]

    render_pie_chart(
        data=churn_data,
        values_column="count",
        names_column="risk_level",
        title="Churn Risk Distribution",
        height=400,
    )

    # Predicted CLV increase opportunities
    if "predicted_ltv_increase" in predictions:
        st.subheader("CLV Improvement Opportunities")

        ltv_increase = predictions["predicted_ltv_increase"]
        increase_data = [
            {"segment": k, "increase_percentage": v} for k, v in ltv_increase.items()
        ]

        render_bar_chart(
            data=increase_data,
            x_column="segment",
            y_column="increase_percentage",
            title="Predicted CLV Increase by Segment (%)",
            height=300,
        )


def render_cohort_analysis(cohort_data: Dict[str, Any]) -> None:
    """Render cohort analysis section."""
    st.header("📈 Cohort Analysis")

    if not cohort_data:
        st.warning("No cohort data available")
        return

    # Cohort summary metrics
    if "summary" in cohort_data:
        summary = cohort_data["summary"]

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Avg Retention (Period 1)",
                format_percentage(summary.get("avg_retention_period_1", 0)),
                help="Average retention rate in first period",
            )

        with col2:
            st.metric(
                "Avg Retention (Period 6)",
                format_percentage(summary.get("avg_retention_period_6", 0)),
                help="Average retention rate in sixth period",
            )

        with col3:
            st.metric(
                "Best Cohort",
                summary.get("best_performing_cohort", "N/A"),
                help="Best performing cohort",
            )

        with col4:
            st.metric(
                "Cohort Health",
                summary.get("cohort_health", "N/A").title(),
                help="Overall cohort health assessment",
            )

    # Cohort heatmap
    render_cohort_heatmap(cohort_data, "Customer Retention Cohort Analysis")

    # Cohort insights
    if "insights" in cohort_data:
        insights = cohort_data["insights"]

        st.subheader("Cohort Insights")

        col1, col2 = st.columns(2)

        with col1:
            st.write(
                "**Retention Trend:**", insights.get("retention_trend", "N/A").title()
            )
            st.write(
                "**Churn Pattern:**",
                insights.get("churn_pattern", "N/A").replace("_", " ").title(),
            )

        with col2:
            st.write(
                "**Revenue Opportunity:**",
                insights.get("revenue_opportunity", "N/A").replace("_", " ").title(),
            )


def render_customer_journey(funnel_data: Dict[str, Any]) -> None:
    """Render customer journey analysis."""
    st.header("🛤️ Customer Journey Analysis")

    if not funnel_data:
        st.warning("No customer journey data available")
        return

    # Journey funnel
    if "steps" in funnel_data:
        steps = funnel_data["steps"]

        render_funnel_chart(
            data=steps,
            stage_column="name",
            value_column="users",
            title="Customer Purchase Journey",
            height=500,
        )

    # Journey metrics
    if "summary" in funnel_data:
        summary = funnel_data["summary"]

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Entries",
                f"{summary.get('total_entries', 0):,}",
                help="Total users entering the funnel",
            )

        with col2:
            st.metric(
                "Total Conversions",
                f"{summary.get('total_conversions', 0):,}",
                help="Total users completing the journey",
            )

        with col3:
            st.metric(
                "Conversion Rate",
                format_percentage(summary.get("overall_conversion_rate", 0)),
                help="Overall conversion rate",
            )

        with col4:
            st.metric(
                "Biggest Drop-off",
                summary.get("biggest_drop_off_step", "N/A"),
                help="Step with highest drop-off rate",
            )

    # Journey optimization recommendations
    if "recommendations" in funnel_data:
        recommendations = funnel_data["recommendations"]

        st.subheader("Optimization Recommendations")

        for rec in recommendations:
            step = rec.get("step", "Unknown")
            action = rec.get("action", "No action specified")
            expected_lift = rec.get("expected_lift", 0)

            st.write(
                f"**{step}:** {action} (Expected lift: {format_percentage(expected_lift)})"
            )


def render_customer_list(customers_data: Dict[str, Any]) -> None:
    """Render customer list section."""
    st.header("📋 Customer List")

    if not customers_data:
        st.warning("No customer data available")
        return

    # Customer table with analytics
    render_customer_table(customers_data)

    # Pagination info
    if "pagination" in customers_data:
        pagination = customers_data["pagination"]
        st.caption(
            f"Showing page {pagination.get('page', 1)} of {pagination.get('pages', 1)} "
            f"({pagination.get('total', 0)} total customers)"
        )


def render() -> None:
    """Render the customer analytics dashboard page."""
    st.title("👥 Customer Analytics")
    st.markdown(
        "Comprehensive customer segmentation, lifetime value, and behavior analysis"
    )

    # Load data
    with st.spinner("Loading customer analytics data..."):
        data = load_customer_data()

    if not data:
        st.error("Failed to load customer analytics data")
        return

    # Customer overview
    render_customer_overview(data.get("segments", {}))

    st.markdown("---")

    # RFM analysis
    render_rfm_analysis(data.get("segments", {}))

    st.markdown("---")

    # Customer lifetime value
    render_customer_lifetime_value(data.get("segments", {}))

    st.markdown("---")

    # Churn analysis
    render_churn_analysis(data.get("segments", {}))

    st.markdown("---")

    # Cohort analysis
    render_cohort_analysis(data.get("cohort", {}))

    st.markdown("---")

    # Customer journey
    render_customer_journey(data.get("funnel", {}))

    st.markdown("---")

    # Customer list
    render_customer_list(data.get("customers", {}))

    # Footer with last update time
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

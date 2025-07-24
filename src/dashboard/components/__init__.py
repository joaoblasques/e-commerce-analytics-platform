"""
Dashboard components package.

This package provides reusable UI components for the Streamlit dashboard,
including sidebar navigation, metrics cards, charts, tables, and alerts.
"""

from .alerts import (
    render_alert,
    render_alert_summary,
    render_business_alerts,
    render_data_quality_alerts,
    render_fraud_alerts,
    render_notification_banner,
    render_performance_alerts,
    render_status_indicator,
    render_system_alerts,
)
from .charts import (
    render_bar_chart,
    render_cohort_heatmap,
    render_customer_metrics_executive,
    render_donut_chart,
    render_dual_axis_chart,
    render_executive_kpi_overview,
    render_funnel_chart,
    render_gauge_chart,
    render_geographic_map,
    render_heatmap,
    render_line_chart,
    render_pie_chart,
    render_revenue_performance_executive,
    render_scatter_plot,
    render_segment_treemap,
    render_time_series_chart,
    render_waterfall_chart,
)
from .metrics_cards import (
    render_business_metrics,
    render_comparison_metrics,
    render_custom_metrics,
    render_customer_metrics,
    render_fraud_metrics,
    render_kpi_row,
    render_metric_card,
    render_revenue_metrics,
    render_system_metrics,
)
from .sidebar import render_sidebar
from .tables import (
    render_customer_table,
    render_data_table,
    render_fraud_alerts_table,
    render_paginated_table,
    render_product_performance_table,
    render_revenue_breakdown_table,
    render_searchable_table,
    render_sortable_table,
    render_system_health_table,
)

__all__ = [
    # Sidebar
    "render_sidebar",
    # Metrics Cards
    "render_metric_card",
    "render_kpi_row",
    "render_revenue_metrics",
    "render_customer_metrics",
    "render_fraud_metrics",
    "render_system_metrics",
    "render_business_metrics",
    "render_custom_metrics",
    "render_comparison_metrics",
    # Charts
    "render_line_chart",
    "render_bar_chart",
    "render_pie_chart",
    "render_donut_chart",
    "render_scatter_plot",
    "render_heatmap",
    "render_gauge_chart",
    "render_funnel_chart",
    "render_waterfall_chart",
    "render_time_series_chart",
    "render_cohort_heatmap",
    "render_segment_treemap",
    "render_dual_axis_chart",
    "render_geographic_map",
    "render_executive_kpi_overview",
    "render_revenue_performance_executive",
    "render_customer_metrics_executive",
    # Tables
    "render_data_table",
    "render_customer_table",
    "render_revenue_breakdown_table",
    "render_fraud_alerts_table",
    "render_product_performance_table",
    "render_system_health_table",
    "render_paginated_table",
    "render_searchable_table",
    "render_sortable_table",
    # Alerts
    "render_alert",
    "render_system_alerts",
    "render_fraud_alerts",
    "render_business_alerts",
    "render_performance_alerts",
    "render_data_quality_alerts",
    "render_alert_summary",
    "render_notification_banner",
    "render_status_indicator",
]

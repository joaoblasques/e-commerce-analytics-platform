"""
Operational Dashboard page.

This module provides comprehensive operational monitoring including
fraud detection monitoring, system health metrics, data quality monitoring,
and alert/notification panels for operations teams.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from dashboard.components import (
    render_alert,
    render_alert_summary,
    render_bar_chart,
    render_data_quality_alerts,
    render_data_table,
    render_fraud_alerts,
    render_gauge_chart,
    render_kpi_row,
    render_line_chart,
    render_metric_card,
    render_notification_banner,
    render_performance_alerts,
    render_pie_chart,
    render_status_indicator,
    render_system_alerts,
    render_system_health_table,
    render_time_series_chart,
)
from dashboard.utils.api_client import APIError, handle_api_error
from dashboard.utils.data_processing import (
    format_currency,
    format_large_number,
    format_number,
    format_percentage,
)


@handle_api_error
def load_operational_data() -> Dict[str, Any]:
    """Load operational dashboard data."""
    api_client = st.session_state.get("api_client")
    if not api_client:
        st.error("API client not available")
        return {}

    data = {}

    try:
        # Load system health data
        data["system_health"] = api_client.get_system_health(
            include_details=True, include_historical=True
        )

        # Load fraud detection operational data
        data["fraud_monitoring"] = api_client.get(
            "/api/v1/operations/fraud-monitoring",
            params={"time_range": "24h", "include_alerts": True}
        )

        # Load data quality metrics
        data["data_quality"] = api_client.get(
            "/api/v1/operations/data-quality",
            params={"time_range": "24h", "include_anomalies": True}
        )

        # Load operational alerts
        data["alerts"] = api_client.get(
            "/api/v1/operations/alerts",
            params={"status": "active", "priority": "all", "page": 1, "size": 50}
        )

        # Load performance benchmarks
        data["performance"] = api_client.get(
            "/api/v1/operations/performance",
            params={"benchmark_type": "all", "time_range": "24h"}
        )

        # Load streaming pipeline health
        data["streaming"] = api_client.get(
            "/api/v1/operations/streaming-health",
            params={"include_lag": True, "include_throughput": True}
        )

    except APIError as e:
        st.error(f"Failed to load operational data: {e.message}")
        return {}

    return data


def render_operational_overview(system_health: Dict[str, Any]) -> None:
    """Render operational overview with key system health metrics."""
    st.header("🎛️ Operational Overview")

    if not system_health:
        st.warning("No system health data available")
        return

    # Overall system status banner
    overall_status = system_health.get("overall_status", "unknown")
    
    if overall_status == "healthy":
        render_notification_banner(
            message="All systems operational",
            banner_type="success",
            icon="✅"
        )
    elif overall_status == "degraded":
        render_notification_banner(
            message="Some systems experiencing issues",
            banner_type="warning",
            icon="⚠️"
        )
    elif overall_status == "critical":
        render_notification_banner(
            message="Critical system issues detected",
            banner_type="error",
            icon="🚨"
        )
    else:
        render_notification_banner(
            message="System status unknown",
            banner_type="info",
            icon="❓"
        )

    # Key operational metrics
    col1, col2, col3, col4, col5 = st.columns(5)

    core_metrics = system_health.get("core_metrics", {})
    
    with col1:
        uptime = system_health.get("uptime_seconds", 0) / 86400
        st.metric(
            "System Uptime",
            f"{uptime:.1f} days",
            help="Total system uptime"
        )

    with col2:
        api_health = core_metrics.get("api_server", {})
        response_time = api_health.get("response_time_p95_ms", 0)
        st.metric(
            "API Response (P95)",
            f"{response_time:.0f}ms",
            help="95th percentile API response time"
        )

    with col3:
        throughput = system_health.get("throughput_rps", 0)
        st.metric(
            "Throughput",
            f"{throughput:,.0f} RPS",
            help="Requests per second"
        )

    with col4:
        error_rate = system_health.get("error_rate", 0)
        delta_color = "inverse" if error_rate > 0.01 else "normal"
        st.metric(
            "Error Rate",
            format_percentage(error_rate),
            delta_color=delta_color,
            help="System-wide error rate"
        )

    with col5:
        cpu_usage = core_metrics.get("overall", {}).get("cpu_usage_percent", 0)
        delta_color = "inverse" if cpu_usage > 80 else "normal"
        st.metric(
            "CPU Usage",
            f"{cpu_usage:.1f}%",
            delta_color=delta_color,
            help="Overall system CPU usage"
        )


def render_fraud_monitoring_ops(fraud_data: Dict[str, Any]) -> None:
    """Render fraud detection monitoring for operations."""
    st.header("🛡️ Fraud Detection Operations")

    if not fraud_data:
        st.warning("No fraud monitoring data available")
        return

    # Fraud operational metrics
    col1, col2, col3, col4 = st.columns(4)

    monitoring = fraud_data.get("monitoring", {})
    model_health = fraud_data.get("model_health", {})

    with col1:
        alerts_24h = monitoring.get("alerts_last_24h", 0)
        st.metric(
            "Alerts (24h)",
            f"{alerts_24h:,}",
            help="Fraud alerts generated in last 24 hours"
        )

    with col2:
        model_accuracy = model_health.get("ensemble_accuracy", 0)
        delta_color = "inverse" if model_accuracy < 0.90 else "normal"
        st.metric(
            "Model Accuracy",
            format_percentage(model_accuracy),
            delta_color=delta_color,
            help="Current fraud detection model accuracy"
        )

    with col3:
        detection_latency = model_health.get("avg_detection_latency_ms", 0)
        delta_color = "inverse" if detection_latency > 200 else "normal"
        st.metric(
            "Detection Latency",
            f"{detection_latency:.0f}ms",
            delta_color=delta_color,
            help="Average fraud detection latency"
        )

    with col4:
        false_positive_rate = monitoring.get("false_positive_rate", 0)
        delta_color = "inverse" if false_positive_rate > 0.15 else "normal"
        st.metric(
            "False Positive Rate",
            format_percentage(false_positive_rate),
            delta_color=delta_color,
            help="Current false positive rate"
        )

    # Fraud model health indicators
    st.subheader("Model Health Status")
    
    col1, col2 = st.columns(2)
    
    with col1:
        models = model_health.get("individual_models", [])
        if models:
            model_status_data = []
            for model in models:
                status = "healthy" if model.get("accuracy", 0) > 0.85 else "degraded"
                model_status_data.append({
                    "Model": model.get("name", ""),
                    "Status": status.title(),
                    "Accuracy": format_percentage(model.get("accuracy", 0)),
                    "Last Update": model.get("last_trained", "Unknown")
                })
            
            df = pd.DataFrame(model_status_data)
            st.dataframe(df, hide_index=True, use_container_width=True)
    
    with col2:
        # Model performance gauge
        ensemble_accuracy = model_health.get("ensemble_accuracy", 0) * 100
        render_gauge_chart(
            value=ensemble_accuracy,
            title="Ensemble Model Performance",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 85], "color": "red"},
                {"range": [85, 95], "color": "yellow"},
                {"range": [95, 100], "color": "green"},
            ],
            height=300,
        )


def render_system_health_ops(system_health: Dict[str, Any]) -> None:
    """Render system health monitoring for operations."""
    st.header("🏥 System Health Monitoring")

    if not system_health:
        st.warning("No system health data available")
        return

    # Component health table
    st.subheader("Component Status")
    
    core_metrics = system_health.get("core_metrics", {})
    component_health_data = []
    
    for component_name, component_data in core_metrics.items():
        status = component_data.get("status", "unknown")
        health_data = {
            "Component": component_name.replace("_", " ").title(),
            "Status": status.title(),
            "CPU": f"{component_data.get('cpu_usage_percent', 0):.1f}%",
            "Memory": f"{component_data.get('memory_usage_mb', 0):,.0f} MB",
            "Response Time": f"{component_data.get('response_time_ms', 0):.0f}ms",
            "Last Check": component_data.get("last_health_check", "Unknown")
        }
        component_health_data.append(health_data)
    
    if component_health_data:
        render_system_health_table(component_health_data)

    # System resource utilization
    st.subheader("Resource Utilization")
    
    col1, col2, col3 = st.columns(3)
    
    overall_metrics = core_metrics.get("overall", {})
    
    with col1:
        cpu_usage = overall_metrics.get("cpu_usage_percent", 0)
        render_gauge_chart(
            value=cpu_usage,
            title="CPU Usage",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 70], "color": "green"},
                {"range": [70, 90], "color": "yellow"},
                {"range": [90, 100], "color": "red"},
            ],
            height=250,
        )
    
    with col2:
        memory_usage = overall_metrics.get("memory_usage_percent", 0)
        render_gauge_chart(
            value=memory_usage,
            title="Memory Usage",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 70], "color": "green"},
                {"range": [70, 90], "color": "yellow"},
                {"range": [90, 100], "color": "red"},
            ],
            height=250,
        )
    
    with col3:
        disk_usage = overall_metrics.get("disk_usage_percent", 0)
        render_gauge_chart(
            value=disk_usage,
            title="Disk Usage",
            min_value=0,
            max_value=100,
            threshold_ranges=[
                {"range": [0, 70], "color": "green"},
                {"range": [70, 90], "color": "yellow"},
                {"range": [90, 100], "color": "red"},
            ],
            height=250,
        )


def render_data_quality_monitoring(data_quality: Dict[str, Any]) -> None:
    """Render data quality monitoring section."""
    st.header("📊 Data Quality Monitoring")

    if not data_quality:
        st.warning("No data quality data available")
        return

    # Data quality overview metrics
    quality_overview = data_quality.get("overview", {})
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        overall_score = quality_overview.get("overall_quality_score", 0)
        delta_color = "inverse" if overall_score < 0.90 else "normal"
        st.metric(
            "Overall Quality Score",
            format_percentage(overall_score),
            delta_color=delta_color,
            help="Overall data quality score"
        )
    
    with col2:
        completeness = quality_overview.get("completeness_score", 0)
        delta_color = "inverse" if completeness < 0.95 else "normal"
        st.metric(
            "Data Completeness",
            format_percentage(completeness),
            delta_color=delta_color,
            help="Percentage of complete data records"
        )
    
    with col3:
        accuracy = quality_overview.get("accuracy_score", 0)
        delta_color = "inverse" if accuracy < 0.95 else "normal"
        st.metric(
            "Data Accuracy",
            format_percentage(accuracy),
            delta_color=delta_color,
            help="Data accuracy score based on validation rules"
        )
    
    with col4:
        freshness_issues = quality_overview.get("freshness_violations", 0)
        delta_color = "inverse" if freshness_issues > 0 else "normal"
        st.metric(
            "Freshness Issues",
            f"{freshness_issues:,}",
            delta_color=delta_color,
            help="Number of data freshness violations"
        )

    # Data quality alerts
    st.subheader("Data Quality Alerts")
    
    quality_alerts = data_quality.get("alerts", [])
    if quality_alerts:
        render_data_quality_alerts(quality_alerts)
    else:
        st.success("✅ No data quality issues detected")

    # Data pipeline health
    st.subheader("Data Pipeline Health")
    
    pipelines = data_quality.get("pipeline_health", [])
    if pipelines:
        pipeline_data = []
        for pipeline in pipelines:
            pipeline_data.append({
                "Pipeline": pipeline.get("name", ""),
                "Status": pipeline.get("status", "").title(),
                "Last Run": pipeline.get("last_run", ""),
                "Success Rate": format_percentage(pipeline.get("success_rate", 0)),
                "Avg Duration": f"{pipeline.get('avg_duration_minutes', 0):.1f}m",
                "Data Volume": format_large_number(pipeline.get("records_processed", 0))
            })
        
        df = pd.DataFrame(pipeline_data)
        st.dataframe(df, hide_index=True, use_container_width=True)

    # Data quality trends
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Quality Score Trends")
        
        # Generate sample trend data
        hours = list(range(24))
        quality_trends = []
        base_score = overall_score
        for hour in hours:
            quality_trends.append({
                "hour": f"{hour:02d}:00",
                "quality_score": base_score * (0.95 + 0.1 * ((hour + 1) % 6) / 6)
            })
        
        render_line_chart(
            data=quality_trends,
            x_column="hour",
            y_column="quality_score",
            title="Data Quality Score (24h)",
            height=300,
        )
    
    with col2:
        st.subheader("Issue Distribution")
        
        issues = data_quality.get("issue_distribution", {})
        if issues:
            issue_data = []
            for issue_type, count in issues.items():
                issue_data.append({
                    "issue_type": issue_type.replace("_", " ").title(),
                    "count": count
                })
            
            render_pie_chart(
                data=issue_data,
                values_column="count",
                names_column="issue_type",
                title="Data Quality Issues",
                height=300,
            )


def render_alert_notification_panel(alerts_data: Dict[str, Any]) -> None:
    """Render alert and notification panel."""
    st.header("🚨 Alerts & Notifications")

    if not alerts_data:
        st.warning("No alerts data available")
        return

    # Alert summary
    alert_summary = alerts_data.get("summary", {})
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        critical_alerts = alert_summary.get("critical", 0)
        delta_color = "inverse" if critical_alerts > 0 else "normal"
        st.metric(
            "Critical Alerts",
            f"{critical_alerts:,}",
            delta_color=delta_color,
            help="Number of critical alerts"
        )
    
    with col2:
        warning_alerts = alert_summary.get("warning", 0)
        st.metric(
            "Warning Alerts",
            f"{warning_alerts:,}",
            help="Number of warning alerts"
        )
    
    with col3:
        resolved_24h = alert_summary.get("resolved_last_24h", 0)
        st.metric(
            "Resolved (24h)",
            f"{resolved_24h:,}",
            help="Alerts resolved in last 24 hours"
        )
    
    with col4:
        avg_resolution_time = alert_summary.get("avg_resolution_time_minutes", 0)
        st.metric(
            "Avg Resolution Time",
            f"{avg_resolution_time:.1f}m",
            help="Average time to resolve alerts"
        )

    # Active alerts by category
    st.subheader("Active Alerts")
    
    alerts_by_category = alerts_data.get("alerts_by_category", {})
    
    # System alerts
    system_alerts = alerts_by_category.get("system", [])
    if system_alerts:
        st.write("**System Alerts**")
        render_system_alerts(system_alerts)
    
    # Performance alerts
    performance_alerts = alerts_by_category.get("performance", [])
    if performance_alerts:
        st.write("**Performance Alerts**")
        render_performance_alerts(performance_alerts)
    
    # Fraud alerts
    fraud_alerts = alerts_by_category.get("fraud", [])
    if fraud_alerts:
        st.write("**Fraud Alerts**")
        render_fraud_alerts(fraud_alerts)

    # Recent alerts table
    st.subheader("Recent Alerts")
    
    recent_alerts = alerts_data.get("recent_alerts", [])
    if recent_alerts:
        alert_table_data = []
        for alert in recent_alerts[:20]:  # Show latest 20
            alert_table_data.append({
                "Timestamp": alert.get("timestamp", ""),
                "Severity": alert.get("severity", "").title(),
                "Category": alert.get("category", "").title(),
                "Message": alert.get("message", ""),
                "Status": alert.get("status", "").title(),
                "Assigned To": alert.get("assigned_to", "Unassigned")
            })
        
        df = pd.DataFrame(alert_table_data)
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.info("No recent alerts")


def render_performance_monitoring(performance_data: Dict[str, Any]) -> None:
    """Render performance monitoring section."""
    st.header("⚡ Performance Monitoring")

    if not performance_data:
        st.warning("No performance data available")
        return

    # Performance metrics
    metrics = performance_data.get("metrics", {})
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        api_p95 = metrics.get("api_response_p95_ms", 0)
        delta_color = "inverse" if api_p95 > 200 else "normal"
        st.metric(
            "API P95 Latency",
            f"{api_p95:.0f}ms",
            delta_color=delta_color,
            help="95th percentile API response time"
        )
    
    with col2:
        db_p95 = metrics.get("database_query_p95_ms", 0)
        delta_color = "inverse" if db_p95 > 100 else "normal"
        st.metric(
            "DB Query P95",
            f"{db_p95:.0f}ms",
            delta_color=delta_color,
            help="95th percentile database query time"
        )
    
    with col3:
        kafka_lag = metrics.get("kafka_consumer_lag", 0)
        delta_color = "inverse" if kafka_lag > 1000 else "normal"
        st.metric(
            "Kafka Consumer Lag",
            f"{kafka_lag:,}",
            delta_color=delta_color,
            help="Current Kafka consumer lag"
        )
    
    with col4:
        spark_jobs = metrics.get("spark_active_jobs", 0)
        st.metric(
            "Active Spark Jobs",
            f"{spark_jobs:,}",
            help="Number of active Spark jobs"
        )

    # Performance trends
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Response Time Trends")
        
        # Generate sample performance trend data
        hours = list(range(24))
        perf_trends = []
        for hour in hours:
            perf_trends.append({
                "hour": f"{hour:02d}:00",
                "api_response": api_p95 * (0.8 + 0.4 * (hour % 8) / 8),
                "db_query": db_p95 * (0.9 + 0.2 * (hour % 6) / 6)
            })
        
        render_time_series_chart(
            data=perf_trends,
            date_column="hour",
            value_columns=["api_response", "db_query"],
            title="Response Time Trends (24h)",
            height=300,
        )
    
    with col2:
        st.subheader("Throughput Monitoring")
        
        throughput_data = []
        for hour in hours:
            throughput_data.append({
                "hour": f"{hour:02d}:00",
                "requests_per_second": 1000 * (0.6 + 0.8 * (hour % 12) / 12)
            })
        
        render_bar_chart(
            data=throughput_data,
            x_column="hour",
            y_column="requests_per_second",
            title="Throughput (RPS) - 24h",
            height=300,
        )


def render_streaming_pipeline_health(streaming_data: Dict[str, Any]) -> None:
    """Render streaming pipeline health monitoring."""
    st.header("🌊 Streaming Pipeline Health")

    if not streaming_data:
        st.warning("No streaming data available")
        return

    # Streaming metrics
    pipeline_metrics = streaming_data.get("pipeline_metrics", {})
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        throughput = pipeline_metrics.get("messages_per_second", 0)
        st.metric(
            "Messages/Second",
            f"{throughput:,.0f}",
            help="Current streaming throughput"
        )
    
    with col2:
        processing_lag = pipeline_metrics.get("processing_lag_seconds", 0)
        delta_color = "inverse" if processing_lag > 10 else "normal"
        st.metric(
            "Processing Lag",
            f"{processing_lag:.1f}s",
            delta_color=delta_color,
            help="Current processing lag"
        )
    
    with col3:
        error_rate = pipeline_metrics.get("error_rate", 0)
        delta_color = "inverse" if error_rate > 0.01 else "normal"
        st.metric(
            "Error Rate",
            format_percentage(error_rate),
            delta_color=delta_color,
            help="Streaming pipeline error rate"
        )
    
    with col4:
        active_consumers = pipeline_metrics.get("active_consumers", 0)
        st.metric(
            "Active Consumers",
            f"{active_consumers:,}",
            help="Number of active stream consumers"
        )

    # Pipeline health by topic
    st.subheader("Pipeline Health by Topic")
    
    topics = streaming_data.get("topic_health", [])
    if topics:
        topic_data = []
        for topic in topics:
            topic_data.append({
                "Topic": topic.get("name", ""),
                "Status": topic.get("status", "").title(),
                "Messages/sec": f"{topic.get('messages_per_second', 0):,.0f}",
                "Consumer Lag": f"{topic.get('consumer_lag', 0):,}",
                "Partitions": f"{topic.get('partition_count', 0):,}",
                "Last Message": topic.get("last_message_time", "")
            })
        
        df = pd.DataFrame(topic_data)
        st.dataframe(df, hide_index=True, use_container_width=True)


def render_operational_filters() -> None:
    """Render filter options for operational dashboard."""
    with st.expander("🔍 Operational Filters", expanded=False):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            time_range = st.selectbox(
                "Time Range",
                options=["Last 1 hour", "Last 4 hours", "Last 24 hours", "Last 7 days"],
                index=2,
            )

        with col2:
            alert_severity = st.selectbox(
                "Alert Severity",
                options=["All", "Critical", "Warning", "Info"],
                index=0,
            )

        with col3:
            component_filter = st.selectbox(
                "Component",
                options=["All", "API Server", "Database", "Kafka", "Spark", "Redis"],
                index=0,
            )

        with col4:
            status_filter = st.selectbox(
                "Status",
                options=["All", "Healthy", "Degraded", "Critical"],
                index=0,
            )

        # Store filter values in session state
        st.session_state.ops_time_range = time_range
        st.session_state.ops_alert_severity = alert_severity
        st.session_state.ops_component_filter = component_filter
        st.session_state.ops_status_filter = status_filter


def render() -> None:
    """Render the operational dashboard page."""
    st.title("🎛️ Operational Dashboard")
    st.markdown("Comprehensive operational monitoring for fraud detection, system health, data quality, and alerts")

    # Add filters
    render_operational_filters()

    # Load data
    with st.spinner("Loading operational dashboard data..."):
        data = load_operational_data()

    if not data:
        st.error("Failed to load operational dashboard data")
        return

    # Extract data components
    system_health = data.get("system_health", {})
    fraud_monitoring = data.get("fraud_monitoring", {})
    data_quality = data.get("data_quality", {})
    alerts_data = data.get("alerts", {})
    performance = data.get("performance", {})
    streaming = data.get("streaming", {})

    # Operational overview
    render_operational_overview(system_health)

    st.markdown("---")

    # Alert and notification panel
    render_alert_notification_panel(alerts_data)

    st.markdown("---")

    # Fraud detection monitoring
    render_fraud_monitoring_ops(fraud_monitoring)

    st.markdown("---")

    # System health monitoring
    render_system_health_ops(system_health)

    st.markdown("---")

    # Data quality monitoring
    render_data_quality_monitoring(data_quality)

    st.markdown("---")

    # Performance monitoring
    render_performance_monitoring(performance)

    st.markdown("---")

    # Streaming pipeline health
    render_streaming_pipeline_health(streaming)

    # Footer with last update time
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
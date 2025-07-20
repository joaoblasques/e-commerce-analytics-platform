"""
Real-time aggregations with windowing for streaming analytics.

This module provides comprehensive windowed aggregation capabilities:
- Time-based windowing (tumbling, sliding, session)
- Real-time KPIs and metrics
- Customer behavior aggregations
- Product performance metrics
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    stddev, first, last, collect_list, collect_set, 
    window, current_timestamp, when, coalesce, lit,
    desc, asc, row_number, rank, dense_rank,
    lag, lead, countDistinct, approx_count_distinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)

from src.utils.logger import get_logger


class StreamingAggregator:
    """
    Comprehensive real-time aggregation engine for streaming data.
    
    Provides windowed aggregations including:
    - Time-based KPIs (revenue, orders, customers)
    - Customer behavior analytics 
    - Product performance metrics
    - Session and engagement analysis
    - Real-time alerting metrics
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize streaming aggregator.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.logger = get_logger(__name__)
        
    def create_real_time_kpis(
        self, 
        transaction_df: DataFrame,
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute"
    ) -> DataFrame:
        """
        Create real-time KPI aggregations from transaction stream.
        
        Args:
            transaction_df: Transaction stream DataFrame
            window_duration: Window size (e.g., "5 minutes", "1 hour")
            slide_duration: Slide interval (e.g., "1 minute", "30 seconds")
            
        Returns:
            DataFrame with windowed KPI metrics
        """
        try:
            # Create time windows
            windowed_df = transaction_df.withWatermark("timestamp", "2 minutes") \
                .groupBy(
                    window(col("timestamp"), window_duration, slide_duration),
                    col("location").alias("region")
                ) \
                .agg(
                    # Revenue metrics
                    spark_sum("total_amount").alias("total_revenue"),
                    avg("total_amount").alias("avg_order_value"),
                    spark_max("total_amount").alias("max_order_value"),
                    spark_min("total_amount").alias("min_order_value"),
                    
                    # Volume metrics  
                    count("*").alias("total_orders"),
                    countDistinct("user_id").alias("unique_customers"),
                    countDistinct("product_id").alias("unique_products"),
                    spark_sum("quantity").alias("total_items_sold"),
                    
                    # Customer metrics
                    avg("customer_lifetime_value").alias("avg_customer_ltv"),
                    countDistinct("customer_tier").alias("customer_tiers_active"),
                    
                    # Product metrics
                    countDistinct("category").alias("categories_sold"),
                    avg("price").alias("avg_product_price"),
                    
                    # Quality metrics
                    spark_sum(when(col("risk_category") == "High", 1).otherwise(0)).alias("high_risk_orders"),
                    spark_sum(when(col("payment_method") == "credit_card", 1).otherwise(0)).alias("credit_card_orders"),
                    
                    # Collection timestamp
                    current_timestamp().alias("aggregation_timestamp")
                )
            
            # Add calculated metrics
            kpi_df = windowed_df.withColumn(
                "revenue_per_customer",
                col("total_revenue") / col("unique_customers")
            ).withColumn(
                "items_per_order", 
                col("total_items_sold") / col("total_orders")
            ).withColumn(
                "high_risk_percentage",
                (col("high_risk_orders") * 100.0) / col("total_orders")
            ).withColumn(
                "credit_card_percentage",
                (col("credit_card_orders") * 100.0) / col("total_orders")
            )
            
            self.logger.info(f"Created real-time KPIs with {window_duration} windows")
            return kpi_df
            
        except Exception as e:
            self.logger.error(f"Error creating real-time KPIs: {e}")
            raise
    
    def create_customer_behavior_aggregations(
        self,
        behavior_df: DataFrame,
        window_duration: str = "10 minutes",
        slide_duration: str = "2 minutes"
    ) -> DataFrame:
        """
        Create customer behavior aggregations from user behavior stream.
        
        Args:
            behavior_df: User behavior stream DataFrame
            window_duration: Window size
            slide_duration: Slide interval
            
        Returns:
            DataFrame with customer behavior metrics
        """
        try:
            # Create time windows for behavior analysis
            windowed_df = behavior_df.withWatermark("timestamp", "3 minutes") \
                .groupBy(
                    window(col("timestamp"), window_duration, slide_duration),
                    col("user_id")
                ) \
                .agg(
                    # Activity metrics
                    count("*").alias("total_events"),
                    countDistinct("event_type").alias("unique_event_types"),
                    countDistinct("session_id").alias("session_count"),
                    countDistinct("page_url").alias("unique_pages_visited"),
                    
                    # Engagement metrics
                    spark_sum("engagement_score").alias("total_engagement_score"),
                    avg("engagement_score").alias("avg_engagement_score"),
                    spark_max("engagement_score").alias("max_engagement_score"),
                    
                    # Session metrics
                    spark_sum("estimated_session_duration").alias("total_session_time"),
                    avg("estimated_session_duration").alias("avg_session_duration"),
                    spark_max("page_depth").alias("max_page_depth"),
                    
                    # Conversion metrics
                    avg("purchase_intent_score").alias("avg_purchase_intent"),
                    spark_max("purchase_intent_score").alias("max_purchase_intent"),
                    
                    # Device metrics
                    first("device_category").alias("primary_device"),
                    first("browser_family").alias("primary_browser"),
                    
                    # Event breakdown
                    spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
                    spark_sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
                    spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
                    spark_sum(when(col("event_type") == "search", 1).otherwise(0)).alias("search_events"),
                    spark_sum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("checkout_events"),
                    
                    # Collection timestamp
                    current_timestamp().alias("aggregation_timestamp")
                )
            
            # Add calculated behavior metrics
            behavior_metrics_df = windowed_df.withColumn(
                "engagement_per_event",
                col("total_engagement_score") / col("total_events")
            ).withColumn(
                "session_efficiency", 
                col("total_engagement_score") / col("total_session_time") * 60  # per minute
            ).withColumn(
                "conversion_rate",
                when(col("page_views") > 0, 
                     col("checkout_events") * 100.0 / col("page_views"))
                .otherwise(lit(0.0))
            ).withColumn(
                "cart_conversion_rate",
                when(col("product_views") > 0,
                     col("add_to_cart_events") * 100.0 / col("product_views"))
                .otherwise(lit(0.0))
            ).withColumn(
                "activity_intensity",
                when(col("total_events") >= 20, lit("High"))
                .when(col("total_events") >= 10, lit("Medium"))
                .when(col("total_events") >= 5, lit("Low"))
                .otherwise(lit("Minimal"))
            )
            
            self.logger.info(f"Created customer behavior aggregations with {window_duration} windows")
            return behavior_metrics_df
            
        except Exception as e:
            self.logger.error(f"Error creating customer behavior aggregations: {e}")
            raise
    
    def create_product_performance_aggregations(
        self,
        transaction_df: DataFrame,
        window_duration: str = "15 minutes",
        slide_duration: str = "5 minutes"
    ) -> DataFrame:
        """
        Create product performance aggregations from transaction stream.
        
        Args:
            transaction_df: Transaction stream DataFrame
            window_duration: Window size
            slide_duration: Slide interval
            
        Returns:
            DataFrame with product performance metrics
        """
        try:
            # Create time windows for product analysis
            windowed_df = transaction_df.withWatermark("timestamp", "2 minutes") \
                .groupBy(
                    window(col("timestamp"), window_duration, slide_duration),
                    col("product_id"),
                    col("category"),
                    col("product_category_main")
                ) \
                .agg(
                    # Sales metrics
                    count("*").alias("order_count"),
                    spark_sum("quantity").alias("units_sold"),
                    spark_sum("total_amount").alias("revenue"),
                    avg("total_amount").alias("avg_order_value"),
                    
                    # Price metrics
                    avg("price").alias("avg_selling_price"),
                    spark_max("price").alias("max_selling_price"), 
                    spark_min("price").alias("min_selling_price"),
                    
                    # Customer metrics
                    countDistinct("user_id").alias("unique_buyers"),
                    avg("customer_lifetime_value").alias("avg_buyer_ltv"),
                    
                    # Geographic metrics
                    countDistinct("location").alias("geographic_reach"),
                    first("location").alias("top_location"),  # Simplified
                    
                    # Quality metrics
                    spark_sum(when(col("risk_category") == "High", 1).otherwise(0)).alias("high_risk_sales"),
                    avg("popularity_score").alias("avg_popularity_score"),
                    
                    # Collection timestamp
                    current_timestamp().alias("aggregation_timestamp")
                )
            
            # Add calculated product metrics
            product_metrics_df = windowed_df.withColumn(
                "revenue_per_unit",
                col("revenue") / col("units_sold")
            ).withColumn(
                "buyer_penetration",
                col("unique_buyers") / col("order_count")  # Customers per order
            ).withColumn(
                "risk_percentage",
                when(col("order_count") > 0,
                     col("high_risk_sales") * 100.0 / col("order_count"))
                .otherwise(lit(0.0))
            ).withColumn(
                "performance_score",
                (col("units_sold") * 0.3 + 
                 col("revenue") * 0.5 + 
                 col("unique_buyers") * 0.2).cast(IntegerType())
            ).withColumn(
                "velocity_category",
                when(col("units_sold") >= 50, lit("Fast Moving"))
                .when(col("units_sold") >= 20, lit("Medium Moving"))
                .when(col("units_sold") >= 5, lit("Slow Moving"))
                .otherwise(lit("Very Slow"))
            )
            
            self.logger.info(f"Created product performance aggregations with {window_duration} windows")
            return product_metrics_df
            
        except Exception as e:
            self.logger.error(f"Error creating product performance aggregations: {e}")
            raise
    
    def create_session_aggregations(
        self,
        behavior_df: DataFrame,
        session_timeout: str = "30 minutes"
    ) -> DataFrame:
        """
        Create session-based aggregations using session windows.
        
        Args:
            behavior_df: User behavior stream DataFrame
            session_timeout: Session timeout duration
            
        Returns:
            DataFrame with session-based metrics
        """
        try:
            # Session window aggregations (requires Spark 3.2+)
            # For compatibility, we'll use time-based approximation
            windowed_df = behavior_df.withWatermark("timestamp", "5 minutes") \
                .groupBy(
                    window(col("timestamp"), session_timeout),
                    col("user_id"),
                    col("session_id")
                ) \
                .agg(
                    # Session duration and activity
                    count("*").alias("events_in_session"),
                    spark_max("timestamp").alias("session_end_time"),
                    spark_min("timestamp").alias("session_start_time"),
                    countDistinct("event_type").alias("unique_event_types"),
                    
                    # Engagement in session
                    spark_sum("engagement_score").alias("session_engagement_score"),
                    avg("engagement_score").alias("avg_event_engagement"),
                    spark_max("page_depth").alias("max_page_depth_reached"),
                    
                    # Funnel progression
                    spark_max("purchase_intent_score").alias("max_purchase_intent"),
                    first("funnel_stage").alias("entry_funnel_stage"),
                    last("funnel_stage").alias("exit_funnel_stage"),
                    
                    # Device consistency
                    countDistinct("device_category").alias("device_switches"),
                    first("device_category").alias("session_device"),
                    
                    # Collection timestamp
                    current_timestamp().alias("aggregation_timestamp")
                )
            
            # Calculate session duration and metrics
            session_metrics_df = windowed_df.withColumn(
                "session_duration_seconds",
                (col("session_end_time").cast("long") - 
                 col("session_start_time").cast("long"))
            ).withColumn(
                "session_duration_minutes",
                col("session_duration_seconds") / 60.0
            ).withColumn(
                "engagement_per_minute",
                when(col("session_duration_minutes") > 0,
                     col("session_engagement_score") / col("session_duration_minutes"))
                .otherwise(lit(0.0))
            ).withColumn(
                "events_per_minute",
                when(col("session_duration_minutes") > 0,
                     col("events_in_session") / col("session_duration_minutes"))
                .otherwise(lit(0.0))
            ).withColumn(
                "session_quality",
                when(col("session_engagement_score") >= 300, lit("High"))
                .when(col("session_engagement_score") >= 150, lit("Medium"))
                .when(col("session_engagement_score") >= 50, lit("Low"))
                .otherwise(lit("Poor"))
            ).withColumn(
                "funnel_progression",
                when((col("entry_funnel_stage") == "Awareness") & 
                     (col("exit_funnel_stage") == "Purchase"), lit("Full Conversion"))
                .when(col("exit_funnel_stage") != col("entry_funnel_stage"), lit("Progressed"))
                .otherwise(lit("No Progression"))
            )
            
            self.logger.info(f"Created session aggregations with {session_timeout} timeout")
            return session_metrics_df
            
        except Exception as e:
            self.logger.error(f"Error creating session aggregations: {e}")
            raise
    
    def create_alert_aggregations(
        self,
        transaction_df: DataFrame,
        behavior_df: DataFrame,
        alert_window: str = "5 minutes"
    ) -> DataFrame:
        """
        Create aggregations for real-time alerting and monitoring.
        
        Args:
            transaction_df: Transaction stream DataFrame
            behavior_df: Behavior stream DataFrame
            alert_window: Alert evaluation window
            
        Returns:
            DataFrame with alert metrics
        """
        try:
            # Transaction-based alerts
            tx_alerts = transaction_df.withWatermark("timestamp", "1 minute") \
                .groupBy(window(col("timestamp"), alert_window)) \
                .agg(
                    # Volume alerts
                    count("*").alias("tx_volume"),
                    spark_sum("total_amount").alias("tx_revenue"),
                    countDistinct("user_id").alias("tx_unique_users"),
                    
                    # Risk alerts
                    spark_sum(when(col("risk_category") == "High", 1).otherwise(0)).alias("high_risk_count"),
                    spark_sum(when(col("total_amount") > 1000, 1).otherwise(0)).alias("large_transaction_count"),
                    
                    # Quality alerts
                    spark_sum(when(col("anomaly_flags") != "none", 1).otherwise(0)).alias("anomaly_count"),
                    
                    current_timestamp().alias("alert_timestamp")
                )
            
            # Behavior-based alerts  
            behavior_alerts = behavior_df.withWatermark("timestamp", "1 minute") \
                .groupBy(window(col("timestamp"), alert_window)) \
                .agg(
                    # Activity alerts
                    count("*").alias("behavior_volume"),
                    countDistinct("user_id").alias("behavior_unique_users"),
                    countDistinct("session_id").alias("active_sessions"),
                    
                    # Engagement alerts
                    avg("engagement_score").alias("avg_engagement"),
                    spark_sum(when(col("engagement_level") == "High", 1).otherwise(0)).alias("high_engagement_events"),
                    
                    current_timestamp().alias("alert_timestamp")
                )
            
            # Combine alerts (simplified join)
            combined_alerts = tx_alerts.join(
                behavior_alerts, 
                tx_alerts.window == behavior_alerts.window,
                "full_outer"
            ).select(
                coalesce(tx_alerts.window, behavior_alerts.window).alias("time_window"),
                coalesce(tx_alerts.tx_volume, lit(0)).alias("transaction_volume"),
                coalesce(tx_alerts.tx_revenue, lit(0.0)).alias("transaction_revenue"),
                coalesce(tx_alerts.high_risk_count, lit(0)).alias("high_risk_transactions"),
                coalesce(tx_alerts.anomaly_count, lit(0)).alias("anomalies_detected"),
                coalesce(behavior_alerts.behavior_volume, lit(0)).alias("behavior_events"),
                coalesce(behavior_alerts.active_sessions, lit(0)).alias("active_sessions"),
                coalesce(behavior_alerts.avg_engagement, lit(0.0)).alias("avg_engagement_score"),
                current_timestamp().alias("alert_evaluation_time")
            )
            
            # Add alert flags
            alert_df = combined_alerts.withColumn(
                "volume_alert",
                when(col("transaction_volume") < 10, lit("LOW_VOLUME"))
                .when(col("transaction_volume") > 1000, lit("HIGH_VOLUME"))
                .otherwise(lit("NORMAL"))
            ).withColumn(
                "risk_alert",
                when(col("high_risk_transactions") > 5, lit("HIGH_RISK_DETECTED"))
                .otherwise(lit("NORMAL"))
            ).withColumn(
                "anomaly_alert",
                when(col("anomalies_detected") > 0, lit("ANOMALIES_DETECTED"))
                .otherwise(lit("NORMAL"))
            ).withColumn(
                "engagement_alert",
                when(col("avg_engagement_score") < 30, lit("LOW_ENGAGEMENT"))
                .when(col("avg_engagement_score") > 80, lit("HIGH_ENGAGEMENT"))
                .otherwise(lit("NORMAL"))
            )
            
            self.logger.info(f"Created alert aggregations with {alert_window} window")
            return alert_df
            
        except Exception as e:
            self.logger.error(f"Error creating alert aggregations: {e}")
            raise
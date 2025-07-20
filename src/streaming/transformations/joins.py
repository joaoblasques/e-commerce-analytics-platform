"""
Stream-to-stream joins for real-time data correlation.

This module provides comprehensive streaming join capabilities:
- Stream-to-stream joins with time windows
- Stream-to-static data joins
- Complex multi-stream correlations
- Late arrival and watermark handling
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, current_timestamp,
    window, expr, broadcast, monotonically_increasing_id,
    lag, lead, first, last, collect_list, collect_set,
    datediff, unix_timestamp, from_unixtime,
    greatest, least, abs as spark_abs
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)

from src.utils.logger import get_logger


class StreamJoinEngine:
    """
    Comprehensive stream joining engine for real-time data correlation.
    
    Provides various join patterns:
    - Transaction and behavior correlation
    - User journey reconstruction  
    - Cross-stream event correlation
    - Late data handling with watermarks
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize stream join engine.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.logger = get_logger(__name__)
        
    def join_transaction_behavior_streams(
        self,
        transaction_df: DataFrame,
        behavior_df: DataFrame,
        join_window: str = "10 minutes",
        watermark_delay: str = "2 minutes"
    ) -> DataFrame:
        """
        Join transaction and behavior streams to correlate purchases with user activity.
        
        Args:
            transaction_df: Transaction stream DataFrame
            behavior_df: User behavior stream DataFrame  
            join_window: Time window for join correlation
            watermark_delay: Watermark delay for late data
            
        Returns:
            Joined DataFrame with correlated transaction and behavior data
        """
        try:
            # Add watermarks for both streams
            tx_watermarked = transaction_df.withWatermark("timestamp", watermark_delay)
            behavior_watermarked = behavior_df.withWatermark("timestamp", watermark_delay)
            
            # Prepare transaction stream for join
            tx_prepared = tx_watermarked.select(
                col("user_id").alias("tx_user_id"),
                col("timestamp").alias("tx_timestamp"),
                col("transaction_id").alias("tx_id"),
                col("product_id").alias("tx_product_id"),
                col("total_amount").alias("tx_amount"),
                col("payment_method").alias("tx_payment_method"),
                col("location").alias("tx_location"),
                col("customer_tier").alias("tx_customer_tier"),
                col("risk_category").alias("tx_risk_category"),
                col("transaction_size").alias("tx_size"),
                current_timestamp().alias("tx_processing_time")
            )
            
            # Prepare behavior stream for join  
            behavior_prepared = behavior_watermarked.select(
                col("user_id").alias("behavior_user_id"),
                col("timestamp").alias("behavior_timestamp"),
                col("session_id").alias("behavior_session_id"),
                col("event_type").alias("behavior_event_type"),
                col("page_url").alias("behavior_page_url"),
                col("product_id").alias("behavior_product_id"),
                col("engagement_score").alias("behavior_engagement"),
                col("purchase_intent_score").alias("behavior_intent"),
                col("device_category").alias("behavior_device"),
                col("funnel_stage").alias("behavior_funnel_stage"),
                current_timestamp().alias("behavior_processing_time")
            )
            
            # Stream-to-stream join with time constraints
            joined_df = tx_prepared.join(
                behavior_prepared,
                expr(f"""
                    tx_user_id = behavior_user_id AND
                    tx_timestamp >= behavior_timestamp AND  
                    tx_timestamp <= behavior_timestamp + interval '{join_window}'
                """),
                "inner"
            )
            
            # Add correlation metrics
            correlated_df = joined_df.withColumn(
                "time_to_purchase_minutes",
                (col("tx_timestamp").cast("long") - col("behavior_timestamp").cast("long")) / 60
            ).withColumn(
                "product_match",
                when(col("tx_product_id") == col("behavior_product_id"), lit(True))
                .otherwise(lit(False))
            ).withColumn(
                "intent_to_purchase_score",
                when(col("product_match") == True, 
                     col("behavior_intent") + lit(20))  # Boost for product match
                .otherwise(col("behavior_intent"))
            ).withColumn(
                "conversion_path",
                when(col("behavior_event_type") == "add_to_cart", lit("Direct Cart"))
                .when(col("behavior_event_type") == "product_view", lit("Product Browse"))
                .when(col("behavior_event_type") == "search", lit("Search Driven"))
                .otherwise(lit("Other"))
            ).withColumn(
                "purchase_velocity",
                when(col("time_to_purchase_minutes") <= 5, lit("Immediate"))
                .when(col("time_to_purchase_minutes") <= 30, lit("Quick"))
                .when(col("time_to_purchase_minutes") <= 120, lit("Considered"))
                .otherwise(lit("Delayed"))
            ).withColumn(
                "correlation_timestamp",
                current_timestamp()
            )
            
            self.logger.info(f"Successfully joined transaction and behavior streams with {join_window} window")
            return correlated_df
            
        except Exception as e:
            self.logger.error(f"Error joining transaction and behavior streams: {e}")
            raise
    
    def create_user_journey_stream(
        self,
        behavior_df: DataFrame,
        transaction_df: DataFrame,
        journey_window: str = "4 hours"
    ) -> DataFrame:
        """
        Create comprehensive user journey by joining multiple interaction points.
        
        Args:
            behavior_df: User behavior stream DataFrame
            transaction_df: Transaction stream DataFrame
            journey_window: Time window for journey reconstruction
            
        Returns:
            DataFrame with reconstructed user journeys
        """
        try:
            # Prepare behavior events with sequence
            behavior_with_sequence = behavior_df.withWatermark("timestamp", "5 minutes") \
                .withColumn(
                    "event_sequence",
                    row_number().over(
                        Window.partitionBy("user_id", "session_id")
                        .orderBy("timestamp")
                    )
                ).select(
                    col("user_id"),
                    col("session_id"),
                    col("timestamp").alias("event_timestamp"),
                    col("event_type"),
                    col("page_url"),
                    col("product_id"),
                    col("engagement_score"),
                    col("funnel_stage"),
                    col("event_sequence"),
                    col("purchase_intent_score")
                )
            
            # Prepare transaction events
            transaction_events = transaction_df.withWatermark("timestamp", "5 minutes") \
                .select(
                    col("user_id"),
                    col("timestamp").alias("purchase_timestamp"),
                    col("transaction_id"),
                    col("product_id").alias("purchased_product_id"),
                    col("total_amount"),
                    col("payment_method"),
                    lit("purchase").alias("event_type_tx")
                )
            
            # Join to create complete journey  
            journey_df = behavior_with_sequence.join(
                transaction_events,
                (behavior_with_sequence.user_id == transaction_events.user_id) &
                (transaction_events.purchase_timestamp >= behavior_with_sequence.event_timestamp) &
                (transaction_events.purchase_timestamp <= 
                 behavior_with_sequence.event_timestamp + expr(f"interval '{journey_window}'")),
                "left_outer"
            )
            
            # Add journey analytics
            journey_analyzed = journey_df.withColumn(
                "led_to_purchase",
                when(col("transaction_id").isNotNull(), lit(True)).otherwise(lit(False))
            ).withColumn(
                "time_to_purchase_hours",
                when(col("purchase_timestamp").isNotNull(),
                     (col("purchase_timestamp").cast("long") - col("event_timestamp").cast("long")) / 3600)
                .otherwise(lit(None))
            ).withColumn(
                "journey_effectiveness",
                when(col("led_to_purchase") == True,
                     when(col("event_type") == "add_to_cart", lit(100))
                     .when(col("event_type") == "product_view", lit(80))
                     .when(col("event_type") == "search", lit(60))
                     .when(col("event_type") == "page_view", lit(40))
                     .otherwise(lit(20)))
                .otherwise(lit(0))
            ).withColumn(
                "product_correlation",
                when(col("product_id") == col("purchased_product_id"), lit("Direct"))
                .when(col("product_id").isNotNull() & col("purchased_product_id").isNotNull(), lit("Related"))
                .when(col("purchased_product_id").isNotNull(), lit("Different"))
                .otherwise(lit("No Purchase"))
            ).withColumn(
                "journey_stage_impact",
                when(col("funnel_stage") == "Purchase", lit(5))
                .when(col("funnel_stage") == "Intent", lit(4))
                .when(col("funnel_stage") == "Consideration", lit(3))
                .when(col("funnel_stage") == "Interest", lit(2))
                .when(col("funnel_stage") == "Awareness", lit(1))
                .otherwise(lit(0))
            ).withColumn(
                "journey_timestamp",
                current_timestamp()
            )
            
            self.logger.info(f"Created user journey stream with {journey_window} window")
            return journey_analyzed
            
        except Exception as e:
            self.logger.error(f"Error creating user journey stream: {e}")
            raise
    
    def join_with_customer_profile_stream(
        self,
        transaction_df: DataFrame,
        customer_profile_updates_df: DataFrame,
        profile_window: str = "1 hour"
    ) -> DataFrame:
        """
        Join transaction stream with dynamic customer profile updates.
        
        Args:
            transaction_df: Transaction stream DataFrame
            customer_profile_updates_df: Customer profile update stream DataFrame
            profile_window: Window for profile lookup
            
        Returns:
            DataFrame with enriched customer profile information
        """
        try:
            # Prepare customer profile updates with versioning
            profile_versioned = customer_profile_updates_df.withWatermark("timestamp", "10 minutes") \
                .withColumn(
                    "profile_version",
                    row_number().over(
                        Window.partitionBy("user_id")
                        .orderBy(col("timestamp").desc())
                    )
                ).select(
                    col("user_id").alias("profile_user_id"),
                    col("timestamp").alias("profile_update_timestamp"),
                    col("customer_tier").alias("current_customer_tier"),
                    col("customer_lifetime_value").alias("current_ltv"),
                    col("total_orders").alias("current_total_orders"),
                    col("account_status").alias("current_account_status"),
                    col("registration_date"),
                    col("last_login"),
                    col("profile_version")
                )
            
            # Get latest profile for each customer
            latest_profiles = profile_versioned.filter(col("profile_version") == 1)
            
            # Join transactions with latest customer profiles
            enriched_df = transaction_df.withWatermark("timestamp", "5 minutes") \
                .join(
                    broadcast(latest_profiles),  # Broadcast for efficiency
                    col("user_id") == col("profile_user_id"),
                    "left_outer"
                )
            
            # Add profile-based enrichments
            profile_enriched = enriched_df.withColumn(
                "customer_tenure_days",
                when(col("registration_date").isNotNull(),
                     datediff(col("timestamp"), col("registration_date")))
                .otherwise(lit(None))
            ).withColumn(
                "days_since_last_login",
                when(col("last_login").isNotNull(),
                     datediff(col("timestamp"), col("last_login")))
                .otherwise(lit(None))
            ).withColumn(
                "tier_upgrade_eligible",
                when((col("current_customer_tier") == "Bronze") & (col("current_ltv") > 500), lit(True))
                .when((col("current_customer_tier") == "Silver") & (col("current_ltv") > 1000), lit(True))
                .when((col("current_customer_tier") == "Gold") & (col("current_ltv") > 2000), lit(True))
                .otherwise(lit(False))
            ).withColumn(
                "customer_segment",
                when(col("customer_tenure_days") < 30, lit("New"))
                .when(col("days_since_last_login") > 90, lit("At Risk"))
                .when(col("current_total_orders") > 10, lit("Loyal"))
                .when(col("current_ltv") > 1000, lit("VIP"))
                .otherwise(lit("Regular"))
            ).withColumn(
                "profile_freshness",
                when(col("profile_update_timestamp").isNotNull(),
                     (col("timestamp").cast("long") - col("profile_update_timestamp").cast("long")) / 3600)
                .otherwise(lit(null))
            ).withColumn(
                "profile_join_timestamp",
                current_timestamp()
            )
            
            self.logger.info(f"Joined transactions with customer profiles using {profile_window} window")
            return profile_enriched
            
        except Exception as e:
            self.logger.error(f"Error joining with customer profile stream: {e}")
            raise
    
    def create_cross_stream_correlation(
        self,
        stream_a: DataFrame,
        stream_b: DataFrame,
        correlation_keys: List[str],
        time_column_a: str = "timestamp",
        time_column_b: str = "timestamp",
        correlation_window: str = "15 minutes",
        correlation_type: str = "inner"
    ) -> DataFrame:
        """
        Create generic cross-stream correlation for any two streams.
        
        Args:
            stream_a: First stream DataFrame
            stream_b: Second stream DataFrame  
            correlation_keys: List of columns to join on
            time_column_a: Timestamp column in stream A
            time_column_b: Timestamp column in stream B
            correlation_window: Time window for correlation
            correlation_type: Join type (inner, left_outer, etc.)
            
        Returns:
            DataFrame with correlated data from both streams
        """
        try:
            # Add watermarks
            stream_a_watermarked = stream_a.withWatermark(time_column_a, "2 minutes")
            stream_b_watermarked = stream_b.withWatermark(time_column_b, "2 minutes")
            
            # Prepare streams with prefixes to avoid column conflicts
            stream_a_prefixed = stream_a_watermarked.select(
                *[col(c).alias(f"a_{c}") for c in stream_a_watermarked.columns]
            )
            
            stream_b_prefixed = stream_b_watermarked.select(
                *[col(c).alias(f"b_{c}") for c in stream_b_watermarked.columns]
            )
            
            # Build join condition
            key_conditions = [
                col(f"a_{key}") == col(f"b_{key}") for key in correlation_keys
            ]
            
            time_condition = expr(f"""
                a_{time_column_a} >= b_{time_column_b} - interval '{correlation_window}' AND
                a_{time_column_a} <= b_{time_column_b} + interval '{correlation_window}'
            """)
            
            all_conditions = key_conditions + [time_condition]
            final_condition = all_conditions[0]
            for condition in all_conditions[1:]:
                final_condition = final_condition & condition
            
            # Perform join
            correlated_df = stream_a_prefixed.join(
                stream_b_prefixed,
                final_condition,
                correlation_type
            )
            
            # Add correlation metadata
            result_df = correlated_df.withColumn(
                "correlation_timestamp", 
                current_timestamp()
            ).withColumn(
                "correlation_window_used",
                lit(correlation_window)
            ).withColumn(
                "correlation_keys_used",
                lit(",".join(correlation_keys))
            ).withColumn(
                "time_difference_seconds",
                when(col(f"a_{time_column_a}").isNotNull() & col(f"b_{time_column_b}").isNotNull(),
                     spark_abs(col(f"a_{time_column_a}").cast("long") - col(f"b_{time_column_b}").cast("long")))
                .otherwise(lit(None))
            )
            
            self.logger.info(f"Created cross-stream correlation on keys {correlation_keys} with {correlation_window} window")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error creating cross-stream correlation: {e}")
            raise
    
    def create_sessionized_join(
        self,
        behavior_df: DataFrame,
        transaction_df: DataFrame,
        session_gap: str = "30 minutes"
    ) -> DataFrame:
        """
        Join streams using sessionization logic for better temporal correlation.
        
        Args:
            behavior_df: User behavior stream DataFrame
            transaction_df: Transaction stream DataFrame
            session_gap: Gap between events to define session boundary
            
        Returns:
            DataFrame with session-based correlation
        """
        try:
            # Create session boundaries for behavior stream
            behavior_sessionized = behavior_df.withWatermark("timestamp", "5 minutes") \
                .withColumn(
                    "prev_timestamp",
                    lag("timestamp").over(
                        Window.partitionBy("user_id").orderBy("timestamp")
                    )
                ).withColumn(
                    "time_gap_minutes",
                    when(col("prev_timestamp").isNotNull(),
                         (col("timestamp").cast("long") - col("prev_timestamp").cast("long")) / 60)
                    .otherwise(lit(0))
                ).withColumn(
                    "session_boundary",
                    when(col("time_gap_minutes") > expr(f"interval '{session_gap}'").cast("long") / 60, lit(1))
                    .otherwise(lit(0))
                ).withColumn(
                    "session_group",
                    spark_sum("session_boundary").over(
                        Window.partitionBy("user_id").orderBy("timestamp")
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                    )
                ).select(
                    col("user_id").alias("behavior_user_id"),
                    col("timestamp").alias("behavior_timestamp"),
                    col("session_id").alias("original_session_id"),
                    col("session_group").alias("computed_session_group"),
                    col("event_type").alias("behavior_event_type"),
                    col("product_id").alias("behavior_product_id"),
                    col("engagement_score").alias("behavior_engagement"),
                    col("funnel_stage").alias("behavior_funnel_stage")
                )
            
            # Prepare transactions with session context
            transaction_sessionized = transaction_df.withWatermark("timestamp", "5 minutes") \
                .select(
                    col("user_id").alias("tx_user_id"),
                    col("timestamp").alias("tx_timestamp"),
                    col("transaction_id"),
                    col("product_id").alias("tx_product_id"),
                    col("total_amount"),
                    col("payment_method")
                )
            
            # Join based on temporal proximity within sessions
            session_joined = behavior_sessionized.join(
                transaction_sessionized,
                (col("behavior_user_id") == col("tx_user_id")) &
                (col("tx_timestamp") >= col("behavior_timestamp")) &
                (col("tx_timestamp") <= col("behavior_timestamp") + expr(f"interval '{session_gap}'")),
                "left_outer"
            )
            
            # Add session analytics
            session_analyzed = session_joined.withColumn(
                "within_session_purchase",
                when(col("transaction_id").isNotNull(), lit(True)).otherwise(lit(False))
            ).withColumn(
                "session_conversion_score",
                when(col("within_session_purchase") == True,
                     col("behavior_engagement") + lit(50))
                .otherwise(col("behavior_engagement"))
            ).withColumn(
                "time_to_conversion_minutes",
                when(col("tx_timestamp").isNotNull(),
                     (col("tx_timestamp").cast("long") - col("behavior_timestamp").cast("long")) / 60)
                .otherwise(lit(None))
            ).withColumn(
                "session_product_match",
                when(col("behavior_product_id") == col("tx_product_id"), lit("Exact Match"))
                .when(col("behavior_product_id").isNotNull() & col("tx_product_id").isNotNull(), lit("Different Product"))
                .when(col("tx_product_id").isNotNull(), lit("No Behavior Product"))
                .otherwise(lit("No Purchase"))
            ).withColumn(
                "session_join_timestamp",
                current_timestamp()
            )
            
            self.logger.info(f"Created sessionized join with {session_gap} session gap")
            return session_analyzed
            
        except Exception as e:
            self.logger.error(f"Error creating sessionized join: {e}")
            raise
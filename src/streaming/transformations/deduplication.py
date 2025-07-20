"""
Stream deduplication for handling duplicate events and ensuring data quality.

This module provides comprehensive deduplication capabilities:
- Event-based deduplication with configurable keys
- Time-window based deduplication
- Exact and fuzzy duplicate detection
- Late arrival handling and reprocessing
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, current_timestamp, hash,
    concat_ws, md5, sha2, monotonically_increasing_id,
    window, row_number, rank, dense_rank, first, last,
    max as spark_max, min as spark_min, count, countDistinct,
    collect_list, collect_set, struct, array, map_keys, map_values,
    greatest, least, abs as spark_abs, regexp_replace, trim, upper
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType, LongType
)

from src.utils.logger import get_logger


class StreamDeduplicator:
    """
    Comprehensive stream deduplication engine for ensuring data quality.
    
    Provides various deduplication strategies:
    - Exact duplicate removal
    - Fuzzy duplicate detection
    - Time-window based deduplication
    - Business key deduplication
    - Late arrival handling
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize stream deduplicator.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.logger = get_logger(__name__)
        
    def deduplicate_exact_events(
        self,
        df: DataFrame,
        dedup_keys: List[str],
        watermark_column: str = "timestamp",
        watermark_delay: str = "10 minutes",
        keep_strategy: str = "first"
    ) -> DataFrame:
        """
        Remove exact duplicate events based on specified keys.
        
        Args:
            df: Input stream DataFrame
            dedup_keys: List of columns that define uniqueness
            watermark_column: Column used for watermarking
            watermark_delay: Delay for handling late data
            keep_strategy: Strategy for keeping duplicates ("first", "last", "latest_timestamp")
            
        Returns:
            Deduplicated DataFrame
        """
        try:
            # Add watermark for late data handling
            watermarked_df = df.withWatermark(watermark_column, watermark_delay)
            
            # Create a composite key for deduplication
            composite_key_expr = concat_ws("||", *[col(key) for key in dedup_keys])
            df_with_key = watermarked_df.withColumn("dedup_composite_key", composite_key_expr)
            
            # Add row number based on strategy
            if keep_strategy == "first":
                window_spec = Window.partitionBy("dedup_composite_key").orderBy(col(watermark_column).asc())
            elif keep_strategy == "last":
                window_spec = Window.partitionBy("dedup_composite_key").orderBy(col(watermark_column).desc())
            elif keep_strategy == "latest_timestamp":
                window_spec = Window.partitionBy("dedup_composite_key").orderBy(col(watermark_column).desc())
            else:
                raise ValueError(f"Unknown keep_strategy: {keep_strategy}")
            
            df_with_rank = df_with_key.withColumn(
                "dedup_rank",
                row_number().over(window_spec)
            )
            
            # Keep only the first occurrence based on strategy
            deduplicated_df = df_with_rank.filter(col("dedup_rank") == 1)
            
            # Add deduplication metadata
            result_df = deduplicated_df.withColumn(
                "deduplication_timestamp", current_timestamp()
            ).withColumn(
                "deduplication_method", lit("exact")
            ).withColumn(
                "deduplication_keys", lit(",".join(dedup_keys))
            ).withColumn(
                "deduplication_strategy", lit(keep_strategy)
            ).drop("dedup_composite_key", "dedup_rank")
            
            self.logger.info(f"Applied exact deduplication on keys: {dedup_keys} with strategy: {keep_strategy}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error in exact deduplication: {e}")
            raise
    
    def deduplicate_transactions(
        self,
        transaction_df: DataFrame,
        watermark_delay: str = "5 minutes",
        similarity_threshold: float = 0.95
    ) -> DataFrame:
        """
        Deduplicate transaction events with business logic.
        
        Args:
            transaction_df: Transaction stream DataFrame
            watermark_delay: Delay for handling late data
            similarity_threshold: Threshold for fuzzy matching (0.0 to 1.0)
            
        Returns:
            Deduplicated transaction DataFrame
        """
        try:
            # Add watermark
            watermarked_df = transaction_df.withWatermark("timestamp", watermark_delay)
            
            # Create business key for exact deduplication
            business_key_df = watermarked_df.withColumn(
                "business_key",
                concat_ws("||", 
                    col("user_id"),
                    col("product_id"), 
                    col("total_amount"),
                    col("payment_method")
                )
            )
            
            # Exact deduplication on business key
            exact_dedup_window = Window.partitionBy("business_key").orderBy("timestamp")
            exact_dedup_df = business_key_df.withColumn(
                "exact_rank",
                row_number().over(exact_dedup_window)
            ).filter(col("exact_rank") == 1)
            
            # Fuzzy deduplication for similar transactions
            # Group by user and time window for fuzzy matching
            fuzzy_window_spec = Window.partitionBy("user_id", 
                window(col("timestamp"), "10 minutes")) \
                .orderBy("timestamp")
            
            fuzzy_analysis_df = exact_dedup_df.withColumn(
                "prev_amount", 
                lag("total_amount").over(fuzzy_window_spec)
            ).withColumn(
                "prev_product",
                lag("product_id").over(fuzzy_window_spec)
            ).withColumn(
                "prev_payment",
                lag("payment_method").over(fuzzy_window_spec)
            ).withColumn(
                "prev_timestamp",
                lag("timestamp").over(fuzzy_window_spec)
            )
            
            # Calculate similarity score
            similarity_df = fuzzy_analysis_df.withColumn(
                "amount_similarity",
                when(col("prev_amount").isNotNull(),
                     1.0 - spark_abs(col("total_amount") - col("prev_amount")) / 
                     greatest(col("total_amount"), col("prev_amount")))
                .otherwise(lit(0.0))
            ).withColumn(
                "product_similarity",
                when(col("product_id") == col("prev_product"), lit(1.0))
                .otherwise(lit(0.0))
            ).withColumn(
                "payment_similarity", 
                when(col("payment_method") == col("prev_payment"), lit(1.0))
                .otherwise(lit(0.0))
            ).withColumn(
                "time_diff_seconds",
                when(col("prev_timestamp").isNotNull(),
                     col("timestamp").cast("long") - col("prev_timestamp").cast("long"))
                .otherwise(lit(null))
            ).withColumn(
                "time_similarity",
                when(col("time_diff_seconds").isNotNull() & (col("time_diff_seconds") <= 300),  # 5 minutes
                     1.0 - col("time_diff_seconds") / 300.0)
                .otherwise(lit(0.0))
            )
            
            # Combined similarity score
            similarity_scored_df = similarity_df.withColumn(
                "overall_similarity",
                (col("amount_similarity") * 0.4 + 
                 col("product_similarity") * 0.3 + 
                 col("payment_similarity") * 0.2 + 
                 col("time_similarity") * 0.1)
            )
            
            # Mark potential fuzzy duplicates
            fuzzy_flagged_df = similarity_scored_df.withColumn(
                "is_fuzzy_duplicate",
                when(col("overall_similarity") >= similarity_threshold, lit(True))
                .otherwise(lit(False))
            ).withColumn(
                "duplicate_type",
                when(col("exact_rank") > 1, lit("exact"))
                .when(col("is_fuzzy_duplicate") == True, lit("fuzzy"))
                .otherwise(lit("unique"))
            )
            
            # Keep only non-fuzzy duplicates
            final_dedup_df = fuzzy_flagged_df.filter(col("is_fuzzy_duplicate") == False)
            
            # Add deduplication metadata
            result_df = final_dedup_df.withColumn(
                "deduplication_timestamp", current_timestamp()
            ).withColumn(
                "deduplication_method", lit("transaction_business_logic")
            ).withColumn(
                "similarity_threshold_used", lit(similarity_threshold)
            ).select(
                # Original transaction columns
                col("user_id"), col("transaction_id"), col("product_id"), 
                col("timestamp"), col("total_amount"), col("payment_method"),
                col("location"), col("quantity"), col("price"),
                # Deduplication metadata
                col("duplicate_type"), col("overall_similarity"),
                col("deduplication_timestamp"), col("deduplication_method"),
                col("similarity_threshold_used")
            )
            
            self.logger.info(f"Applied transaction deduplication with {similarity_threshold} similarity threshold")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error in transaction deduplication: {e}")
            raise
    
    def deduplicate_user_behavior(
        self,
        behavior_df: DataFrame,
        session_dedup: bool = True,
        event_dedup_window: str = "30 seconds",
        watermark_delay: str = "2 minutes"
    ) -> DataFrame:
        """
        Deduplicate user behavior events with session and temporal logic.
        
        Args:
            behavior_df: User behavior stream DataFrame
            session_dedup: Whether to deduplicate within sessions
            event_dedup_window: Time window for event deduplication
            watermark_delay: Delay for handling late data
            
        Returns:
            Deduplicated behavior DataFrame
        """
        try:
            # Add watermark
            watermarked_df = behavior_df.withWatermark("timestamp", watermark_delay)
            
            if session_dedup:
                # Session-based deduplication
                session_dedup_keys = ["user_id", "session_id", "event_type", "page_url"]
                session_window = Window.partitionBy(session_dedup_keys).orderBy("timestamp")
                
                session_dedup_df = watermarked_df.withColumn(
                    "session_event_rank",
                    row_number().over(session_window)
                ).withColumn(
                    "prev_event_timestamp",
                    lag("timestamp").over(session_window)
                ).withColumn(
                    "time_since_prev_event",
                    when(col("prev_event_timestamp").isNotNull(),
                         col("timestamp").cast("long") - col("prev_event_timestamp").cast("long"))
                    .otherwise(lit(null))
                )
                
                # Keep events that are either first in session or sufficiently spaced
                time_threshold = int(event_dedup_window.split()[0]) if event_dedup_window.endswith("seconds") else 30
                
                filtered_df = session_dedup_df.filter(
                    (col("session_event_rank") == 1) |
                    (col("time_since_prev_event") > time_threshold)
                )
            else:
                # Simple temporal deduplication
                temporal_keys = ["user_id", "event_type", "page_url"]
                temporal_window = Window.partitionBy(temporal_keys).orderBy("timestamp")
                
                filtered_df = watermarked_df.withColumn(
                    "temporal_rank", 
                    row_number().over(temporal_window)
                ).filter(col("temporal_rank") == 1)
            
            # Add rapid-fire event detection
            user_window = Window.partitionBy("user_id").orderBy("timestamp")
            rapid_fire_df = filtered_df.withColumn(
                "prev_user_timestamp",
                lag("timestamp").over(user_window)
            ).withColumn(
                "time_since_prev_user_event",
                when(col("prev_user_timestamp").isNotNull(),
                     col("timestamp").cast("long") - col("prev_user_timestamp").cast("long"))
                .otherwise(lit(null))
            ).withColumn(
                "is_rapid_fire",
                when(col("time_since_prev_user_event") <= 1, lit(True))  # 1 second threshold
                .otherwise(lit(False))
            )
            
            # Product view deduplication (special case)
            product_view_df = rapid_fire_df.withColumn(
                "product_view_key",
                when(col("event_type") == "product_view",
                     concat_ws("||", col("user_id"), col("product_id")))
                .otherwise(lit(null))
            )
            
            product_window = Window.partitionBy("product_view_key", 
                window(col("timestamp"), "5 minutes")).orderBy("timestamp")
            
            product_dedup_df = product_view_df.withColumn(
                "product_view_rank",
                when(col("product_view_key").isNotNull(),
                     row_number().over(product_window))
                .otherwise(lit(1))
            ).filter(col("product_view_rank") == 1)
            
            # Add deduplication metadata
            result_df = product_dedup_df.withColumn(
                "deduplication_timestamp", current_timestamp()
            ).withColumn(
                "deduplication_method", 
                when(session_dedup, lit("session_and_temporal"))
                .otherwise(lit("temporal_only"))
            ).withColumn(
                "rapid_fire_filtered", 
                col("is_rapid_fire") == False
            ).select(
                # Original behavior columns
                col("user_id"), col("session_id"), col("event_type"),
                col("timestamp"), col("page_url"), col("product_id"),
                col("user_agent"), col("ip_address"),
                # Deduplication metadata
                col("is_rapid_fire"), col("rapid_fire_filtered"),
                col("deduplication_timestamp"), col("deduplication_method")
            )
            
            self.logger.info(f"Applied user behavior deduplication with session_dedup={session_dedup}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error in user behavior deduplication: {e}")
            raise
    
    def create_deduplication_statistics(
        self,
        original_df: DataFrame,
        deduplicated_df: DataFrame,
        dedup_keys: List[str],
        window_duration: str = "5 minutes"
    ) -> DataFrame:
        """
        Create statistics about the deduplication process.
        
        Args:
            original_df: Original DataFrame before deduplication
            deduplicated_df: DataFrame after deduplication
            dedup_keys: Keys used for deduplication
            window_duration: Window for statistical analysis
            
        Returns:
            DataFrame with deduplication statistics
        """
        try:
            # Count records in time windows for both dataframes
            original_counts = original_df.withWatermark("timestamp", "2 minutes") \
                .groupBy(window(col("timestamp"), window_duration)) \
                .agg(
                    count("*").alias("original_count"),
                    countDistinct(*dedup_keys).alias("original_unique_keys")
                )
            
            dedup_counts = deduplicated_df.withWatermark("timestamp", "2 minutes") \
                .groupBy(window(col("timestamp"), window_duration)) \
                .agg(
                    count("*").alias("deduplicated_count"),
                    countDistinct(*dedup_keys).alias("deduplicated_unique_keys")
                )
            
            # Join to create comparison statistics
            stats_df = original_counts.join(
                dedup_counts,
                original_counts.window == dedup_counts.window,
                "full_outer"
            ).select(
                coalesce(original_counts.window, dedup_counts.window).alias("time_window"),
                coalesce(col("original_count"), lit(0)).alias("original_count"),
                coalesce(col("deduplicated_count"), lit(0)).alias("deduplicated_count"),
                coalesce(col("original_unique_keys"), lit(0)).alias("original_unique_keys"),
                coalesce(col("deduplicated_unique_keys"), lit(0)).alias("deduplicated_unique_keys")
            )
            
            # Calculate deduplication metrics
            metrics_df = stats_df.withColumn(
                "duplicates_removed",
                col("original_count") - col("deduplicated_count")
            ).withColumn(
                "deduplication_rate",
                when(col("original_count") > 0,
                     (col("original_count") - col("deduplicated_count")) * 100.0 / col("original_count"))
                .otherwise(lit(0.0))
            ).withColumn(
                "data_quality_improvement",
                when(col("original_count") > 0,
                     col("deduplicated_count") * 100.0 / col("original_unique_keys"))
                .otherwise(lit(100.0))
            ).withColumn(
                "efficiency_score",
                when(col("original_count") > 0,
                     (col("deduplicated_unique_keys") * 100.0) / col("original_count"))
                .otherwise(lit(100.0))
            ).withColumn(
                "statistics_timestamp",
                current_timestamp()
            ).withColumn(
                "dedup_keys_analyzed",
                lit(",".join(dedup_keys))
            )
            
            self.logger.info(f"Created deduplication statistics for keys: {dedup_keys}")
            return metrics_df
            
        except Exception as e:
            self.logger.error(f"Error creating deduplication statistics: {e}")
            raise
    
    def handle_late_arrivals(
        self,
        df: DataFrame,
        watermark_column: str = "timestamp",
        grace_period: str = "15 minutes",
        late_data_strategy: str = "flag"
    ) -> DataFrame:
        """
        Handle late-arriving data in the deduplication process.
        
        Args:
            df: Input DataFrame with potential late arrivals
            watermark_column: Column used for watermarking
            grace_period: Grace period for accepting late data
            late_data_strategy: Strategy for handling late data ("flag", "drop", "separate")
            
        Returns:
            DataFrame with late arrival handling applied
        """
        try:
            # Add watermark with grace period
            watermarked_df = df.withWatermark(watermark_column, grace_period)
            
            # Calculate event processing delay
            delay_df = watermarked_df.withColumn(
                "processing_timestamp", current_timestamp()
            ).withColumn(
                "processing_delay_seconds",
                col("processing_timestamp").cast("long") - col(watermark_column).cast("long")
            ).withColumn(
                "is_late_arrival",
                col("processing_delay_seconds") > expr(f"interval '{grace_period}'").cast("long")
            )
            
            if late_data_strategy == "flag":
                # Keep all data but flag late arrivals
                result_df = delay_df.withColumn(
                    "late_arrival_flag", col("is_late_arrival")
                ).withColumn(
                    "data_quality_flag",
                    when(col("is_late_arrival") == True, lit("late_arrival"))
                    .otherwise(lit("on_time"))
                )
                
            elif late_data_strategy == "drop":
                # Drop late arrivals
                result_df = delay_df.filter(col("is_late_arrival") == False) \
                    .withColumn("late_arrival_handling", lit("dropped_late_data"))
                
            elif late_data_strategy == "separate":
                # Add metadata for separate processing
                result_df = delay_df.withColumn(
                    "processing_stream",
                    when(col("is_late_arrival") == True, lit("late_data_stream"))
                    .otherwise(lit("main_stream"))
                ).withColumn(
                    "requires_reprocessing", col("is_late_arrival")
                )
            else:
                raise ValueError(f"Unknown late_data_strategy: {late_data_strategy}")
            
            # Add late arrival statistics
            final_df = result_df.withColumn(
                "late_arrival_strategy_used", lit(late_data_strategy)
            ).withColumn(
                "grace_period_used", lit(grace_period)
            ).withColumn(
                "late_arrival_processing_timestamp", current_timestamp()
            )
            
            self.logger.info(f"Applied late arrival handling with strategy: {late_data_strategy}")
            return final_df
            
        except Exception as e:
            self.logger.error(f"Error handling late arrivals: {e}")
            raise
"""
Kafka Structured Streaming Consumers for E-Commerce Analytics Platform

This module provides robust Kafka consumers using Spark Structured Streaming
with comprehensive error handling, schema validation, checkpointing, and
backpressure management.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    isnan,
    isnull,
    regexp_extract,
    split,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, when
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.data_quality import DataQualityEngine
from src.utils.logger import get_logger


class StreamingConsumerError(Exception):
    """Custom exception for streaming consumer errors."""

    pass


class SchemaValidator:
    """Schema validation utilities for streaming data."""

    @staticmethod
    def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
        """Validate DataFrame schema against expected schema."""
        actual_schema = df.schema
        return actual_schema == expected_schema

    @staticmethod
    def get_schema_diff(
        actual_schema: StructType, expected_schema: StructType
    ) -> Dict[str, List[str]]:
        """Get differences between actual and expected schemas."""
        actual_fields = {field.name: field for field in actual_schema.fields}
        expected_fields = {field.name: field for field in expected_schema.fields}

        missing_fields = set(expected_fields.keys()) - set(actual_fields.keys())
        extra_fields = set(actual_fields.keys()) - set(expected_fields.keys())
        type_mismatches = []

        for field_name in set(actual_fields.keys()) & set(expected_fields.keys()):
            if (
                actual_fields[field_name].dataType
                != expected_fields[field_name].dataType
            ):
                type_mismatches.append(
                    f"{field_name}: expected {expected_fields[field_name].dataType}, "
                    f"got {actual_fields[field_name].dataType}"
                )

        return {
            "missing_fields": list(missing_fields),
            "extra_fields": list(extra_fields),
            "type_mismatches": type_mismatches,
        }


class DataQualityChecker:
    """Data quality validation for streaming DataFrames."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def check_nulls(self, df: DataFrame, critical_columns: List[str]) -> DataFrame:
        """Check for null values in critical columns."""
        for column in critical_columns:
            if column in df.columns:
                null_count = df.select(
                    spark_sum(col(column).isNull().cast("int")).alias("null_count")
                ).collect()[0]["null_count"]
                if null_count > 0:
                    self.logger.warning(
                        f"Found {null_count} null values in critical column: {column}"
                    )
        return df

    def check_data_freshness(
        self, df: DataFrame, timestamp_column: str, max_age_seconds: int = 300
    ) -> DataFrame:
        """Check if data is fresh (within specified age)."""
        if timestamp_column in df.columns:
            current_time = current_timestamp()
            df_with_age = df.withColumn(
                "data_age_seconds",
                (current_time.cast("long") - col(timestamp_column).cast("long")),
            )

            stale_data = df_with_age.filter(col("data_age_seconds") > max_age_seconds)
            stale_count = stale_data.count()

            if stale_count > 0:
                self.logger.warning(
                    f"Found {stale_count} stale records (older than {max_age_seconds} seconds)"
                )

            return df_with_age.drop("data_age_seconds")
        return df


class BaseStreamingConsumer(ABC):
    """Base class for Kafka Structured Streaming consumers."""

    def __init__(
        self,
        spark: SparkSession,
        kafka_bootstrap_servers: str = "localhost:9092",
        topic: str = "",
        consumer_group: str = "",
        checkpoint_location: str = "/tmp/streaming_checkpoints",
        max_offsets_per_trigger: int = 1000,
        enable_backpressure: bool = True,
    ):
        """Initialize base streaming consumer.

        Args:
            spark: SparkSession instance
            kafka_bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
            consumer_group: Consumer group ID
            checkpoint_location: Checkpoint directory path
            max_offsets_per_trigger: Maximum offsets to process per trigger
            enable_backpressure: Enable adaptive query execution backpressure
        """
        self.spark = spark
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.checkpoint_location = (
            f"{checkpoint_location}/{self.topic}_{self.consumer_group}"
        )
        self.max_offsets_per_trigger = max_offsets_per_trigger
        self.enable_backpressure = enable_backpressure

        self.logger = get_logger(self.__class__.__name__)
        self.schema_validator = SchemaValidator()
        self.quality_checker = DataQualityChecker(self.logger)

        # Initialize comprehensive data quality engine
        self.data_quality_engine = DataQualityEngine(self.spark)
        self.enable_data_quality = True  # Flag to enable/disable quality checks

        # Configure Spark settings for streaming
        self._configure_spark_settings()

        # Streaming query reference
        self.streaming_query: Optional[StreamingQuery] = None

        # Error handling
        self.error_callback: Optional[Callable[[Exception], None]] = None

    def _configure_spark_settings(self) -> None:
        """Configure Spark settings for optimal streaming performance."""
        spark_conf = self.spark.conf

        # Backpressure configuration
        if self.enable_backpressure:
            spark_conf.set("spark.sql.streaming.kafka.consumer.pollTimeoutMs", "512")
            spark_conf.set(
                "spark.sql.streaming.kafka.consumer.fetchOffset.numRetries", "3"
            )
            spark_conf.set(
                "spark.sql.streaming.kafka.consumer.fetchOffset.retryIntervalMs", "64"
            )

        # Checkpoint configuration
        spark_conf.set(
            "spark.sql.streaming.checkpointLocation", self.checkpoint_location
        )

        # Memory and performance optimization
        spark_conf.set("spark.sql.streaming.metricsEnabled", "true")
        spark_conf.set("spark.sql.streaming.ui.enabled", "true")

        self.logger.info("Configured Spark settings for streaming")

    def apply_data_quality_checks(self, df: DataFrame, stream_type: str) -> DataFrame:
        """
        Apply comprehensive data quality checks to streaming data.

        Args:
            df: Input DataFrame to check
            stream_type: Type of stream (transaction, user_behavior, etc.)

        Returns:
            DataFrame with quality flags and high-quality data filtering
        """
        if not self.enable_data_quality:
            return df

        try:
            # Apply comprehensive data quality assessment
            quality_df, quality_report = self.data_quality_engine.assess_data_quality(
                df,
                stream_type=stream_type,
                enable_profiling=False,  # Disable profiling for performance
            )

            # Log quality metrics
            self.logger.info(f"Data Quality Assessment for {stream_type}:")
            self.logger.info(f"  - Quality Level: {quality_report.quality_level.value}")
            self.logger.info(
                f"  - Valid Records: {quality_report.valid_records}/{quality_report.total_records}"
            )
            self.logger.info(
                f"  - Completeness: {quality_report.completeness_score:.2%}"
            )
            self.logger.info(f"  - Anomalies: {quality_report.anomaly_count}")

            # Log recommendations if any
            if quality_report.recommendations:
                self.logger.info("  - Recommendations:")
                for rec in quality_report.recommendations[
                    :3
                ]:  # Log top 3 recommendations
                    self.logger.info(f"    * {rec}")

            # Filter out poor quality data (configurable threshold)
            quality_threshold = 50  # Minimum quality score to keep records
            high_quality_df = quality_df.filter(
                col("validation_passed")
                & col("completeness_passed")
                & (col("data_quality_score") >= quality_threshold)
            )

            # Log filtering results
            original_count = df.count()
            filtered_count = high_quality_df.count()
            filtered_percentage = (filtered_count / max(original_count, 1)) * 100

            self.logger.info(
                f"Quality filtering: {filtered_count}/{original_count} records retained ({filtered_percentage:.1f}%)"
            )

            # Create quality monitoring stream (for alerts/dashboards)
            quality_monitoring_df = (
                self.data_quality_engine.create_quality_monitoring_stream(quality_df)
            )

            # TODO: In production, you might want to write quality_monitoring_df to a monitoring topic
            # quality_monitoring_df.writeStream.format("kafka").option("topic", "data-quality-monitoring").start()

            return high_quality_df

        except Exception as e:
            self.logger.error(f"Error in data quality checks: {e}")
            # Return original data if quality checks fail to avoid breaking the pipeline
            return df

    @abstractmethod
    def get_schema(self) -> StructType:
        """Return the expected schema for the Kafka messages."""
        pass

    @abstractmethod
    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Transform the streaming DataFrame."""
        pass

    def create_kafka_stream(self) -> DataFrame:
        """Create a Kafka streaming DataFrame."""
        try:
            # Base Kafka stream configuration
            kafka_options = {
                "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
                "subscribe": self.topic,
                "startingOffsets": "latest",
                "maxOffsetsPerTrigger": str(self.max_offsets_per_trigger),
                "failOnDataLoss": "false",  # Handle data loss gracefully
                "kafka.consumer.commit.groupid": self.consumer_group,
            }

            self.logger.info(f"Creating Kafka stream for topic: {self.topic}")

            # Create the streaming DataFrame
            kafka_df = (
                self.spark.readStream.format("kafka").options(**kafka_options).load()
            )

            # Parse JSON messages and apply schema
            schema = self.get_schema()
            parsed_df = (
                kafka_df.selectExpr(
                    "CAST(value AS STRING) as json_value",
                    "timestamp",
                    "partition",
                    "offset",
                )
                .select(
                    from_json(col("json_value"), schema).alias("data"),
                    col("timestamp").alias("kafka_timestamp"),
                    col("partition"),
                    col("offset"),
                )
                .select("data.*", "kafka_timestamp", "partition", "offset")
            )

            # Validate schema
            self._validate_stream_schema(parsed_df, schema)

            # Apply data quality checks
            parsed_df = self._apply_quality_checks(parsed_df)

            return parsed_df

        except Exception as e:
            self.logger.error(f"Failed to create Kafka stream: {e}")
            if self.error_callback:
                self.error_callback(e)
            raise StreamingConsumerError(f"Failed to create Kafka stream: {e}")

    def _validate_stream_schema(
        self, df: DataFrame, expected_schema: StructType
    ) -> None:
        """Validate the streaming DataFrame schema."""
        # Get the data column schema (excluding metadata columns)
        data_schema = StructType(
            [
                field
                for field in df.schema.fields
                if field.name not in ["kafka_timestamp", "partition", "offset"]
            ]
        )

        if not self.schema_validator.validate_schema(
            df.select("data.*"), expected_schema
        ):
            schema_diff = self.schema_validator.get_schema_diff(
                data_schema, expected_schema
            )
            self.logger.warning(f"Schema validation failed for topic {self.topic}")
            self.logger.warning(f"Schema differences: {schema_diff}")

            # Optionally fail fast on schema mismatch
            # raise StreamingConsumerError(f"Schema validation failed: {schema_diff}")

    def _apply_quality_checks(self, df: DataFrame) -> DataFrame:
        """Apply data quality checks to the streaming DataFrame."""
        # Define critical columns based on the schema
        critical_columns = self._get_critical_columns()

        # Check for nulls in critical columns
        df = self.quality_checker.check_nulls(df, critical_columns)

        # Check data freshness if timestamp column exists
        timestamp_columns = ["timestamp", "event_timestamp", "created_at"]
        for ts_col in timestamp_columns:
            if ts_col in df.columns:
                df = self.quality_checker.check_data_freshness(df, ts_col)
                break

        return df

    def _get_critical_columns(self) -> List[str]:
        """Get list of critical columns that should not be null."""
        # Override in subclasses for topic-specific critical columns
        return ["timestamp"]

    def start_stream(
        self,
        output_mode: str = "append",
        trigger_interval: str = "5 seconds",
        output_format: str = "console",
        output_options: Optional[Dict[str, str]] = None,
        foreach_batch_function: Optional[Callable[[DataFrame, int], None]] = None,
    ) -> StreamingQuery:
        """Start the streaming query.

        Args:
            output_mode: Streaming output mode (append, update, complete)
            trigger_interval: Trigger processing interval
            output_format: Output format (console, memory, parquet, delta, etc.)
            output_options: Additional output options
            foreach_batch_function: Function to process each micro-batch

        Returns:
            StreamingQuery instance
        """
        try:
            # Create Kafka stream
            stream_df = self.create_kafka_stream()

            # Apply transformations
            transformed_df = self.transform_stream(stream_df)

            # Configure the streaming writer
            writer = (
                transformed_df.writeStream.outputMode(output_mode)
                .trigger(processingTime=trigger_interval)
                .option("checkpointLocation", self.checkpoint_location)
            )

            # Configure output
            if foreach_batch_function:
                writer = writer.foreachBatch(foreach_batch_function)
            else:
                writer = writer.format(output_format)
                if output_options:
                    for key, value in output_options.items():
                        writer = writer.option(key, value)

            # Start the query
            self.streaming_query = writer.start()

            self.logger.info(f"Started streaming query for topic {self.topic}")
            self.logger.info(f"Query ID: {self.streaming_query.id}")
            self.logger.info(f"Checkpoint location: {self.checkpoint_location}")

            return self.streaming_query

        except Exception as e:
            self.logger.error(f"Failed to start streaming query: {e}")
            if self.error_callback:
                self.error_callback(e)
            raise StreamingConsumerError(f"Failed to start streaming query: {e}")

    def stop_stream(self) -> None:
        """Stop the streaming query gracefully."""
        if self.streaming_query and self.streaming_query.isActive:
            try:
                self.logger.info("Stopping streaming query...")
                self.streaming_query.stop()
                self.logger.info("Streaming query stopped successfully")
            except Exception as e:
                self.logger.error(f"Error stopping streaming query: {e}")
                raise

    def wait_for_termination(self, timeout: Optional[int] = None) -> None:
        """Wait for the streaming query to terminate.

        Args:
            timeout: Optional timeout in seconds
        """
        if self.streaming_query:
            try:
                if timeout:
                    self.streaming_query.awaitTermination(timeout)
                else:
                    self.streaming_query.awaitTermination()
            except Exception as e:
                self.logger.error(f"Error waiting for termination: {e}")
                raise

    def get_stream_status(self) -> Dict[str, Any]:
        """Get current streaming query status."""
        if not self.streaming_query:
            return {"status": "not_started"}

        try:
            progress = self.streaming_query.lastProgress
            status = self.streaming_query.status

            return {
                "status": "active" if self.streaming_query.isActive else "stopped",
                "id": str(self.streaming_query.id),
                "run_id": str(self.streaming_query.runId),
                "name": self.streaming_query.name,
                "timestamp": progress.get("timestamp") if progress else None,
                "input_rows_per_second": progress.get("inputRowsPerSecond")
                if progress
                else None,
                "processed_rows_per_second": progress.get("processedRowsPerSecond")
                if progress
                else None,
                "batch_duration": progress.get("durationMs", {}).get("triggerExecution")
                if progress
                else None,
                "message": status.get("message") if status else None,
                "is_data_available": status.get("isDataAvailable") if status else None,
                "is_trigger_active": status.get("isTriggerActive") if status else None,
            }
        except Exception as e:
            self.logger.error(f"Error getting stream status: {e}")
            return {"status": "error", "error": str(e)}

    def set_error_callback(self, callback: Callable[[Exception], None]) -> None:
        """Set callback function for error handling."""
        self.error_callback = callback


class TransactionStreamConsumer(BaseStreamingConsumer):
    """Streaming consumer for e-commerce transaction data."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.topic:
            self.topic = "transactions"
        if not self.consumer_group:
            self.consumer_group = "transaction-analytics-group"

    def get_schema(self) -> StructType:
        """Return the expected schema for transaction messages."""
        return StructType(
            [
                StructField("transaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), True),
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), True),
                StructField("category_id", StringType(), True),
                StructField("category_name", StringType(), True),
                StructField("subcategory", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("discount_applied", DoubleType(), True),
                StructField("payment_method", StringType(), True),
                StructField("payment_provider", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("timestamp", TimestampType(), False),
                StructField(
                    "user_location",
                    StructType(
                        [
                            StructField("country", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("city", StringType(), True),
                            StructField("zip_code", StringType(), True),
                            StructField("latitude", DoubleType(), True),
                            StructField("longitude", DoubleType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "device_info",
                    StructType(
                        [
                            StructField("type", StringType(), True),
                            StructField("os", StringType(), True),
                            StructField("browser", StringType(), True),
                            StructField("user_agent", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "marketing_attribution",
                    StructType(
                        [
                            StructField("campaign_id", StringType(), True),
                            StructField("channel", StringType(), True),
                            StructField("referrer", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Transform transaction stream data with comprehensive enrichment."""
        from src.streaming.transformations import (
            DataEnrichmentPipeline,
            StreamDeduplicator,
        )

        # Initialize transformation components
        enrichment_pipeline = DataEnrichmentPipeline(self.spark)
        deduplicator = StreamDeduplicator(self.spark)

        # Add computed columns (basic transformation)
        transformed_df = (
            df.withColumn(
                "total_amount", col("price") * col("quantity") - col("discount_applied")
            )
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn(
                "hour_of_day", col("timestamp").cast("string").substr(12, 2).cast("int")
            )
            .withColumn(
                "day_of_week",
                when(
                    col("timestamp").isNotNull(),
                    (col("timestamp").cast("long") / 86400) % 7,
                ).otherwise(0),
            )
        )

        # Filter out invalid transactions
        valid_df = transformed_df.filter(
            (col("price") > 0) & (col("quantity") > 0) & (col("total_amount") >= 0)
        )

        # Apply deduplication
        deduplicated_df = deduplicator.deduplicate_transactions(
            valid_df, watermark_delay="5 minutes", similarity_threshold=0.95
        )

        # Apply data quality checks before enrichment
        quality_checked_df = self.apply_data_quality_checks(
            deduplicated_df, "transaction"
        )

        # Apply comprehensive enrichment
        enriched_df = enrichment_pipeline.enrich_transaction_stream(quality_checked_df)

        return enriched_df

    def _get_critical_columns(self) -> List[str]:
        """Get critical columns for transaction data."""
        return [
            "transaction_id",
            "user_id",
            "product_id",
            "price",
            "quantity",
            "timestamp",
        ]


class UserBehaviorStreamConsumer(BaseStreamingConsumer):
    """Streaming consumer for user behavior event data."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.topic:
            self.topic = "user-events"
        if not self.consumer_group:
            self.consumer_group = "user-behavior-analytics-group"

    def get_schema(self) -> StructType:
        """Return the expected schema for user behavior messages."""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("event_category", StringType(), True),
                StructField("page_url", StringType(), True),
                StructField("page_title", StringType(), True),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), True),
                StructField(
                    "event_properties", MapType(StringType(), StringType()), True
                ),
                StructField(
                    "user_location",
                    StructType(
                        [
                            StructField("country", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("city", StringType(), True),
                            StructField("ip_address", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "device_info",
                    StructType(
                        [
                            StructField("type", StringType(), True),
                            StructField("os", StringType(), True),
                            StructField("browser", StringType(), True),
                            StructField("screen_resolution", StringType(), True),
                            StructField("viewport_size", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Transform user behavior stream data with comprehensive enrichment."""
        from src.streaming.transformations import (
            DataEnrichmentPipeline,
            StreamDeduplicator,
        )

        # Initialize transformation components
        enrichment_pipeline = DataEnrichmentPipeline(self.spark)
        deduplicator = StreamDeduplicator(self.spark)

        # Add computed columns (basic transformation)
        transformed_df = (
            df.withColumn("processing_timestamp", current_timestamp())
            .withColumn(
                "hour_of_day", col("timestamp").cast("string").substr(12, 2).cast("int")
            )
            .withColumn(
                "is_mobile",
                when(col("device_info.type") == "mobile", True).otherwise(False),
            )
            .withColumn(
                "page_category",
                when(col("page_url").contains("/product/"), "product")
                .when(col("page_url").contains("/category/"), "category")
                .when(col("page_url").contains("/cart"), "cart")
                .when(col("page_url").contains("/checkout"), "checkout")
                .otherwise("other"),
            )
        )

        # Filter out invalid events
        valid_df = transformed_df.filter(
            col("event_type").isNotNull()
            & col("user_id").isNotNull()
            & col("session_id").isNotNull()
        )

        # Apply deduplication for user behavior
        deduplicated_df = deduplicator.deduplicate_user_behavior(
            valid_df,
            session_dedup=True,
            event_dedup_window="30 seconds",
            watermark_delay="2 minutes",
        )

        # Apply data quality checks before enrichment
        quality_checked_df = self.apply_data_quality_checks(
            deduplicated_df, "user_behavior"
        )

        # Apply comprehensive enrichment
        enriched_df = enrichment_pipeline.enrich_user_behavior_stream(
            quality_checked_df
        )

        return enriched_df

    def _get_critical_columns(self) -> List[str]:
        """Get critical columns for user behavior data."""
        return ["event_id", "user_id", "session_id", "event_type", "timestamp"]

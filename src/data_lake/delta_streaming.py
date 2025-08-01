"""
Delta Lake Streaming Integration

Provides streaming data integration with Delta Lake for real-time ACID transactions,
automatic schema evolution, and optimized streaming writes.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, when
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from ..utils.logger import setup_logging
from ..utils.spark_utils import get_secure_temp_dir
from .delta import DeltaLakeManager
from .delta_config import DeltaTableConfigurations, create_checkpoint_location

logger = setup_logging(__name__)


class DeltaStreamingManager:
    """
    Manages streaming data integration with Delta Lake, providing ACID guarantees
    for real-time data processing and automatic schema evolution.
    """

    def __init__(
        self,
        delta_manager: DeltaLakeManager,
        kafka_bootstrap_servers: str = "localhost:9092",
        checkpoint_base_path: str = None,
    ):
        """
        Initialize Delta Streaming Manager.

        Args:
            delta_manager: Delta Lake manager instance
            kafka_bootstrap_servers: Kafka bootstrap servers
            checkpoint_base_path: Base path for streaming checkpoints
        """
        self.delta_manager = delta_manager
        self.spark = delta_manager.spark
        self.kafka_servers = kafka_bootstrap_servers
        self.checkpoint_base_path = checkpoint_base_path or get_secure_temp_dir(
            "delta_streaming_checkpoints"
        )
        self.active_streams: Dict[str, StreamingQuery] = {}

        logger.info("Delta Streaming Manager initialized")
        logger.info(f"Kafka servers: {kafka_bootstrap_servers}")
        logger.info(f"Checkpoint base path: {checkpoint_base_path}")

    def create_kafka_stream(
        self,
        topic: str,
        schema: StructType,
        starting_offsets: str = "latest",
        max_offsets_per_trigger: Optional[int] = None,
    ) -> DataFrame:
        """
        Create Kafka streaming DataFrame with JSON deserialization.

        Args:
            topic: Kafka topic name
            schema: Expected message schema
            starting_offsets: Starting offset strategy
            max_offsets_per_trigger: Rate limiting for streaming

        Returns:
            Streaming DataFrame
        """
        # Build Kafka stream options
        kafka_options = {
            "kafka.bootstrap.servers": self.kafka_servers,
            "subscribe": topic,
            "startingOffsets": starting_offsets,
        }

        if max_offsets_per_trigger:
            kafka_options["maxOffsetsPerTrigger"] = str(max_offsets_per_trigger)

        # Read from Kafka
        kafka_df = self.spark.readStream.format("kafka").options(**kafka_options).load()

        # Parse JSON and extract schema
        parsed_df = (
            kafka_df.select(
                col("key").cast("string").alias("kafka_key"),
                from_json(col("value").cast("string"), schema).alias("data"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
            )
            .select(
                "kafka_key", "data.*", "topic", "partition", "offset", "kafka_timestamp"
            )
            .withColumn("ingestion_timestamp", current_timestamp())
        )

        logger.info(f"Created Kafka stream for topic '{topic}'")
        logger.info(f"Starting offsets: {starting_offsets}")

        return parsed_df

    def start_transaction_streaming(
        self,
        trigger_interval: str = "30 seconds",
        max_offsets_per_trigger: int = 10000,
    ) -> StreamingQuery:
        """
        Start streaming transactions to Delta Lake.

        Args:
            trigger_interval: Processing trigger interval
            max_offsets_per_trigger: Rate limiting

        Returns:
            StreamingQuery for transactions
        """
        # Get transaction table configuration
        config = DeltaTableConfigurations.transaction_table_config()

        # Ensure table exists
        if config["name"] not in self.delta_manager.tables:
            self.delta_manager.create_delta_table(
                table_name=config["name"],
                schema=config["schema"],
                partition_columns=config["partition_columns"],
                properties=config["properties"],
            )

        # Create Kafka stream
        stream_df = self.create_kafka_stream(
            topic="transactions",
            schema=config["schema"],
            max_offsets_per_trigger=max_offsets_per_trigger,
        )

        # Add metadata and processing columns
        enriched_df = (
            stream_df.withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
            # Ensure partition columns are present
            .withColumn(
                "year",
                col("timestamp").cast("date").cast("string").substr(1, 4).cast("int"),
            )
            .withColumn(
                "month",
                col("timestamp").cast("date").cast("string").substr(6, 2).cast("int"),
            )
            .withColumn(
                "day",
                col("timestamp").cast("date").cast("string").substr(9, 2).cast("int"),
            )
            .withColumn(
                "hour",
                col("timestamp")
                .cast("timestamp")
                .cast("string")
                .substr(12, 2)
                .cast("int"),
            )
        )

        # Create checkpoint location
        checkpoint_location = create_checkpoint_location(
            "transactions_streaming", self.checkpoint_base_path
        )

        # Start streaming query
        query = self.delta_manager.create_streaming_writer(
            df=enriched_df,
            table_name=config["name"],
            checkpoint_location=checkpoint_location,
            trigger_interval=trigger_interval,
            output_mode="append",
        )

        self.active_streams["transactions"] = query
        logger.info("Started transaction streaming to Delta Lake")

        return query

    def start_user_events_streaming(
        self,
        trigger_interval: str = "15 seconds",
        max_offsets_per_trigger: int = 50000,
    ) -> StreamingQuery:
        """
        Start streaming user events to Delta Lake.

        Args:
            trigger_interval: Processing trigger interval
            max_offsets_per_trigger: Rate limiting

        Returns:
            StreamingQuery for user events
        """
        # Get user events table configuration
        config = DeltaTableConfigurations.user_events_table_config()

        # Ensure table exists
        if config["name"] not in self.delta_manager.tables:
            self.delta_manager.create_delta_table(
                table_name=config["name"],
                schema=config["schema"],
                partition_columns=config["partition_columns"],
                properties=config["properties"],
            )

        # Create Kafka stream
        stream_df = self.create_kafka_stream(
            topic="user-events",
            schema=config["schema"],
            max_offsets_per_trigger=max_offsets_per_trigger,
        )

        # Add metadata and processing columns
        enriched_df = (
            stream_df.withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
            # Event type categorization for partitioning
            .withColumn(
                "event_type_category",
                when(col("event_type").isin(["page_view", "product_view"]), "browsing")
                .when(
                    col("event_type").isin(["add_to_cart", "remove_from_cart"]), "cart"
                )
                .when(
                    col("event_type").isin(
                        ["checkout_start", "checkout_complete", "purchase"]
                    ),
                    "conversion",
                )
                .when(col("event_type").isin(["search", "filter"]), "discovery")
                .otherwise("other"),
            )
            # Ensure partition columns are present
            .withColumn(
                "year",
                col("timestamp").cast("date").cast("string").substr(1, 4).cast("int"),
            )
            .withColumn(
                "month",
                col("timestamp").cast("date").cast("string").substr(6, 2).cast("int"),
            )
            .withColumn(
                "day",
                col("timestamp").cast("date").cast("string").substr(9, 2).cast("int"),
            )
        )

        # Create checkpoint location
        checkpoint_location = create_checkpoint_location(
            "user_events_streaming", self.checkpoint_base_path
        )

        # Start streaming query
        query = self.delta_manager.create_streaming_writer(
            df=enriched_df,
            table_name=config["name"],
            checkpoint_location=checkpoint_location,
            trigger_interval=trigger_interval,
            output_mode="append",
        )

        self.active_streams["user_events"] = query
        logger.info("Started user events streaming to Delta Lake")

        return query

    def start_customer_profile_streaming(
        self,
        trigger_interval: str = "60 seconds",
        max_offsets_per_trigger: int = 1000,
    ) -> StreamingQuery:
        """
        Start streaming customer profile updates to Delta Lake with merge logic.

        Args:
            trigger_interval: Processing trigger interval
            max_offsets_per_trigger: Rate limiting

        Returns:
            StreamingQuery for customer profiles
        """
        # Get customer profile table configuration
        config = DeltaTableConfigurations.customer_profile_table_config()

        # Ensure table exists
        if config["name"] not in self.delta_manager.tables:
            self.delta_manager.create_delta_table(
                table_name=config["name"],
                schema=config["schema"],
                partition_columns=config["partition_columns"],
                properties=config["properties"],
            )

        # Create Kafka stream
        stream_df = self.create_kafka_stream(
            topic="customer-profiles",
            schema=config["schema"],
            max_offsets_per_trigger=max_offsets_per_trigger,
        )

        # Add metadata columns
        enriched_df = (
            stream_df.withColumn("updated_at", current_timestamp())
            # Set created_at only for new records (this will be handled in merge logic)
            .withColumn(
                "created_at",
                when(col("created_at").isNull(), current_timestamp()).otherwise(
                    col("created_at")
                ),
            )
        )

        # Create checkpoint location
        checkpoint_location = create_checkpoint_location(
            "customer_profiles_streaming", self.checkpoint_base_path
        )

        # For customer profiles, we need merge logic instead of append
        # This requires a custom foreachBatch function
        def merge_customer_profiles(batch_df: DataFrame, batch_id: int):
            """Custom merge logic for customer profiles."""
            if not batch_df.isEmpty():
                logger.info(
                    f"Processing batch {batch_id} with {batch_df.count()} customer profile updates"
                )

                # Merge logic: update existing records, insert new ones
                self.delta_manager.write_to_delta(
                    df=batch_df,
                    table_name=config["name"],
                    mode="merge",
                    merge_condition="target.customer_id = source.customer_id",
                    update_condition={
                        "email": "source.email",
                        "first_name": "source.first_name",
                        "last_name": "source.last_name",
                        "phone": "source.phone",
                        "address": "source.address",
                        "city": "source.city",
                        "state": "source.state",
                        "country": "source.country",
                        "customer_segment": "source.customer_segment",
                        "customer_tier": "source.customer_tier",
                        "lifetime_value": "source.lifetime_value",
                        "average_order_value": "source.average_order_value",
                        "total_orders": "source.total_orders",
                        "total_spent": "source.total_spent",
                        "last_order_date": "source.last_order_date",
                        "is_active": "source.is_active",
                        "preferred_categories": "source.preferred_categories",
                        "updated_at": "source.updated_at",
                    },
                )

        # Start streaming query with custom merge logic
        query = (
            enriched_df.writeStream.foreachBatch(merge_customer_profiles)
            .outputMode("update")
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=trigger_interval)
            .start()
        )

        self.active_streams["customer_profiles"] = query
        logger.info(
            "Started customer profiles streaming to Delta Lake with merge logic"
        )

        return query

    def create_streaming_aggregations(
        self,
        watermark_column: str = "timestamp",
        watermark_threshold: str = "10 minutes",
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute",
    ) -> Dict[str, StreamingQuery]:
        """
        Create streaming aggregations with Delta Lake output.

        Args:
            watermark_column: Column for watermarking
            watermark_threshold: Late data threshold
            window_duration: Aggregation window duration
            slide_duration: Window slide duration

        Returns:
            Dictionary of aggregation streaming queries
        """
        queries = {}

        # Transaction aggregations
        if "transactions" in self.delta_manager.tables:
            transaction_df = self.spark.readStream.format("delta").load(
                self.delta_manager.tables["transactions"]
            )

            # Windowed transaction metrics
            transaction_agg = (
                transaction_df.withWatermark(watermark_column, watermark_threshold)
                .groupBy(
                    self.spark.sql(
                        f"window({watermark_column}, '{window_duration}', '{slide_duration}')"
                    ).alias("window"),
                    "category",
                    "customer_segment",
                )
                .agg(
                    {
                        "total_amount": "sum",
                        "quantity": "sum",
                        "transaction_id": "count",
                        "customer_id": "countDistinct",
                    }
                )
                .withColumn("result_type", lit("transaction_metrics"))
                .withColumn("analysis_date", col("window.start").cast("date"))
                .withColumn("metric_category", lit("real_time_aggregation"))
                .withColumn("created_at", current_timestamp())
                .withColumn(
                    "year", col("analysis_date").cast("string").substr(1, 4).cast("int")
                )
                .withColumn(
                    "month",
                    col("analysis_date").cast("string").substr(6, 2).cast("int"),
                )
            )

            # Create analytics results table if not exists
            analytics_config = DeltaTableConfigurations.analytics_results_table_config()
            if analytics_config["name"] not in self.delta_manager.tables:
                self.delta_manager.create_delta_table(
                    table_name=analytics_config["name"],
                    schema=analytics_config["schema"],
                    partition_columns=analytics_config["partition_columns"],
                    properties=analytics_config["properties"],
                )

            # Start aggregation query
            agg_checkpoint = create_checkpoint_location(
                "transaction_aggregations", self.checkpoint_base_path
            )

            queries["transaction_aggregations"] = (
                transaction_agg.writeStream.format("delta")
                .outputMode("append")
                .option("checkpointLocation", agg_checkpoint)
                .trigger(processingTime="2 minutes")
                .start(self.delta_manager.tables[analytics_config["name"]])
            )

            logger.info("Started transaction aggregation streaming")

        return queries

    def monitor_streaming_health(self) -> Dict[str, Dict[str, any]]:
        """
        Monitor health of all active streaming queries.

        Returns:
            Dictionary with health metrics for each stream
        """
        health_metrics = {}

        for stream_name, query in self.active_streams.items():
            if query.isActive:
                progress = query.lastProgress
                health_metrics[stream_name] = {
                    "status": "active",
                    "run_id": query.runId,
                    "batch_id": progress.get("batchId", -1) if progress else -1,
                    "input_rows_per_second": progress.get("inputRowsPerSecond", 0)
                    if progress
                    else 0,
                    "processed_rows_per_second": progress.get(
                        "processedRowsPerSecond", 0
                    )
                    if progress
                    else 0,
                    "batch_duration_ms": progress.get("batchDuration", 0)
                    if progress
                    else 0,
                    "timestamp": progress.get("timestamp") if progress else None,
                }
            else:
                health_metrics[stream_name] = {
                    "status": "inactive",
                    "run_id": query.runId,
                    "exception": str(query.exception) if query.exception else None,
                }

        logger.info(f"Health check completed for {len(health_metrics)} streams")
        return health_metrics

    def stop_stream(self, stream_name: str) -> bool:
        """
        Stop a specific streaming query.

        Args:
            stream_name: Name of the stream to stop

        Returns:
            True if stopped successfully
        """
        if stream_name in self.active_streams:
            query = self.active_streams[stream_name]
            query.stop()
            del self.active_streams[stream_name]
            logger.info(f"Stopped stream: {stream_name}")
            return True
        else:
            logger.warning(f"Stream '{stream_name}' not found in active streams")
            return False

    def stop_all_streams(self) -> int:
        """
        Stop all active streaming queries.

        Returns:
            Number of streams stopped
        """
        stopped_count = 0
        for stream_name in list(self.active_streams.keys()):
            if self.stop_stream(stream_name):
                stopped_count += 1

        logger.info(f"Stopped {stopped_count} streams")
        return stopped_count

    def get_stream_statistics(self) -> Dict[str, any]:
        """
        Get comprehensive statistics for all streams.

        Returns:
            Dictionary with stream statistics
        """
        stats = {
            "active_streams": len(self.active_streams),
            "stream_details": {},
            "total_processed_records": 0,
            "average_processing_rate": 0,
        }

        total_rate = 0
        active_count = 0

        for stream_name, query in self.active_streams.items():
            if query.isActive:
                progress = query.lastProgress
                if progress:
                    processed_rate = progress.get("processedRowsPerSecond", 0)
                    total_rate += processed_rate
                    active_count += 1

                    stats["stream_details"][stream_name] = {
                        "processed_rows_per_second": processed_rate,
                        "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                        "current_batch_id": progress.get("batchId", -1),
                        "batch_duration_ms": progress.get("batchDuration", 0),
                    }

        if active_count > 0:
            stats["average_processing_rate"] = total_rate / active_count

        logger.info(f"Generated statistics for {len(self.active_streams)} streams")
        return stats

    def restart_failed_streams(self) -> List[str]:
        """
        Restart any failed streaming queries.

        Returns:
            List of restarted stream names
        """
        restarted_streams = []

        for stream_name, query in list(self.active_streams.items()):
            if not query.isActive and query.exception:
                logger.warning(f"Stream '{stream_name}' failed: {query.exception}")

                # Remove failed stream
                del self.active_streams[stream_name]

                # Attempt to restart based on stream type
                try:
                    if stream_name == "transactions":
                        self.start_transaction_streaming()
                    elif stream_name == "user_events":
                        self.start_user_events_streaming()
                    elif stream_name == "customer_profiles":
                        self.start_customer_profile_streaming()

                    restarted_streams.append(stream_name)
                    logger.info(f"Successfully restarted stream: {stream_name}")

                except Exception as e:
                    logger.error(f"Failed to restart stream '{stream_name}': {e}")

        return restarted_streams

    def close(self) -> None:
        """Clean up resources and stop all streams."""
        self.stop_all_streams()
        logger.info("Delta Streaming Manager closed")

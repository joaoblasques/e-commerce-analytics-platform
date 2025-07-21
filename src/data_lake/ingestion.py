"""
Data Lake Ingestion Module

Provides automated data ingestion from various sources into the data lake
with support for both batch and streaming ingestion patterns.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.streaming import StreamingQuery

from ..utils.logger import setup_logger
from ..utils.spark_utils import create_spark_session
from .storage import DataLakeStorage

logger = setup_logger(__name__)


class DataLakeIngester:
    """
    Automated data ingestion into the data lake.

    Supports both batch and streaming ingestion from various sources
    including Kafka, databases, files, and APIs.
    """

    def __init__(
        self,
        storage: DataLakeStorage,
        spark_session: Optional[SparkSession] = None,
        kafka_bootstrap_servers: str = "kafka:9092",
    ):
        """
        Initialize DataLakeIngester.

        Args:
            storage: DataLakeStorage instance for writing data
            spark_session: Existing Spark session, creates new if None
            kafka_bootstrap_servers: Kafka bootstrap servers
        """
        self.storage = storage
        self.spark = spark_session or create_spark_session("DataLakeIngester")
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.active_streams: Dict[str, StreamingQuery] = {}

        logger.info("Initialized DataLakeIngester")

    def ingest_from_kafka_batch(
        self,
        topic: str,
        table_name: str,
        data_type: str,
        batch_size: int = 1000,
        max_batches: Optional[int] = None,
        transform_func: Optional[Callable[[DataFrame], DataFrame]] = None,
    ) -> Dict[str, Any]:
        """
        Ingest data from Kafka topic in batch mode.

        Args:
            topic: Kafka topic to consume from
            table_name: Target table name in data lake
            data_type: Type of data for partitioning strategy
            batch_size: Number of messages per batch
            max_batches: Maximum number of batches to process (None for unlimited)
            transform_func: Optional transformation function

        Returns:
            Dictionary with ingestion results
        """
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=f"data_lake_ingester_{table_name}",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                max_poll_records=batch_size,
            )

            total_records = 0
            batch_count = 0
            start_time = datetime.now()

            logger.info(f"Starting batch ingestion from topic {topic} to {table_name}")

            while max_batches is None or batch_count < max_batches:
                # Poll for messages
                message_batch = consumer.poll(timeout_ms=10000)

                if not message_batch:
                    logger.info("No more messages available")
                    break

                # Collect messages from all partitions
                records = []
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        records.append(message.value)

                if not records:
                    continue

                # Convert to DataFrame
                df = self.spark.createDataFrame(records)

                # Apply transformation if provided
                if transform_func:
                    df = transform_func(df)

                # Add ingestion metadata
                df = df.withColumn("ingested_at", current_timestamp())
                df = df.withColumn("ingestion_batch", lit(batch_count))

                # Write to data lake
                self.storage.write_partitioned_data(
                    df=df, table_name=table_name, data_type=data_type, mode="append"
                )

                batch_records = len(records)
                total_records += batch_records
                batch_count += 1

                logger.info(f"Batch {batch_count}: Ingested {batch_records} records")

            consumer.close()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "topic": topic,
                "table_name": table_name,
                "total_records": total_records,
                "total_batches": batch_count,
                "duration_seconds": duration,
                "records_per_second": total_records / duration if duration > 0 else 0,
                "status": "completed",
                "completed_at": end_time.isoformat(),
            }

            logger.info(
                f"Completed batch ingestion: {total_records} records "
                f"in {duration:.2f}s ({result['records_per_second']:.1f} records/sec)"
            )

            return result

        except Exception as e:
            logger.error(f"Batch ingestion failed for topic {topic}: {str(e)}")
            return {
                "topic": topic,
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "failed_at": datetime.now().isoformat(),
            }

    def ingest_from_kafka_streaming(
        self,
        topic: str,
        table_name: str,
        data_type: str,
        checkpoint_location: Optional[str] = None,
        trigger_interval: str = "10 seconds",
        transform_func: Optional[Callable[[DataFrame], DataFrame]] = None,
    ) -> str:
        """
        Start streaming ingestion from Kafka topic.

        Args:
            topic: Kafka topic to consume from
            table_name: Target table name in data lake
            data_type: Type of data for partitioning strategy
            checkpoint_location: Checkpoint location for fault tolerance
            trigger_interval: Trigger interval for micro-batches
            transform_func: Optional transformation function

        Returns:
            Stream ID for managing the streaming query
        """
        try:
            if not checkpoint_location:
                checkpoint_location = f"s3a://data-lake/checkpoints/{table_name}"

            # Read from Kafka
            kafka_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .load()
            )

            # Parse JSON values
            from pyspark.sql.functions import col, from_json
            from pyspark.sql.types import StringType

            # For simplicity, assume JSON structure - in production, you'd define proper schema
            parsed_df = (
                kafka_df.selectExpr("CAST(value AS STRING) as json_string")
                .select(from_json(col("json_string"), StringType()).alias("data"))
                .select("data.*")
            )

            # Apply transformation if provided
            if transform_func:
                parsed_df = transform_func(parsed_df)

            # Add ingestion metadata
            processed_df = parsed_df.withColumn(
                "ingested_at", current_timestamp()
            ).withColumn("stream_id", lit(f"{table_name}_{int(time.time())}"))

            # Define the streaming query
            stream_id = f"{table_name}_stream_{int(time.time())}"

            # Configure write to data lake
            output_path = f"{self.storage.base_path}/{table_name}"

            query = (
                processed_df.writeStream.outputMode("append")
                .format("parquet")
                .option("path", output_path)
                .option("checkpointLocation", checkpoint_location)
                .trigger(processingTime=trigger_interval)
                .start()
            )

            # Store the query for management
            self.active_streams[stream_id] = query

            logger.info(
                f"Started streaming ingestion from {topic} to {table_name} "
                f"with stream ID: {stream_id}"
            )

            return stream_id

        except Exception as e:
            logger.error(
                f"Failed to start streaming ingestion for topic {topic}: {str(e)}"
            )
            raise

    def ingest_from_database(
        self,
        connection_config: Dict[str, str],
        query: str,
        table_name: str,
        data_type: str,
        mode: str = "overwrite",
        partition_column: Optional[str] = None,
        num_partitions: int = 4,
        transform_func: Optional[Callable[[DataFrame], DataFrame]] = None,
    ) -> Dict[str, Any]:
        """
        Ingest data from a database.

        Args:
            connection_config: Database connection configuration
            query: SQL query to extract data
            table_name: Target table name in data lake
            data_type: Type of data for partitioning strategy
            mode: Write mode (overwrite, append, etc.)
            partition_column: Column to use for parallel reading
            num_partitions: Number of parallel partitions for reading
            transform_func: Optional transformation function

        Returns:
            Dictionary with ingestion results
        """
        try:
            start_time = datetime.now()

            # Build JDBC reader
            reader = (
                self.spark.read.format("jdbc")
                .option("url", connection_config["url"])
                .option("user", connection_config.get("user", ""))
                .option("password", connection_config.get("password", ""))
                .option("query", query)
            )

            # Add partitioning options for parallel reading
            if partition_column:
                reader = reader.option("partitionColumn", partition_column).option(
                    "numPartitions", str(num_partitions)
                )

            # Read data
            df = reader.load()

            # Apply transformation if provided
            if transform_func:
                df = transform_func(df)

            # Add ingestion metadata
            df = df.withColumn("ingested_at", current_timestamp())

            # Write to data lake
            output_path = self.storage.write_partitioned_data(
                df=df, table_name=table_name, data_type=data_type, mode=mode
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            record_count = df.count()

            result = {
                "source": "database",
                "table_name": table_name,
                "query": query,
                "record_count": record_count,
                "duration_seconds": duration,
                "records_per_second": record_count / duration if duration > 0 else 0,
                "output_path": output_path,
                "status": "completed",
                "completed_at": end_time.isoformat(),
            }

            logger.info(
                f"Completed database ingestion: {record_count} records "
                f"in {duration:.2f}s ({result['records_per_second']:.1f} records/sec)"
            )

            return result

        except Exception as e:
            logger.error(f"Database ingestion failed for table {table_name}: {str(e)}")
            return {
                "source": "database",
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "failed_at": datetime.now().isoformat(),
            }

    def ingest_files_batch(
        self,
        file_paths: List[str],
        table_name: str,
        data_type: str,
        file_format: str = "json",
        mode: str = "append",
        transform_func: Optional[Callable[[DataFrame], DataFrame]] = None,
        parallel: bool = True,
    ) -> Dict[str, Any]:
        """
        Ingest multiple files in batch.

        Args:
            file_paths: List of file paths to ingest
            table_name: Target table name in data lake
            data_type: Type of data for partitioning strategy
            file_format: Format of source files (json, csv, parquet)
            mode: Write mode (overwrite, append, etc.)
            transform_func: Optional transformation function
            parallel: Whether to process files in parallel

        Returns:
            Dictionary with ingestion results
        """
        try:
            start_time = datetime.now()
            total_records = 0
            processed_files = 0
            failed_files = []

            def process_file(file_path: str) -> Dict[str, Any]:
                try:
                    # Read file based on format
                    if file_format.lower() == "json":
                        df = self.spark.read.json(file_path)
                    elif file_format.lower() == "csv":
                        df = self.spark.read.option("header", "true").csv(file_path)
                    elif file_format.lower() == "parquet":
                        df = self.spark.read.parquet(file_path)
                    else:
                        raise ValueError(f"Unsupported file format: {file_format}")

                    # Apply transformation if provided
                    if transform_func:
                        df = transform_func(df)

                    # Add metadata
                    df = df.withColumn("ingested_at", current_timestamp()).withColumn(
                        "source_file", lit(file_path)
                    )

                    record_count = df.count()

                    # Write to data lake
                    self.storage.write_partitioned_data(
                        df=df,
                        table_name=table_name,
                        data_type=data_type,
                        mode="append",  # Always append when processing multiple files
                    )

                    return {
                        "file_path": file_path,
                        "record_count": record_count,
                        "status": "success",
                    }

                except Exception as e:
                    logger.error(f"Failed to process file {file_path}: {str(e)}")
                    return {"file_path": file_path, "status": "failed", "error": str(e)}

            # Process files
            if parallel and len(file_paths) > 1:
                # Parallel processing
                with ThreadPoolExecutor(max_workers=4) as executor:
                    future_to_file = {
                        executor.submit(process_file, file_path): file_path
                        for file_path in file_paths
                    }

                    for future in as_completed(future_to_file):
                        result = future.result()
                        if result["status"] == "success":
                            total_records += result["record_count"]
                            processed_files += 1
                        else:
                            failed_files.append(result)
            else:
                # Sequential processing
                for file_path in file_paths:
                    result = process_file(file_path)
                    if result["status"] == "success":
                        total_records += result["record_count"]
                        processed_files += 1
                    else:
                        failed_files.append(result)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "source": "files",
                "table_name": table_name,
                "file_format": file_format,
                "total_files": len(file_paths),
                "processed_files": processed_files,
                "failed_files": len(failed_files),
                "total_records": total_records,
                "duration_seconds": duration,
                "records_per_second": total_records / duration if duration > 0 else 0,
                "failed_file_details": failed_files,
                "status": "completed" if not failed_files else "partially_completed",
                "completed_at": end_time.isoformat(),
            }

            logger.info(
                f"Completed file ingestion: {processed_files}/{len(file_paths)} files, "
                f"{total_records} records in {duration:.2f}s"
            )

            return result

        except Exception as e:
            logger.error(f"File batch ingestion failed: {str(e)}")
            return {
                "source": "files",
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "failed_at": datetime.now().isoformat(),
            }

    def stop_streaming_query(self, stream_id: str) -> bool:
        """
        Stop a streaming ingestion query.

        Args:
            stream_id: ID of the streaming query to stop

        Returns:
            True if successfully stopped, False otherwise
        """
        try:
            if stream_id in self.active_streams:
                query = self.active_streams[stream_id]
                query.stop()
                del self.active_streams[stream_id]

                logger.info(f"Stopped streaming query: {stream_id}")
                return True
            else:
                logger.warning(f"Stream ID not found: {stream_id}")
                return False

        except Exception as e:
            logger.error(f"Failed to stop streaming query {stream_id}: {str(e)}")
            return False

    def stop_all_streams(self) -> int:
        """
        Stop all active streaming queries.

        Returns:
            Number of streams stopped
        """
        stopped_count = 0
        stream_ids = list(self.active_streams.keys())

        for stream_id in stream_ids:
            if self.stop_streaming_query(stream_id):
                stopped_count += 1

        logger.info(f"Stopped {stopped_count} streaming queries")
        return stopped_count

    def get_stream_status(self, stream_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get status of streaming queries.

        Args:
            stream_id: Specific stream ID to check, None for all streams

        Returns:
            Dictionary with stream status information
        """
        if stream_id:
            if stream_id in self.active_streams:
                query = self.active_streams[stream_id]
                return {
                    "stream_id": stream_id,
                    "is_active": query.isActive,
                    "status": query.status,
                    "last_progress": query.lastProgress,
                }
            else:
                return {"stream_id": stream_id, "status": "not_found"}

        # Return status for all active streams
        statuses = {}
        for sid, query in self.active_streams.items():
            statuses[sid] = {
                "is_active": query.isActive,
                "status": query.status,
                "last_progress": query.lastProgress,
            }

        return {"active_streams": len(statuses), "streams": statuses}

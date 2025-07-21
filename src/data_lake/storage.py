"""
Data Lake Storage Layer

Provides efficient storage of data in Parquet format with optimal partitioning
strategies for different data types and access patterns.
"""

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_format, dayofmonth, month, year
from pyspark.sql.types import StructType

from ..utils.logger import setup_logging
from ..utils.spark_utils import create_spark_session

logger = setup_logging(__name__)


class DataLakeStorage:
    """
    Data Lake Storage with optimal partitioning strategies.

    Provides efficient storage of data in Parquet format with different
    partitioning strategies optimized for various data access patterns.
    """

    def __init__(
        self,
        base_path: str = "s3a://data-lake",
        spark_session: Optional[SparkSession] = None,
        compression: str = "snappy",
    ):
        """
        Initialize DataLakeStorage.

        Args:
            base_path: Base path for data lake storage (S3/MinIO)
            spark_session: Existing Spark session, creates new if None
            compression: Compression algorithm for Parquet files
        """
        self.base_path = base_path.rstrip("/")
        self.spark = spark_session or create_spark_session("DataLakeStorage")
        self.compression = compression

        # Configure Spark for S3/MinIO access
        self._configure_s3_access()

        logger.info(f"Initialized DataLakeStorage with base path: {self.base_path}")

    def _configure_s3_access(self) -> None:
        """Configure Spark for S3/MinIO access."""
        spark_conf = self.spark.sparkContext._conf

        # MinIO/S3 configuration
        spark_conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        spark_conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
        spark_conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        spark_conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        spark_conf.set(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        spark_conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Performance optimizations
        spark_conf.set("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
        spark_conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
        spark_conf.set("spark.hadoop.fs.s3a.connection.maximum", "20")

        logger.debug("Configured Spark for S3/MinIO access")

    def get_partitioning_strategy(
        self, data_type: str, custom_partitions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get optimal partitioning strategy for different data types.

        Args:
            data_type: Type of data (transactions, user_events, product_updates, etc.)
            custom_partitions: Custom partition columns to use

        Returns:
            Dictionary with partitioning configuration
        """
        strategies = {
            "transactions": {
                "partition_columns": ["year", "month", "day"],
                "date_column": "transaction_timestamp",
                "bucket_count": None,
                "sort_columns": ["transaction_id"],
                "description": "Time-based partitioning for transaction analysis",
            },
            "user_events": {
                "partition_columns": ["year", "month", "day", "event_type"],
                "date_column": "event_timestamp",
                "bucket_count": None,
                "sort_columns": ["session_id", "event_timestamp"],
                "description": "Time and event type partitioning for user behavior analysis",
            },
            "product_updates": {
                "partition_columns": ["category", "year", "month"],
                "date_column": "updated_at",
                "bucket_count": None,
                "sort_columns": ["product_id"],
                "description": "Category-based partitioning for product catalog updates",
            },
            "analytics_results": {
                "partition_columns": ["result_type", "year", "month"],
                "date_column": "computed_at",
                "bucket_count": None,
                "sort_columns": ["computation_id"],
                "description": "Result type partitioning for analytics outputs",
            },
            "customer_profiles": {
                "partition_columns": ["customer_segment", "region"],
                "date_column": None,
                "bucket_count": 10,
                "sort_columns": ["customer_id"],
                "description": "Segment and region partitioning for customer data",
            },
        }

        if custom_partitions:
            return {
                "partition_columns": custom_partitions,
                "date_column": None,
                "bucket_count": None,
                "sort_columns": [],
                "description": f"Custom partitioning: {custom_partitions}",
            }

        strategy = strategies.get(
            data_type,
            {
                "partition_columns": ["year", "month"],
                "date_column": "created_at",
                "bucket_count": None,
                "sort_columns": [],
                "description": "Default time-based partitioning",
            },
        )

        logger.info(
            f"Using partitioning strategy for {data_type}: {strategy['description']}"
        )
        return strategy

    def prepare_dataframe_for_partitioning(
        self, df: DataFrame, strategy: Dict[str, Any]
    ) -> DataFrame:
        """
        Prepare DataFrame with partition columns based on strategy.

        Args:
            df: Input DataFrame
            strategy: Partitioning strategy from get_partitioning_strategy()

        Returns:
            DataFrame with added partition columns
        """
        result_df = df

        # Add time-based partition columns if date_column is specified
        if strategy.get("date_column") and strategy["date_column"] in df.columns:
            date_col = strategy["date_column"]

            if "year" in strategy["partition_columns"]:
                result_df = result_df.withColumn("year", year(col(date_col)))

            if "month" in strategy["partition_columns"]:
                result_df = result_df.withColumn("month", month(col(date_col)))

            if "day" in strategy["partition_columns"]:
                result_df = result_df.withColumn("day", dayofmonth(col(date_col)))

            logger.debug(f"Added time-based partition columns from {date_col}")

        return result_df

    def write_partitioned_data(
        self,
        df: DataFrame,
        table_name: str,
        data_type: str,
        mode: str = "append",
        custom_partitions: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Write DataFrame to data lake with optimal partitioning.

        Args:
            df: DataFrame to write
            table_name: Name of the table/dataset
            data_type: Type of data for partitioning strategy
            mode: Write mode (append, overwrite, etc.)
            custom_partitions: Custom partition columns
            options: Additional write options

        Returns:
            Path where data was written
        """
        strategy = self.get_partitioning_strategy(data_type, custom_partitions)
        prepared_df = self.prepare_dataframe_for_partitioning(df, strategy)

        # Build output path
        output_path = f"{self.base_path}/{table_name}"

        # Configure write options
        write_options = {"compression": self.compression, "mergeSchema": "true"}
        if options:
            write_options.update(options)

        # Write data with partitioning
        writer = prepared_df.coalesce(1).write.mode(mode)

        # Apply write options
        for key, value in write_options.items():
            writer = writer.option(key, value)

        # Apply partitioning
        partition_cols = strategy["partition_columns"]
        available_partition_cols = [
            col for col in partition_cols if col in prepared_df.columns
        ]

        if available_partition_cols:
            writer = writer.partitionBy(*available_partition_cols)
            logger.info(f"Writing with partitions: {available_partition_cols}")

        # Apply sorting if specified
        if strategy.get("sort_columns"):
            sort_cols = [
                col for col in strategy["sort_columns"] if col in prepared_df.columns
            ]
            if sort_cols:
                prepared_df = prepared_df.sortWithinPartitions(*sort_cols)

        # Execute write
        start_time = datetime.now()
        writer.parquet(output_path)
        end_time = datetime.now()

        duration = (end_time - start_time).total_seconds()
        record_count = prepared_df.count()

        logger.info(
            f"Successfully wrote {record_count} records to {output_path} "
            f"in {duration:.2f}s with strategy: {strategy['description']}"
        )

        return output_path

    def read_partitioned_data(
        self,
        table_name: str,
        partition_filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Read partitioned data from data lake with optional filtering.

        Args:
            table_name: Name of the table/dataset to read
            partition_filters: Dictionary of partition column filters
            columns: Specific columns to read (None for all)

        Returns:
            DataFrame with requested data
        """
        input_path = f"{self.base_path}/{table_name}"

        # Build reader
        reader = self.spark.read.format("parquet")

        if columns:
            reader = reader.option("select", ",".join(columns))

        # Read the data
        df = reader.load(input_path)

        # Apply partition filters
        if partition_filters:
            for column, value in partition_filters.items():
                if isinstance(value, list):
                    df = df.filter(col(column).isin(value))
                else:
                    df = df.filter(col(column) == value)

            logger.info(f"Applied partition filters: {partition_filters}")

        logger.info(f"Read data from {input_path}")
        return df

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a stored table.

        Args:
            table_name: Name of the table to analyze

        Returns:
            Dictionary with table information
        """
        table_path = f"{self.base_path}/{table_name}"

        try:
            df = self.spark.read.parquet(table_path)

            # Get basic info
            record_count = df.count()
            schema = df.schema
            columns = df.columns

            # Get partition information
            partition_cols = []
            try:
                # Try to get partition columns from catalog if available
                catalog_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
                # This would work if table is registered in catalog
            except Exception:
                # Fallback to detecting from path structure
                pass

            info = {
                "table_name": table_name,
                "path": table_path,
                "record_count": record_count,
                "columns": columns,
                "schema": schema.json(),
                "partition_columns": partition_cols,
                "last_analyzed": datetime.now().isoformat(),
            }

            logger.info(
                f"Retrieved info for table {table_name}: {record_count} records"
            )
            return info

        except Exception as e:
            logger.error(f"Failed to get info for table {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "path": table_path,
                "error": str(e),
                "last_analyzed": datetime.now().isoformat(),
            }

    def optimize_table(
        self, table_name: str, target_file_size_mb: int = 128
    ) -> Dict[str, Any]:
        """
        Optimize table by consolidating small files.

        Args:
            table_name: Name of the table to optimize
            target_file_size_mb: Target file size in MB

        Returns:
            Dictionary with optimization results
        """
        table_path = f"{self.base_path}/{table_name}"

        try:
            # Read current data
            df = self.spark.read.parquet(table_path)
            original_count = df.count()

            # Calculate optimal partition count
            estimated_size_mb = original_count * 0.001  # Rough estimate: 1KB per record
            target_partitions = max(1, int(estimated_size_mb / target_file_size_mb))

            # Optimize by repartitioning and rewriting
            optimized_df = df.coalesce(target_partitions)

            # Backup original (optional)
            backup_path = f"{table_path}_backup_{int(datetime.now().timestamp())}"

            # Write optimized version
            start_time = datetime.now()
            optimized_df.write.mode("overwrite").parquet(table_path)
            end_time = datetime.now()

            optimization_time = (end_time - start_time).total_seconds()

            result = {
                "table_name": table_name,
                "record_count": original_count,
                "target_partitions": target_partitions,
                "optimization_time_seconds": optimization_time,
                "status": "success",
                "optimized_at": end_time.isoformat(),
            }

            logger.info(
                f"Optimized table {table_name}: {target_partitions} partitions in {optimization_time:.2f}s"
            )
            return result

        except Exception as e:
            logger.error(f"Failed to optimize table {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "optimized_at": datetime.now().isoformat(),
            }

    def list_tables(self) -> List[Dict[str, str]]:
        """
        List all tables in the data lake.

        Returns:
            List of dictionaries with table information
        """
        try:
            # Use Spark to list directories in the base path
            # This is a simplified approach - in production, you'd use a proper catalog
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_conf
            )
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                self.base_path
            )

            tables = []
            if fs.exists(path):
                statuses = fs.listStatus(path)
                for status in statuses:
                    if status.isDirectory():
                        table_name = status.getPath().getName()
                        tables.append(
                            {
                                "table_name": table_name,
                                "path": f"{self.base_path}/{table_name}",
                                "modified": datetime.fromtimestamp(
                                    status.getModificationTime() / 1000
                                ).isoformat(),
                            }
                        )

            logger.info(f"Listed {len(tables)} tables in data lake")
            return tables

        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
            return []

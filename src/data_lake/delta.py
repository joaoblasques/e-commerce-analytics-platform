"""
Delta Lake Integration Module

Provides ACID transaction capabilities, time travel, schema evolution,
and optimization features for the data lake using Delta Lake.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from .lifecycle_manager import DataLifecycleManager

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from ..utils.logger import setup_logging

logger = setup_logging(__name__)


class DeltaLakeManager:
    """
    Manages Delta Lake tables with ACID transactions, time travel,
    and schema evolution capabilities.
    """

    def __init__(
        self,
        base_path: str = "s3a://data-lake/delta",
        spark_session: Optional[SparkSession] = None,
    ):
        """
        Initialize Delta Lake Manager.

        Args:
            base_path: Base path for Delta Lake tables
            spark_session: Optional Spark session with Delta Lake configured
        """
        self.base_path = base_path
        self.spark = spark_session or self._create_delta_spark_session()
        self.tables: Dict[str, str] = {}  # table_name -> table_path mapping

        logger.info(f"Delta Lake Manager initialized with base path: {base_path}")

    def _create_delta_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake configuration."""
        builder = SparkSession.builder.appName("DeltaLakeManager")

        # Configure Spark for Delta Lake
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Additional Delta Lake configurations
        spark.conf.set(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        spark.conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        logger.info("Delta Lake Spark session created successfully")
        return spark

    def create_delta_table(
        self,
        table_name: str,
        schema: StructType,
        partition_columns: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Create a new Delta Lake table.

        Args:
            table_name: Name of the table
            schema: Table schema
            partition_columns: Columns to partition by
            properties: Table properties

        Returns:
            Path to the created table
        """
        table_path = f"{self.base_path}/{table_name}"
        self.tables[table_name] = table_path

        # Create empty DataFrame with schema
        empty_df = self.spark.createDataFrame([], schema)

        # Build Delta table writer
        writer = empty_df.write.format("delta")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        # Set table properties
        if properties:
            for key, value in properties.items():
                writer = writer.option(f"delta.{key}", value)

        # Create the table
        writer.mode("overwrite").save(table_path)

        logger.info(f"Created Delta table '{table_name}' at {table_path}")
        logger.info(f"Partition columns: {partition_columns or 'None'}")

        return table_path

    def write_to_delta(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        merge_condition: Optional[str] = None,
        update_condition: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Write DataFrame to Delta Lake table with ACID guarantees.

        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode ('append', 'overwrite', 'merge')
            merge_condition: Condition for merge operations
            update_condition: Update conditions for merge
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found. Create it first.")

        table_path = self.tables[table_name]

        if mode == "merge" and merge_condition:
            self._merge_to_delta(df, table_path, merge_condition, update_condition)
        else:
            # Standard write with ACID guarantees
            df.write.format("delta").mode(mode).save(table_path)

        logger.info(
            f"Written {df.count()} records to Delta table '{table_name}' in {mode} mode"
        )

    def _merge_to_delta(
        self,
        source_df: DataFrame,
        table_path: str,
        merge_condition: str,
        update_condition: Optional[Dict[str, str]] = None,
    ) -> None:
        """Execute merge (upsert) operation on Delta table."""
        delta_table = DeltaTable.forPath(self.spark, table_path)

        merge_builder = delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        )

        if update_condition:
            # Update matched records
            merge_builder = merge_builder.whenMatchedUpdate(set=update_condition)
        else:
            # Update all columns for matched records
            merge_builder = merge_builder.whenMatchedUpdateAll()

        # Insert new records
        merge_builder = merge_builder.whenNotMatchedInsertAll()

        merge_builder.execute()
        logger.info(f"Merge operation completed on {table_path}")

    def create_streaming_writer(
        self,
        df: DataFrame,
        table_name: str,
        checkpoint_location: str,
        trigger_interval: str = "30 seconds",
        output_mode: str = "append",
    ) -> StreamingQuery:
        """
        Create streaming writer for Delta Lake with ACID guarantees.

        Args:
            df: Streaming DataFrame
            table_name: Target table name
            checkpoint_location: Checkpoint location for fault tolerance
            trigger_interval: Trigger interval for micro-batches
            output_mode: Output mode for streaming

        Returns:
            StreamingQuery object
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found. Create it first.")

        table_path = self.tables[table_name]

        query = (
            df.writeStream.format("delta")
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=trigger_interval)
            .start(table_path)
        )

        logger.info(f"Started streaming writer for table '{table_name}'")
        logger.info(f"Checkpoint location: {checkpoint_location}")
        logger.info(f"Trigger interval: {trigger_interval}")

        return query

    def time_travel_read(
        self,
        table_name: str,
        timestamp: Optional[datetime] = None,
        version: Optional[int] = None,
    ) -> DataFrame:
        """
        Read Delta table at a specific point in time or version.

        Args:
            table_name: Table name to read
            timestamp: Specific timestamp to read
            version: Specific version to read

        Returns:
            DataFrame at the specified point in time/version
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_path = self.tables[table_name]

        if timestamp:
            df = (
                self.spark.read.format("delta")
                .option("timestampAsOf", timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                .load(table_path)
            )
            logger.info(
                f"Time travel read from '{table_name}' at timestamp: {timestamp}"
            )
        elif version is not None:
            df = (
                self.spark.read.format("delta")
                .option("versionAsOf", version)
                .load(table_path)
            )
            logger.info(f"Time travel read from '{table_name}' at version: {version}")
        else:
            # Latest version
            df = self.spark.read.format("delta").load(table_path)
            logger.info(f"Reading latest version of '{table_name}'")

        return df

    def get_table_history(self, table_name: str, limit: int = 20) -> DataFrame:
        """
        Get the history of operations on a Delta table.

        Args:
            table_name: Table name
            limit: Maximum number of history entries

        Returns:
            DataFrame with table history
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_path = self.tables[table_name]
        delta_table = DeltaTable.forPath(self.spark, table_path)

        history = delta_table.history(limit)
        logger.info(f"Retrieved {limit} history entries for table '{table_name}'")

        return history

    def evolve_schema(
        self,
        table_name: str,
        new_df: DataFrame,
        merge_schema: bool = True,
    ) -> None:
        """
        Evolve table schema by merging with new DataFrame schema.

        Args:
            table_name: Table name to evolve
            new_df: DataFrame with new schema
            merge_schema: Whether to merge schemas automatically
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_path = self.tables[table_name]

        if merge_schema:
            # Enable schema evolution
            new_df.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).save(table_path)

            logger.info(f"Schema evolution completed for table '{table_name}'")
            logger.info(f"New schema: {new_df.schema}")
        else:
            raise ValueError("Schema evolution requires merge_schema=True")

    def optimize_table(
        self,
        table_name: str,
        where_clause: Optional[str] = None,
        z_order_columns: Optional[List[str]] = None,
    ) -> None:
        """
        Optimize Delta table by compacting small files.

        Args:
            table_name: Table name to optimize
            where_clause: Optional where clause to optimize specific partitions
            z_order_columns: Columns for Z-ordering optimization
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_path = self.tables[table_name]
        delta_table = DeltaTable.forPath(self.spark, table_path)

        optimize_cmd = delta_table.optimize()

        if where_clause:
            optimize_cmd = optimize_cmd.where(where_clause)

        if z_order_columns:
            optimize_cmd = optimize_cmd.executeZOrderBy(*z_order_columns)
            logger.info(
                f"Z-order optimization completed for columns: {z_order_columns}"
            )
        else:
            optimize_cmd.executeCompaction()
            logger.info("File compaction optimization completed")

        logger.info(f"Optimization completed for table '{table_name}'")

    def vacuum_table(
        self,
        table_name: str,
        retention_hours: int = 168,  # 7 days default
        dry_run: bool = False,
    ) -> DataFrame:
        """
        Vacuum Delta table to remove old files.

        Args:
            table_name: Table name to vacuum
            retention_hours: Retention period in hours
            dry_run: Whether to perform dry run

        Returns:
            DataFrame with vacuum results
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_path = self.tables[table_name]
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Vacuum old files
        if dry_run:
            result = delta_table.vacuum(retention_hours, dry_run=True)
            logger.info(
                f"Vacuum dry run completed for '{table_name}' with {retention_hours}h retention"
            )
        else:
            result = delta_table.vacuum(retention_hours)
            logger.info(
                f"Vacuum completed for '{table_name}' with {retention_hours}h retention"
            )

        return result

    def create_table_properties(
        self,
        auto_optimize: bool = True,
        auto_compact: bool = True,
        deletion_vectors: bool = True,
        checkpoint_interval: int = 10,
    ) -> Dict[str, str]:
        """
        Create recommended table properties for Delta Lake.

        Args:
            auto_optimize: Enable auto-optimization
            auto_compact: Enable auto-compaction
            deletion_vectors: Enable deletion vectors
            checkpoint_interval: Checkpoint interval

        Returns:
            Dictionary of table properties
        """
        properties = {
            "autoOptimize.optimizeWrite": str(auto_optimize).lower(),
            "autoOptimize.autoCompact": str(auto_compact).lower(),
            "enableDeletionVectors": str(deletion_vectors).lower(),
            "checkpointInterval": str(checkpoint_interval),
        }

        logger.info(f"Created table properties: {properties}")
        return properties

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive information about a Delta table.

        Args:
            table_name: Table name

        Returns:
            Dictionary with table information
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_path = self.tables[table_name]
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Get current data
        current_df = self.time_travel_read(table_name)

        # Get history
        history_df = self.get_table_history(table_name, limit=5)

        info = {
            "table_name": table_name,
            "table_path": table_path,
            "current_count": current_df.count(),
            "schema": current_df.schema.json(),
            "latest_version": history_df.first()["version"]
            if not history_df.isEmpty()
            else 0,
            "creation_time": history_df.orderBy("timestamp").first()["timestamp"]
            if not history_df.isEmpty()
            else None,
        }

        logger.info(f"Retrieved table info for '{table_name}'")
        return info

    def list_tables(self) -> List[Dict[str, str]]:
        """
        List all registered Delta tables.

        Returns:
            List of table information
        """
        tables = []
        for name, path in self.tables.items():
            tables.append({"name": name, "path": path, "status": "active"})

        logger.info(f"Listed {len(tables)} Delta tables")
        return tables

    def enable_lifecycle_tracking(
        self, lifecycle_manager: "DataLifecycleManager"
    ) -> None:
        """
        Enable automatic lifecycle tracking for Delta Lake operations.

        Args:
            lifecycle_manager: DataLifecycleManager instance for tracking
        """
        from .lifecycle_manager import LineageRecord

        self.lifecycle_manager = lifecycle_manager
        self.auto_track_lineage = True
        logger.info("Lifecycle tracking enabled for Delta Lake operations")

    def _track_operation_lineage(
        self,
        operation: str,
        table_name: str,
        source_tables: List[str] = None,
        record_count: int = 0,
        bytes_processed: int = 0,
    ) -> None:
        """Track lineage for Delta Lake operations."""
        if not hasattr(self, "lifecycle_manager") or not self.auto_track_lineage:
            return

        import os
        import uuid

        from .lifecycle_manager import LineageRecord

        lineage_record = LineageRecord(
            table_name=table_name,
            operation=operation,
            timestamp=datetime.now(),
            source_tables=source_tables or [],
            target_table=table_name,
            record_count=record_count,
            bytes_processed=bytes_processed,
            job_id=f"delta_job_{uuid.uuid4().hex[:8]}",
            user=os.getenv("USER", "system"),
            metadata={
                "spark_app_id": self.spark.sparkContext.applicationId,
                "delta_version": "3.0.0",  # Could be dynamic
                "base_path": self.base_path,
            },
        )

        try:
            self.lifecycle_manager.track_lineage(lineage_record)
        except Exception as e:
            logger.warning(
                f"Failed to track lineage for {operation} on {table_name}: {e}"
            )

    def create_delta_table_with_lifecycle(
        self,
        df: DataFrame,
        table_name: str,
        table_path: Optional[str] = None,
        partition_columns: Optional[List[str]] = None,
        mode: str = "error",
        enable_retention: bool = True,
        retention_config: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Create Delta table with automatic lifecycle management setup.

        Args:
            df: DataFrame to write
            table_name: Name of the table
            table_path: Path to store the table
            partition_columns: Columns to partition by
            mode: Write mode (error, overwrite, ignore)
            enable_retention: Enable automatic retention policies
            retention_config: Custom retention configuration

        Returns:
            True if successful
        """
        # Create the Delta table
        result = self.create_delta_table(
            df, table_name, table_path, partition_columns, mode
        )

        if result and hasattr(self, "lifecycle_manager") and enable_retention:
            # Setup default retention policy if not exists
            if table_name not in self.lifecycle_manager.retention_rules:
                from .lifecycle_config import LifecycleConfigManager
                from .lifecycle_manager import RetentionRule

                config_manager = LifecycleConfigManager()
                retention_rule = config_manager.get_retention_rule(table_name)
                retention_rule.enabled = True  # Enable by default for new tables

                if retention_config:
                    # Apply custom retention configuration
                    for key, value in retention_config.items():
                        if hasattr(retention_rule, key):
                            setattr(retention_rule, key, value)

                self.lifecycle_manager.add_retention_rule(retention_rule)
                logger.info(f"Added default retention policy for table: {table_name}")

        # Track lineage
        self._track_operation_lineage(
            operation="CREATE",
            table_name=table_name,
            record_count=df.count() if df else 0,
        )

        return result

    def write_to_delta_with_lifecycle(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        merge_condition: Optional[str] = None,
        source_tables: Optional[List[str]] = None,
        track_lineage: bool = True,
    ) -> bool:
        """
        Write to Delta table with automatic lineage tracking.

        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode
            merge_condition: Condition for merge operations
            source_tables: List of source tables for lineage
            track_lineage: Whether to track lineage

        Returns:
            True if successful
        """
        record_count = df.count() if df else 0

        # Perform the write operation
        result = self.write_to_delta(df, table_name, mode, merge_condition)

        # Track lineage if enabled
        if track_lineage:
            operation = "MERGE" if merge_condition else mode.upper()
            self._track_operation_lineage(
                operation=operation,
                table_name=table_name,
                source_tables=source_tables,
                record_count=record_count,
            )

        return result

    def optimize_with_lifecycle(
        self,
        table_name: str,
        z_order_columns: Optional[List[str]] = None,
        track_lineage: bool = True,
    ) -> Dict[str, Any]:
        """
        Optimize Delta table and track the operation.

        Args:
            table_name: Table to optimize
            z_order_columns: Columns for Z-ordering
            track_lineage: Whether to track lineage

        Returns:
            Optimization results
        """
        # Get table info before optimization
        table_path = self.tables.get(table_name)
        if not table_path:
            raise ValueError(f"Table {table_name} not found")

        # Perform optimization
        result = self.optimize_table(table_name, z_order_columns)

        # Track lineage
        if track_lineage:
            self._track_operation_lineage(
                operation="OPTIMIZE",
                table_name=table_name,
                record_count=0,  # Optimization doesn't change record count
                bytes_processed=result.get("bytes_processed", 0),
            )

        return result

    def vacuum_with_lifecycle(
        self, table_name: str, retention_hours: int = 168, track_lineage: bool = True
    ) -> Dict[str, Any]:
        """
        Vacuum Delta table and track the operation.

        Args:
            table_name: Table to vacuum
            retention_hours: Retention period in hours
            track_lineage: Whether to track lineage

        Returns:
            Vacuum results
        """
        # Perform vacuum
        result = self.vacuum_table(table_name, retention_hours)

        # Track lineage
        if track_lineage:
            self._track_operation_lineage(
                operation="VACUUM",
                table_name=table_name,
                record_count=0,
                bytes_processed=result.get("bytes_removed", 0),
            )

        return result

    def apply_retention_policies(self, dry_run: bool = True) -> Dict[str, Any]:
        """
        Apply retention policies to all managed tables.

        Args:
            dry_run: Whether to perform a dry run

        Returns:
            Retention policy results
        """
        if not hasattr(self, "lifecycle_manager"):
            logger.warning(
                "Lifecycle manager not enabled. Call enable_lifecycle_tracking() first."
            )
            return {}

        return self.lifecycle_manager.apply_retention_policies(dry_run=dry_run)

    def get_table_lineage(self, table_name: str, depth: int = 3) -> Dict[str, Any]:
        """
        Get lineage information for a table.

        Args:
            table_name: Table to get lineage for
            depth: Maximum lineage depth

        Returns:
            Lineage graph
        """
        if not hasattr(self, "lifecycle_manager"):
            logger.warning(
                "Lifecycle manager not enabled. Call enable_lifecycle_tracking() first."
            )
            return {}

        return self.lifecycle_manager.get_lineage_graph(table_name, depth)

    def generate_lifecycle_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive lifecycle report for all managed tables.

        Returns:
            Lifecycle report
        """
        if not hasattr(self, "lifecycle_manager"):
            logger.warning(
                "Lifecycle manager not enabled. Call enable_lifecycle_tracking() first."
            )
            return {}

        return self.lifecycle_manager.generate_lifecycle_report()

    def close(self) -> None:
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
        logger.info("Delta Lake Manager closed")

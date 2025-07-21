"""
Data Lake Metadata Management and Cataloging

Provides comprehensive metadata management and cataloging capabilities
for data lake tables including schema tracking, lineage, and statistics.
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import mean
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import stddev
from pyspark.sql.types import DataType, StructField, StructType

from ..utils.logger import setup_logging
from ..utils.spark_utils import create_spark_session

logger = setup_logging(__name__)


class TableStatus(Enum):
    """Enumeration of table statuses."""

    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    UNDER_MAINTENANCE = "under_maintenance"


@dataclass
class ColumnMetadata:
    """Metadata for a table column."""

    name: str
    data_type: str
    nullable: bool
    distinct_count: Optional[int] = None
    null_count: Optional[int] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    std_deviation: Optional[float] = None
    description: Optional[str] = None
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class TableMetadata:
    """Comprehensive metadata for a data lake table."""

    table_name: str
    database_name: str
    table_path: str
    schema: Dict[str, Any]
    columns: List[ColumnMetadata]
    partition_columns: List[str]
    record_count: Optional[int] = None
    file_count: Optional[int] = None
    total_size_bytes: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_accessed: Optional[str] = None
    status: TableStatus = TableStatus.ACTIVE
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = None
    properties: Dict[str, str] = None
    lineage: Dict[str, List[str]] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.properties is None:
            self.properties = {}
        if self.lineage is None:
            self.lineage = {"upstream": [], "downstream": []}


class MetadataManager:
    """
    Data Lake Metadata Management and Cataloging.

    Provides comprehensive metadata management including schema tracking,
    statistics, lineage, and catalog operations.
    """

    def __init__(
        self,
        spark_session: Optional[SparkSession] = None,
        catalog_path: str = "s3a://data-lake/catalog",
        database_name: str = "data_lake",
    ):
        """
        Initialize MetadataManager.

        Args:
            spark_session: Existing Spark session, creates new if None
            catalog_path: Path to store catalog metadata
            database_name: Default database name for tables
        """
        self.spark = spark_session or create_spark_session("MetadataManager")
        self.catalog_path = catalog_path.rstrip("/")
        self.database_name = database_name
        self.metadata_cache: Dict[str, TableMetadata] = {}

        # Initialize catalog storage
        self._initialize_catalog()

        logger.info(f"Initialized MetadataManager for database: {database_name}")

    def _initialize_catalog(self) -> None:
        """Initialize catalog storage structure."""
        try:
            # Create catalog database if it doesn't exist
            catalog_tables_path = f"{self.catalog_path}/tables"
            catalog_lineage_path = f"{self.catalog_path}/lineage"
            catalog_statistics_path = f"{self.catalog_path}/statistics"

            # Create empty DataFrames to establish structure if needed
            # This is a simplified approach - in production, you'd use a proper catalog service

            logger.debug(f"Initialized catalog at: {self.catalog_path}")

        except Exception as e:
            logger.error(f"Failed to initialize catalog: {str(e)}")
            raise

    def register_table(
        self,
        table_name: str,
        table_path: str,
        description: Optional[str] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> TableMetadata:
        """
        Register a new table in the catalog.

        Args:
            table_name: Name of the table
            table_path: Path to the table data
            description: Optional description
            owner: Table owner
            tags: Optional tags for categorization
            properties: Additional properties

        Returns:
            TableMetadata object for the registered table
        """
        try:
            logger.info(f"Registering table: {table_name}")

            # Read table schema and basic info
            df = self.spark.read.parquet(table_path)
            schema = df.schema

            # Get basic statistics
            record_count = df.count()

            # Analyze schema
            columns = []
            for field in schema.fields:
                column_meta = ColumnMetadata(
                    name=field.name,
                    data_type=str(field.dataType),
                    nullable=field.nullable,
                )
                columns.append(column_meta)

            # Detect partition columns (simplified approach)
            partition_columns = self._detect_partition_columns(table_path)

            # Create table metadata
            metadata = TableMetadata(
                table_name=table_name,
                database_name=self.database_name,
                table_path=table_path,
                schema=schema.json(),
                columns=columns,
                partition_columns=partition_columns,
                record_count=record_count,
                created_at=datetime.now().isoformat(),
                updated_at=datetime.now().isoformat(),
                description=description,
                owner=owner,
                tags=tags or [],
                properties=properties or {},
            )

            # Store in catalog
            self._store_table_metadata(metadata)

            # Cache metadata
            self.metadata_cache[table_name] = metadata

            logger.info(
                f"Successfully registered table {table_name} with {record_count} records"
            )
            return metadata

        except Exception as e:
            logger.error(f"Failed to register table {table_name}: {str(e)}")
            raise

    def update_table_statistics(
        self,
        table_name: str,
        compute_column_stats: bool = True,
        sample_size: Optional[int] = None,
    ) -> TableMetadata:
        """
        Update comprehensive statistics for a table.

        Args:
            table_name: Name of the table to analyze
            compute_column_stats: Whether to compute detailed column statistics
            sample_size: Sample size for statistics (None for full table)

        Returns:
            Updated TableMetadata object
        """
        try:
            metadata = self.get_table_metadata(table_name)
            if not metadata:
                raise ValueError(f"Table {table_name} not found in catalog")

            logger.info(f"Updating statistics for table: {table_name}")

            # Read table data
            df = self.spark.read.parquet(metadata.table_path)

            # Sample if requested
            if sample_size:
                total_records = df.count()
                if total_records > sample_size:
                    sample_fraction = sample_size / total_records
                    df = df.sample(withReplacement=False, fraction=sample_fraction)

            # Update basic statistics
            metadata.record_count = df.count()
            metadata.updated_at = datetime.now().isoformat()

            # Get file statistics
            file_info = self._get_file_statistics(metadata.table_path)
            metadata.file_count = file_info.get("file_count", 0)
            metadata.total_size_bytes = file_info.get("total_size_bytes", 0)

            # Update column statistics if requested
            if compute_column_stats and metadata.columns:
                self._compute_column_statistics(df, metadata)

            # Store updated metadata
            self._store_table_metadata(metadata)
            self.metadata_cache[table_name] = metadata

            logger.info(
                f"Updated statistics for {table_name}: {metadata.record_count} records"
            )
            return metadata

        except Exception as e:
            logger.error(
                f"Failed to update statistics for table {table_name}: {str(e)}"
            )
            raise

    def _compute_column_statistics(
        self, df: DataFrame, metadata: TableMetadata
    ) -> None:
        """Compute detailed statistics for all columns."""
        try:
            # Group columns by type for efficient computation
            numeric_columns = []
            string_columns = []
            other_columns = []

            for col_meta in metadata.columns:
                if (
                    "int" in col_meta.data_type.lower()
                    or "double" in col_meta.data_type.lower()
                    or "float" in col_meta.data_type.lower()
                ):
                    numeric_columns.append(col_meta.name)
                elif "string" in col_meta.data_type.lower():
                    string_columns.append(col_meta.name)
                else:
                    other_columns.append(col_meta.name)

            # Compute statistics for all columns
            column_stats = {}

            # Basic statistics (count, distinct, nulls)
            for col_meta in metadata.columns:
                col_name = col_meta.name

                # Count distinct and nulls
                stats = df.agg(
                    countDistinct(col(col_name)).alias(f"{col_name}_distinct"),
                    count(col(col_name)).alias(f"{col_name}_count"),
                ).collect()[0]

                total_count = metadata.record_count
                non_null_count = stats[f"{col_name}_count"]
                distinct_count = stats[f"{col_name}_distinct"]
                null_count = total_count - non_null_count

                column_stats[col_name] = {
                    "distinct_count": distinct_count,
                    "null_count": null_count,
                }

            # Numeric column statistics
            if numeric_columns:
                numeric_stats_exprs = []
                for col_name in numeric_columns:
                    numeric_stats_exprs.extend(
                        [
                            spark_min(col(col_name)).alias(f"{col_name}_min"),
                            spark_max(col(col_name)).alias(f"{col_name}_max"),
                            mean(col(col_name)).alias(f"{col_name}_mean"),
                            stddev(col(col_name)).alias(f"{col_name}_stddev"),
                        ]
                    )

                if numeric_stats_exprs:
                    numeric_stats = df.agg(*numeric_stats_exprs).collect()[0]

                    for col_name in numeric_columns:
                        column_stats[col_name].update(
                            {
                                "min_value": numeric_stats[f"{col_name}_min"],
                                "max_value": numeric_stats[f"{col_name}_max"],
                                "mean_value": numeric_stats[f"{col_name}_mean"],
                                "std_deviation": numeric_stats[f"{col_name}_stddev"],
                            }
                        )

            # String column statistics (min/max length, etc.)
            for col_name in string_columns:
                # For strings, we can compute length statistics
                # This is simplified - you might want more sophisticated string analysis
                pass

            # Update column metadata with computed statistics
            for col_meta in metadata.columns:
                stats = column_stats.get(col_meta.name, {})
                col_meta.distinct_count = stats.get("distinct_count")
                col_meta.null_count = stats.get("null_count")
                col_meta.min_value = stats.get("min_value")
                col_meta.max_value = stats.get("max_value")
                col_meta.mean_value = stats.get("mean_value")
                col_meta.std_deviation = stats.get("std_deviation")

            logger.debug(
                f"Computed column statistics for {len(metadata.columns)} columns"
            )

        except Exception as e:
            logger.error(f"Failed to compute column statistics: {str(e)}")
            # Don't fail the entire operation for column stats

    def get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """
        Get metadata for a table.

        Args:
            table_name: Name of the table

        Returns:
            TableMetadata object if found, None otherwise
        """
        try:
            # Check cache first
            if table_name in self.metadata_cache:
                return self.metadata_cache[table_name]

            # Load from catalog
            metadata = self._load_table_metadata(table_name)
            if metadata:
                self.metadata_cache[table_name] = metadata

            return metadata

        except Exception as e:
            logger.error(f"Failed to get metadata for table {table_name}: {str(e)}")
            return None

    def list_tables(
        self,
        database: Optional[str] = None,
        status: Optional[TableStatus] = None,
        tags: Optional[List[str]] = None,
    ) -> List[TableMetadata]:
        """
        List tables in the catalog with optional filtering.

        Args:
            database: Database name to filter by
            status: Table status to filter by
            tags: Tags to filter by (tables must have all specified tags)

        Returns:
            List of TableMetadata objects
        """
        try:
            # Load all table metadata
            all_tables = self._load_all_table_metadata()

            # Apply filters
            filtered_tables = []
            for table_meta in all_tables:
                # Database filter
                if database and table_meta.database_name != database:
                    continue

                # Status filter
                if status and table_meta.status != status:
                    continue

                # Tags filter
                if tags:
                    table_tags = set(table_meta.tags or [])
                    required_tags = set(tags)
                    if not required_tags.issubset(table_tags):
                        continue

                filtered_tables.append(table_meta)

            logger.info(
                f"Listed {len(filtered_tables)} tables (filtered from {len(all_tables)})"
            )
            return filtered_tables

        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
            return []

    def add_table_lineage(
        self,
        table_name: str,
        upstream_tables: Optional[List[str]] = None,
        downstream_tables: Optional[List[str]] = None,
    ) -> None:
        """
        Add lineage information for a table.

        Args:
            table_name: Name of the table
            upstream_tables: List of upstream table dependencies
            downstream_tables: List of downstream table consumers
        """
        try:
            metadata = self.get_table_metadata(table_name)
            if not metadata:
                raise ValueError(f"Table {table_name} not found in catalog")

            # Update lineage
            if upstream_tables:
                current_upstream = set(metadata.lineage.get("upstream", []))
                current_upstream.update(upstream_tables)
                metadata.lineage["upstream"] = list(current_upstream)

            if downstream_tables:
                current_downstream = set(metadata.lineage.get("downstream", []))
                current_downstream.update(downstream_tables)
                metadata.lineage["downstream"] = list(current_downstream)

            metadata.updated_at = datetime.now().isoformat()

            # Store updated metadata
            self._store_table_metadata(metadata)
            self.metadata_cache[table_name] = metadata

            logger.info(f"Updated lineage for table {table_name}")

        except Exception as e:
            logger.error(f"Failed to add lineage for table {table_name}: {str(e)}")
            raise

    def get_table_lineage_graph(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Get the complete lineage graph for all tables.

        Returns:
            Dictionary with table lineage relationships
        """
        try:
            all_tables = self.list_tables()
            lineage_graph = {}

            for table_meta in all_tables:
                lineage_graph[table_meta.table_name] = {
                    "upstream": table_meta.lineage.get("upstream", []),
                    "downstream": table_meta.lineage.get("downstream", []),
                }

            logger.info(f"Generated lineage graph for {len(lineage_graph)} tables")
            return lineage_graph

        except Exception as e:
            logger.error(f"Failed to generate lineage graph: {str(e)}")
            return {}

    def search_tables(
        self, search_term: str, search_fields: List[str] = None
    ) -> List[TableMetadata]:
        """
        Search tables by name, description, tags, or other metadata.

        Args:
            search_term: Term to search for
            search_fields: Fields to search in (name, description, tags, etc.)

        Returns:
            List of matching TableMetadata objects
        """
        try:
            if search_fields is None:
                search_fields = ["table_name", "description", "tags", "owner"]

            all_tables = self.list_tables()
            matching_tables = []

            search_term_lower = search_term.lower()

            for table_meta in all_tables:
                match_found = False

                # Search in specified fields
                for field in search_fields:
                    if (
                        field == "table_name"
                        and search_term_lower in table_meta.table_name.lower()
                    ):
                        match_found = True
                        break
                    elif (
                        field == "description"
                        and table_meta.description
                        and search_term_lower in table_meta.description.lower()
                    ):
                        match_found = True
                        break
                    elif field == "tags" and table_meta.tags:
                        for tag in table_meta.tags:
                            if search_term_lower in tag.lower():
                                match_found = True
                                break
                    elif (
                        field == "owner"
                        and table_meta.owner
                        and search_term_lower in table_meta.owner.lower()
                    ):
                        match_found = True
                        break

                if match_found:
                    matching_tables.append(table_meta)

            logger.info(f"Found {len(matching_tables)} tables matching '{search_term}'")
            return matching_tables

        except Exception as e:
            logger.error(f"Failed to search tables: {str(e)}")
            return []

    def _detect_partition_columns(self, table_path: str) -> List[str]:
        """Detect partition columns from table path structure."""
        try:
            # This is a simplified approach - in production, you'd use proper catalog APIs
            # Look for common partition patterns in the path
            common_partitions = [
                "year",
                "month",
                "day",
                "dt",
                "partition_date",
                "event_type",
                "category",
                "region",
            ]

            detected = []
            for partition in common_partitions:
                if f"/{partition}=" in table_path:
                    detected.append(partition)

            return detected

        except Exception as e:
            logger.error(f"Failed to detect partition columns: {str(e)}")
            return []

    def _get_file_statistics(self, table_path: str) -> Dict[str, Any]:
        """Get file count and size statistics for a table."""
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_conf
            )
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(table_path)

            file_count = 0
            total_size = 0

            if fs.exists(path):

                def analyze_recursive(dir_path):
                    nonlocal file_count, total_size
                    statuses = fs.listStatus(dir_path)
                    for status in statuses:
                        if status.isDirectory():
                            analyze_recursive(status.getPath())
                        elif status.getPath().getName().endswith(".parquet"):
                            file_count += 1
                            total_size += status.getLen()

                analyze_recursive(path)

            return {"file_count": file_count, "total_size_bytes": total_size}

        except Exception as e:
            logger.error(f"Failed to get file statistics: {str(e)}")
            return {"file_count": 0, "total_size_bytes": 0}

    def _store_table_metadata(self, metadata: TableMetadata) -> None:
        """Store table metadata in catalog."""
        try:
            # Convert to JSON for storage
            metadata_dict = asdict(metadata)
            metadata_json = json.dumps(metadata_dict, indent=2, default=str)

            # Store in catalog (simplified approach using file system)
            catalog_file = f"{self.catalog_path}/tables/{metadata.table_name}.json"

            # Create a temporary DataFrame to write the metadata
            # In production, you'd use a proper catalog service
            metadata_df = self.spark.createDataFrame([metadata_dict])
            metadata_df.coalesce(1).write.mode("overwrite").json(
                f"{self.catalog_path}/tables/{metadata.table_name}"
            )

            logger.debug(f"Stored metadata for table: {metadata.table_name}")

        except Exception as e:
            logger.error(
                f"Failed to store metadata for {metadata.table_name}: {str(e)}"
            )
            raise

    def _load_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """Load table metadata from catalog."""
        try:
            metadata_path = f"{self.catalog_path}/tables/{table_name}"

            # Load metadata (simplified approach)
            df = self.spark.read.json(metadata_path)
            if df.count() == 0:
                return None

            metadata_dict = df.collect()[0].asDict()

            # Convert back to TableMetadata object
            # This is simplified - in production, you'd need proper deserialization
            return self._dict_to_table_metadata(metadata_dict)

        except Exception as e:
            logger.debug(f"Could not load metadata for {table_name}: {str(e)}")
            return None

    def _load_all_table_metadata(self) -> List[TableMetadata]:
        """Load metadata for all tables."""
        try:
            tables_path = f"{self.catalog_path}/tables"

            # List all table metadata files
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_conf
            )
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(tables_path)

            all_metadata = []

            if fs.exists(path):
                statuses = fs.listStatus(path)
                for status in statuses:
                    if status.isDirectory():
                        table_name = status.getPath().getName()
                        metadata = self._load_table_metadata(table_name)
                        if metadata:
                            all_metadata.append(metadata)

            return all_metadata

        except Exception as e:
            logger.error(f"Failed to load all table metadata: {str(e)}")
            return []

    def _dict_to_table_metadata(self, metadata_dict: Dict[str, Any]) -> TableMetadata:
        """Convert dictionary to TableMetadata object."""
        # This is a simplified conversion - in production, you'd use proper serialization

        # Convert columns
        columns = []
        if metadata_dict.get("columns"):
            for col_dict in metadata_dict["columns"]:
                columns.append(ColumnMetadata(**col_dict))

        # Convert status
        status = TableStatus.ACTIVE
        if metadata_dict.get("status"):
            try:
                status = TableStatus(metadata_dict["status"])
            except ValueError:
                pass

        return TableMetadata(
            table_name=metadata_dict["table_name"],
            database_name=metadata_dict["database_name"],
            table_path=metadata_dict["table_path"],
            schema=metadata_dict.get("schema", {}),
            columns=columns,
            partition_columns=metadata_dict.get("partition_columns", []),
            record_count=metadata_dict.get("record_count"),
            file_count=metadata_dict.get("file_count"),
            total_size_bytes=metadata_dict.get("total_size_bytes"),
            created_at=metadata_dict.get("created_at"),
            updated_at=metadata_dict.get("updated_at"),
            last_accessed=metadata_dict.get("last_accessed"),
            status=status,
            description=metadata_dict.get("description"),
            owner=metadata_dict.get("owner"),
            tags=metadata_dict.get("tags", []),
            properties=metadata_dict.get("properties", {}),
            lineage=metadata_dict.get("lineage", {"upstream": [], "downstream": []}),
        )

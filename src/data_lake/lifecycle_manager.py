"""
Data Lifecycle Management Module

Provides automated data retention policies, archiving strategies,
data lineage tracking, and cost optimization for storage in the data lake.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
from enum import Enum
from dataclasses import dataclass, asdict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, date_sub, datediff,
    when, sum as spark_sum, count, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

from .delta import DeltaLakeManager
from ..utils.logger import setup_logging

logger = setup_logging(__name__)


class RetentionPolicy(Enum):
    """Data retention policy types."""
    HOT = "hot"          # Frequently accessed data (0-30 days)
    WARM = "warm"        # Occasionally accessed data (30-90 days) 
    COLD = "cold"        # Rarely accessed data (90-365 days)
    FROZEN = "frozen"    # Archive data (365+ days)
    DELETED = "deleted"  # Data marked for deletion


class DataTier(Enum):
    """Data storage tiers for cost optimization."""
    STANDARD = "standard"      # High performance, high cost
    INFREQUENT = "infrequent"  # Lower performance, lower cost
    ARCHIVE = "archive"        # Lowest cost, retrieval fees
    DEEP_ARCHIVE = "deep_archive"  # Lowest cost, long retrieval time


@dataclass
class RetentionRule:
    """Data retention rule configuration."""
    table_name: str
    hot_days: int = 30
    warm_days: int = 90
    cold_days: int = 365
    archive_after_days: int = 1095  # 3 years
    delete_after_days: Optional[int] = None  # None means never delete
    partition_column: str = "created_at"
    enabled: bool = True


@dataclass
class ArchivePolicy:
    """Data archiving policy configuration."""
    table_name: str
    archive_path: str
    compression: str = "gzip"
    format: str = "parquet"
    partition_by: List[str] = None
    enabled: bool = True


@dataclass
class LineageRecord:
    """Data lineage tracking record."""
    table_name: str
    operation: str  # CREATE, UPDATE, DELETE, MERGE, OPTIMIZE
    timestamp: datetime
    source_tables: List[str]
    target_table: str
    record_count: int
    bytes_processed: int
    job_id: str
    user: str
    metadata: Dict[str, Any]


@dataclass
class StorageMetrics:
    """Storage cost and usage metrics."""
    table_name: str
    tier: DataTier
    size_bytes: int
    record_count: int
    last_accessed: datetime
    storage_cost_usd: float
    access_count_30d: int
    estimated_monthly_cost: float


class DataLifecycleManager:
    """
    Manages the complete data lifecycle including retention policies,
    archiving strategies, lineage tracking, and cost optimization.
    """

    def __init__(
        self,
        delta_manager: DeltaLakeManager,
        config_path: Optional[str] = None,
        metadata_table: str = "lifecycle_metadata"
    ):
        """
        Initialize Data Lifecycle Manager.

        Args:
            delta_manager: Delta Lake manager instance
            config_path: Path to lifecycle configuration file
            metadata_table: Table name for storing lifecycle metadata
        """
        self.delta_manager = delta_manager
        self.spark = delta_manager.spark
        self.config_path = config_path
        self.metadata_table = metadata_table
        
        # Storage for policies and rules
        self.retention_rules: Dict[str, RetentionRule] = {}
        self.archive_policies: Dict[str, ArchivePolicy] = {}
        self.lineage_records: List[LineageRecord] = []
        
        # Initialize metadata tables
        self._initialize_metadata_tables()
        
        # Load configuration if provided
        if config_path:
            self.load_configuration(config_path)
            
        logger.info("Data Lifecycle Manager initialized")

    def _initialize_metadata_tables(self):
        """Initialize metadata tables for lifecycle management."""
        try:
            # Lineage tracking table
            lineage_schema = StructType([
                StructField("table_name", StringType(), True),
                StructField("operation", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("source_tables", StringType(), True),  # JSON array
                StructField("target_table", StringType(), True),
                StructField("record_count", LongType(), True),
                StructField("bytes_processed", LongType(), True),
                StructField("job_id", StringType(), True),
                StructField("user", StringType(), True),
                StructField("metadata", StringType(), True)  # JSON object
            ])
            
            lineage_table_path = f"{self.delta_manager.base_path}/{self.metadata_table}_lineage"
            
            # Create lineage table if it doesn't exist
            if not self.delta_manager.table_exists(f"{self.metadata_table}_lineage"):
                empty_df = self.spark.createDataFrame([], lineage_schema)
                self.delta_manager.create_delta_table(
                    empty_df,
                    f"{self.metadata_table}_lineage",
                    lineage_table_path,
                    partition_columns=["table_name"]
                )
                logger.info(f"Created lineage tracking table: {self.metadata_table}_lineage")
            
            # Storage metrics table
            metrics_schema = StructType([
                StructField("table_name", StringType(), True),
                StructField("tier", StringType(), True),
                StructField("size_bytes", LongType(), True),
                StructField("record_count", LongType(), True),
                StructField("last_accessed", TimestampType(), True),
                StructField("storage_cost_usd", StringType(), True),  # Decimal as string
                StructField("access_count_30d", LongType(), True),
                StructField("estimated_monthly_cost", StringType(), True),
                StructField("measurement_timestamp", TimestampType(), True)
            ])
            
            metrics_table_path = f"{self.delta_manager.base_path}/{self.metadata_table}_metrics"
            
            # Create metrics table if it doesn't exist
            if not self.delta_manager.table_exists(f"{self.metadata_table}_metrics"):
                empty_df = self.spark.createDataFrame([], metrics_schema)
                self.delta_manager.create_delta_table(
                    empty_df,
                    f"{self.metadata_table}_metrics",
                    metrics_table_path,
                    partition_columns=["table_name"]
                )
                logger.info(f"Created storage metrics table: {self.metadata_table}_metrics")
                
        except Exception as e:
            logger.error(f"Failed to initialize metadata tables: {e}")
            raise

    def add_retention_rule(self, rule: RetentionRule) -> None:
        """
        Add or update a data retention rule.

        Args:
            rule: Retention rule configuration
        """
        self.retention_rules[rule.table_name] = rule
        logger.info(f"Added retention rule for table: {rule.table_name}")

    def add_archive_policy(self, policy: ArchivePolicy) -> None:
        """
        Add or update a data archiving policy.

        Args:
            policy: Archive policy configuration
        """
        self.archive_policies[policy.table_name] = policy
        logger.info(f"Added archive policy for table: {policy.table_name}")

    def track_lineage(self, lineage_record: LineageRecord) -> None:
        """
        Track data lineage for an operation.

        Args:
            lineage_record: Lineage tracking record
        """
        try:
            # Convert lineage record to DataFrame row
            lineage_data = [{
                "table_name": lineage_record.table_name,
                "operation": lineage_record.operation,
                "timestamp": lineage_record.timestamp,
                "source_tables": json.dumps(lineage_record.source_tables),
                "target_table": lineage_record.target_table,
                "record_count": lineage_record.record_count,
                "bytes_processed": lineage_record.bytes_processed,
                "job_id": lineage_record.job_id,
                "user": lineage_record.user,
                "metadata": json.dumps(lineage_record.metadata)
            }]
            
            lineage_df = self.spark.createDataFrame(lineage_data)
            
            # Append to lineage tracking table
            self.delta_manager.write_to_delta(
                lineage_df,
                f"{self.metadata_table}_lineage",
                mode="append"
            )
            
            # Store in memory for quick access
            self.lineage_records.append(lineage_record)
            
            logger.info(f"Tracked lineage for operation: {lineage_record.operation} on {lineage_record.table_name}")
            
        except Exception as e:
            logger.error(f"Failed to track lineage: {e}")
            raise

    def apply_retention_policies(self, dry_run: bool = True) -> Dict[str, Dict[str, int]]:
        """
        Apply retention policies to all configured tables.

        Args:
            dry_run: If True, only report what would be done without executing

        Returns:
            Dictionary with retention statistics per table
        """
        results = {}
        
        for table_name, rule in self.retention_rules.items():
            if not rule.enabled:
                continue
                
            try:
                result = self._apply_table_retention_policy(table_name, rule, dry_run)
                results[table_name] = result
                
            except Exception as e:
                logger.error(f"Failed to apply retention policy for {table_name}: {e}")
                results[table_name] = {"error": str(e)}
        
        return results

    def _apply_table_retention_policy(
        self, 
        table_name: str, 
        rule: RetentionRule, 
        dry_run: bool
    ) -> Dict[str, int]:
        """Apply retention policy to a specific table."""
        
        if not self.delta_manager.table_exists(table_name):
            logger.warning(f"Table {table_name} does not exist")
            return {"error": "Table not found"}
        
        # Read table to analyze retention
        df = self.delta_manager.read_delta_table(table_name)
        current_date = datetime.now()
        
        # Calculate retention boundaries
        hot_boundary = current_date - timedelta(days=rule.hot_days)
        warm_boundary = current_date - timedelta(days=rule.warm_days)
        cold_boundary = current_date - timedelta(days=rule.cold_days)
        
        # Count records in each tier
        hot_count = df.filter(col(rule.partition_column) >= lit(hot_boundary)).count()
        warm_count = df.filter(
            (col(rule.partition_column) >= lit(warm_boundary)) & 
            (col(rule.partition_column) < lit(hot_boundary))
        ).count()
        cold_count = df.filter(
            (col(rule.partition_column) >= lit(cold_boundary)) & 
            (col(rule.partition_column) < lit(warm_boundary))
        ).count()
        
        archive_count = 0
        delete_count = 0
        
        # Handle archiving
        if rule.archive_after_days:
            archive_boundary = current_date - timedelta(days=rule.archive_after_days)
            archive_df = df.filter(col(rule.partition_column) < lit(archive_boundary))
            archive_count = archive_df.count()
            
            if archive_count > 0 and not dry_run:
                self._archive_data(table_name, archive_df)
        
        # Handle deletion
        if rule.delete_after_days:
            delete_boundary = current_date - timedelta(days=rule.delete_after_days)
            delete_df = df.filter(col(rule.partition_column) < lit(delete_boundary))
            delete_count = delete_df.count()
            
            if delete_count > 0 and not dry_run:
                self._delete_old_data(table_name, delete_boundary, rule.partition_column)
        
        result = {
            "hot_records": hot_count,
            "warm_records": warm_count,
            "cold_records": cold_count,
            "archived_records": archive_count,
            "deleted_records": delete_count
        }
        
        logger.info(f"Retention policy results for {table_name}: {result}")
        return result

    def _archive_data(self, table_name: str, archive_df: DataFrame) -> None:
        """Archive old data to archive storage."""
        if table_name not in self.archive_policies:
            logger.warning(f"No archive policy defined for {table_name}")
            return
        
        policy = self.archive_policies[table_name]
        
        if not policy.enabled:
            return
            
        try:
            # Write to archive location
            archive_path = f"{policy.archive_path}/{table_name}/archived_{datetime.now().strftime('%Y%m%d')}"
            
            writer = archive_df.write.mode("append").format(policy.format)
            
            if policy.partition_by:
                writer = writer.partitionBy(*policy.partition_by)
            
            if policy.compression:
                writer = writer.option("compression", policy.compression)
                
            writer.save(archive_path)
            
            logger.info(f"Archived {archive_df.count()} records from {table_name} to {archive_path}")
            
        except Exception as e:
            logger.error(f"Failed to archive data for {table_name}: {e}")
            raise

    def _delete_old_data(self, table_name: str, delete_boundary: datetime, partition_column: str) -> None:
        """Delete old data based on retention policy."""
        try:
            # Use Delta Lake delete operation
            delete_condition = f"{partition_column} < '{delete_boundary}'"
            
            deleted_count = self.delta_manager.delete_from_table(table_name, delete_condition)
            
            logger.info(f"Deleted {deleted_count} old records from {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to delete old data from {table_name}: {e}")
            raise

    def optimize_storage_costs(self) -> Dict[str, StorageMetrics]:
        """
        Analyze storage usage and optimize costs by moving data to appropriate tiers.

        Returns:
            Dictionary with storage metrics per table
        """
        metrics = {}
        
        for table_name in self.delta_manager.tables.keys():
            try:
                table_metrics = self._analyze_table_storage(table_name)
                metrics[table_name] = table_metrics
                
                # Apply cost optimization recommendations
                self._apply_cost_optimization(table_name, table_metrics)
                
            except Exception as e:
                logger.error(f"Failed to optimize storage for {table_name}: {e}")
        
        return metrics

    def _analyze_table_storage(self, table_name: str) -> StorageMetrics:
        """Analyze storage metrics for a table."""
        
        # Read table metadata
        df = self.delta_manager.read_delta_table(table_name)
        
        # Calculate basic metrics
        record_count = df.count()
        
        # Estimate size (this is a simplified calculation)
        # In production, you would use actual storage APIs
        estimated_size_bytes = record_count * 1000  # Rough estimate
        
        # Get last modified time (simplified)
        last_accessed = datetime.now()
        
        # Calculate storage cost (simplified pricing model)
        storage_cost_per_gb = 0.023  # AWS S3 standard pricing per GB
        size_gb = estimated_size_bytes / (1024 * 1024 * 1024)
        storage_cost_usd = size_gb * storage_cost_per_gb
        
        # Estimate tier based on age and access patterns
        tier = self._determine_optimal_tier(table_name, last_accessed)
        
        return StorageMetrics(
            table_name=table_name,
            tier=tier,
            size_bytes=estimated_size_bytes,
            record_count=record_count,
            last_accessed=last_accessed,
            storage_cost_usd=storage_cost_usd,
            access_count_30d=100,  # Placeholder
            estimated_monthly_cost=storage_cost_usd * 30
        )

    def _determine_optimal_tier(self, table_name: str, last_accessed: datetime) -> DataTier:
        """Determine optimal storage tier based on access patterns."""
        days_since_access = (datetime.now() - last_accessed).days
        
        if days_since_access <= 7:
            return DataTier.STANDARD
        elif days_since_access <= 30:
            return DataTier.INFREQUENT
        elif days_since_access <= 90:
            return DataTier.ARCHIVE
        else:
            return DataTier.DEEP_ARCHIVE

    def _apply_cost_optimization(self, table_name: str, metrics: StorageMetrics) -> None:
        """Apply cost optimization recommendations."""
        # This would integrate with cloud storage APIs to change storage classes
        # For now, we log the recommendations
        
        current_tier = metrics.tier
        optimal_tier = self._determine_optimal_tier(table_name, metrics.last_accessed)
        
        if current_tier != optimal_tier:
            logger.info(f"Recommend moving {table_name} from {current_tier.value} to {optimal_tier.value}")

    def get_lineage_graph(self, table_name: str, depth: int = 3) -> Dict[str, Any]:
        """
        Get data lineage graph for a table.

        Args:
            table_name: Name of the table to trace
            depth: Maximum depth of lineage to trace

        Returns:
            Dictionary representing the lineage graph
        """
        try:
            # Read lineage records from Delta table
            lineage_df = self.delta_manager.read_delta_table(f"{self.metadata_table}_lineage")
            
            # Filter for the specific table
            table_lineage = lineage_df.filter(
                (col("table_name") == table_name) | 
                (col("target_table") == table_name)
            ).collect()
            
            # Build lineage graph
            graph = {
                "table": table_name,
                "upstream": [],
                "downstream": [],
                "operations": []
            }
            
            for record in table_lineage:
                if record.target_table == table_name:
                    # Upstream dependency
                    source_tables = json.loads(record.source_tables)
                    for source_table in source_tables:
                        if source_table not in graph["upstream"]:
                            graph["upstream"].append(source_table)
                
                if record.table_name == table_name:
                    # Downstream usage
                    if record.target_table not in graph["downstream"]:
                        graph["downstream"].append(record.target_table)
                
                # Add operation details
                graph["operations"].append({
                    "operation": record.operation,
                    "timestamp": record.timestamp.isoformat(),
                    "job_id": record.job_id,
                    "user": record.user,
                    "record_count": record.record_count
                })
            
            return graph
            
        except Exception as e:
            logger.error(f"Failed to get lineage graph for {table_name}: {e}")
            return {"error": str(e)}

    def generate_lifecycle_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive data lifecycle report.

        Returns:
            Dictionary containing lifecycle statistics and recommendations
        """
        report = {
            "timestamp": datetime.now().isoformat(),
            "retention_summary": {},
            "storage_summary": {},
            "cost_optimization": {},
            "lineage_summary": {},
            "recommendations": []
        }
        
        # Retention summary
        retention_results = self.apply_retention_policies(dry_run=True)
        report["retention_summary"] = retention_results
        
        # Storage summary
        storage_metrics = self.optimize_storage_costs()
        total_size = sum(m.size_bytes for m in storage_metrics.values())
        total_cost = sum(m.storage_cost_usd for m in storage_metrics.values())
        
        report["storage_summary"] = {
            "total_tables": len(storage_metrics),
            "total_size_gb": total_size / (1024 * 1024 * 1024),
            "total_monthly_cost_usd": total_cost,
            "by_tier": self._summarize_by_tier(storage_metrics)
        }
        
        # Lineage summary
        report["lineage_summary"] = {
            "total_operations": len(self.lineage_records),
            "tables_tracked": len(set(r.table_name for r in self.lineage_records)),
            "recent_operations": len([r for r in self.lineage_records 
                                   if (datetime.now() - r.timestamp).days <= 7])
        }
        
        # Generate recommendations
        report["recommendations"] = self._generate_recommendations(storage_metrics, retention_results)
        
        return report

    def _summarize_by_tier(self, storage_metrics: Dict[str, StorageMetrics]) -> Dict[str, Any]:
        """Summarize storage metrics by tier."""
        tier_summary = {}
        
        for tier in DataTier:
            tier_metrics = [m for m in storage_metrics.values() if m.tier == tier]
            if tier_metrics:
                tier_summary[tier.value] = {
                    "table_count": len(tier_metrics),
                    "total_size_gb": sum(m.size_bytes for m in tier_metrics) / (1024 * 1024 * 1024),
                    "total_cost_usd": sum(m.storage_cost_usd for m in tier_metrics)
                }
        
        return tier_summary

    def _generate_recommendations(
        self, 
        storage_metrics: Dict[str, StorageMetrics], 
        retention_results: Dict[str, Dict[str, int]]
    ) -> List[str]:
        """Generate cost and efficiency recommendations."""
        recommendations = []
        
        # Storage tier recommendations
        for table_name, metrics in storage_metrics.items():
            if metrics.tier == DataTier.STANDARD and metrics.access_count_30d < 10:
                recommendations.append(
                    f"Consider moving {table_name} to infrequent access tier to reduce costs"
                )
        
        # Retention recommendations
        for table_name, results in retention_results.items():
            if "cold_records" in results and results["cold_records"] > 100000:
                recommendations.append(
                    f"Table {table_name} has {results['cold_records']} cold records - consider archiving"
                )
        
        # General recommendations
        total_size_gb = sum(m.size_bytes for m in storage_metrics.values()) / (1024 * 1024 * 1024)
        if total_size_gb > 1000:  # 1TB
            recommendations.append(
                "Large storage usage detected - review retention policies and archiving strategies"
            )
        
        return recommendations

    def load_configuration(self, config_path: str) -> None:
        """
        Load lifecycle configuration from file.

        Args:
            config_path: Path to configuration file
        """
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Load retention rules
            for rule_config in config.get("retention_rules", []):
                rule = RetentionRule(**rule_config)
                self.add_retention_rule(rule)
            
            # Load archive policies
            for policy_config in config.get("archive_policies", []):
                policy = ArchivePolicy(**policy_config)
                self.add_archive_policy(policy)
            
            logger.info(f"Loaded configuration from {config_path}")
            
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_path}: {e}")
            raise

    def save_configuration(self, config_path: str) -> None:
        """
        Save current configuration to file.

        Args:
            config_path: Path to save configuration file
        """
        try:
            config = {
                "retention_rules": [asdict(rule) for rule in self.retention_rules.values()],
                "archive_policies": [asdict(policy) for policy in self.archive_policies.values()]
            }
            
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2, default=str)
            
            logger.info(f"Saved configuration to {config_path}")
            
        except Exception as e:
            logger.error(f"Failed to save configuration to {config_path}: {e}")
            raise
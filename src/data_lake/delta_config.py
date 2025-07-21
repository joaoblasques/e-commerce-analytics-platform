"""
Delta Lake Configuration and Utilities

Provides configuration, schemas, and utility functions for Delta Lake tables
in the e-commerce analytics platform.
"""

from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..utils.logger import setup_logging

logger = setup_logging(__name__)


class DeltaTableSchemas:
    """Predefined schemas for Delta Lake tables in the e-commerce platform."""

    @staticmethod
    def transaction_schema() -> StructType:
        """Schema for transaction data in Delta Lake."""
        return StructType(
            [
                StructField("transaction_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("subcategory", StringType(), True),
                StructField("quantity", IntegerType(), False),
                StructField("price", DoubleType(), False),
                StructField("total_amount", DoubleType(), False),
                StructField("discount", DoubleType(), True),
                StructField("payment_method", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("location", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("timestamp", TimestampType(), False),
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("day", IntegerType(), False),
                StructField("hour", IntegerType(), False),
                StructField("created_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), True),
            ]
        )

    @staticmethod
    def user_events_schema() -> StructType:
        """Schema for user behavior events in Delta Lake."""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("user_id", StringType(), True),
                StructField("event_type", StringType(), False),
                StructField("page_url", StringType(), True),
                StructField("referrer_url", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("category", StringType(), True),
                StructField("search_query", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("ip_address", StringType(), True),
                StructField("country", StringType(), True),
                StructField("city", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("operating_system", StringType(), True),
                StructField("browser", StringType(), True),
                StructField("screen_resolution", StringType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("timestamp", TimestampType(), False),
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("day", IntegerType(), False),
                StructField("event_type_category", StringType(), True),
                StructField("created_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), True),
            ]
        )

    @staticmethod
    def customer_profile_schema() -> StructType:
        """Schema for customer profiles in Delta Lake."""
        return StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("email", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("date_of_birth", DateType(), True),
                StructField("gender", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("address", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("customer_tier", StringType(), True),
                StructField("lifetime_value", DoubleType(), True),
                StructField("average_order_value", DoubleType(), True),
                StructField("total_orders", IntegerType(), True),
                StructField("total_spent", DoubleType(), True),
                StructField("last_order_date", DateType(), True),
                StructField("registration_date", DateType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("preferred_categories", ArrayType(StringType()), True),
                StructField("created_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), True),
            ]
        )

    @staticmethod
    def product_catalog_schema() -> StructType:
        """Schema for product catalog in Delta Lake."""
        return StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), False),
                StructField("category", StringType(), False),
                StructField("subcategory", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), False),
                StructField("cost", DoubleType(), True),
                StructField("inventory_count", IntegerType(), True),
                StructField("description", StringType(), True),
                StructField("weight", DoubleType(), True),
                StructField("dimensions", StringType(), True),
                StructField("color", StringType(), True),
                StructField("size", StringType(), True),
                StructField("material", StringType(), True),
                StructField("supplier_id", StringType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("launch_date", DateType(), True),
                StructField("discontinue_date", DateType(), True),
                StructField("tags", ArrayType(StringType()), True),
                StructField("created_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), True),
            ]
        )

    @staticmethod
    def analytics_results_schema() -> StructType:
        """Schema for analytics results in Delta Lake."""
        return StructType(
            [
                StructField("result_id", StringType(), False),
                StructField("result_type", StringType(), False),
                StructField("analysis_date", DateType(), False),
                StructField("metric_name", StringType(), False),
                StructField("metric_value", DoubleType(), True),
                StructField("metric_category", StringType(), True),
                StructField("dimensions", StringType(), True),  # JSON string
                StructField("customer_segment", StringType(), True),
                StructField("product_category", StringType(), True),
                StructField("time_period", StringType(), True),
                StructField("confidence_score", DoubleType(), True),
                StructField("data_quality_score", DoubleType(), True),
                StructField("created_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), True),
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
            ]
        )


class DeltaTableConfigurations:
    """Predefined configurations for Delta Lake tables."""

    @staticmethod
    def transaction_table_config() -> Dict[str, any]:
        """Configuration for transaction Delta table."""
        return {
            "name": "transactions",
            "schema": DeltaTableSchemas.transaction_schema(),
            "partition_columns": ["year", "month", "day"],
            "properties": {
                "autoOptimize.optimizeWrite": "true",
                "autoOptimize.autoCompact": "true",
                "enableDeletionVectors": "true",
                "checkpointInterval": "10",
                "dataSkippingNumIndexedCols": "10",
                "logRetentionDuration": "interval 30 days",
                "deletedFileRetentionDuration": "interval 7 days",
            },
            "z_order_columns": ["customer_id", "product_id", "timestamp"],
        }

    @staticmethod
    def user_events_table_config() -> Dict[str, any]:
        """Configuration for user events Delta table."""
        return {
            "name": "user_events",
            "schema": DeltaTableSchemas.user_events_schema(),
            "partition_columns": ["year", "month", "day", "event_type_category"],
            "properties": {
                "autoOptimize.optimizeWrite": "true",
                "autoOptimize.autoCompact": "true",
                "enableDeletionVectors": "true",
                "checkpointInterval": "10",
                "dataSkippingNumIndexedCols": "15",
                "logRetentionDuration": "interval 7 days",
                "deletedFileRetentionDuration": "interval 3 days",
            },
            "z_order_columns": ["user_id", "session_id", "event_type", "timestamp"],
        }

    @staticmethod
    def customer_profile_table_config() -> Dict[str, any]:
        """Configuration for customer profiles Delta table."""
        return {
            "name": "customer_profiles",
            "schema": DeltaTableSchemas.customer_profile_schema(),
            "partition_columns": ["customer_segment", "country"],
            "properties": {
                "autoOptimize.optimizeWrite": "true",
                "autoOptimize.autoCompact": "true",
                "enableDeletionVectors": "true",
                "checkpointInterval": "10",
                "dataSkippingNumIndexedCols": "8",
                "logRetentionDuration": "interval 90 days",
                "deletedFileRetentionDuration": "interval 30 days",
            },
            "z_order_columns": ["customer_id", "customer_segment", "lifetime_value"],
        }

    @staticmethod
    def product_catalog_table_config() -> Dict[str, any]:
        """Configuration for product catalog Delta table."""
        return {
            "name": "product_catalog",
            "schema": DeltaTableSchemas.product_catalog_schema(),
            "partition_columns": ["category", "is_active"],
            "properties": {
                "autoOptimize.optimizeWrite": "true",
                "autoOptimize.autoCompact": "true",
                "enableDeletionVectors": "true",
                "checkpointInterval": "5",
                "dataSkippingNumIndexedCols": "12",
                "logRetentionDuration": "interval 180 days",
                "deletedFileRetentionDuration": "interval 90 days",
            },
            "z_order_columns": ["product_id", "category", "brand", "price"],
        }

    @staticmethod
    def analytics_results_table_config() -> Dict[str, any]:
        """Configuration for analytics results Delta table."""
        return {
            "name": "analytics_results",
            "schema": DeltaTableSchemas.analytics_results_schema(),
            "partition_columns": ["result_type", "year", "month"],
            "properties": {
                "autoOptimize.optimizeWrite": "true",
                "autoOptimize.autoCompact": "true",
                "enableDeletionVectors": "true",
                "checkpointInterval": "10",
                "dataSkippingNumIndexedCols": "8",
                "logRetentionDuration": "interval 60 days",
                "deletedFileRetentionDuration": "interval 14 days",
            },
            "z_order_columns": ["result_type", "metric_name", "analysis_date"],
        }

    @staticmethod
    def get_all_table_configs() -> List[Dict[str, any]]:
        """Get all predefined table configurations."""
        return [
            DeltaTableConfigurations.transaction_table_config(),
            DeltaTableConfigurations.user_events_table_config(),
            DeltaTableConfigurations.customer_profile_table_config(),
            DeltaTableConfigurations.product_catalog_table_config(),
            DeltaTableConfigurations.analytics_results_table_config(),
        ]


class DeltaOptimizationStrategies:
    """Optimization strategies for different Delta Lake use cases."""

    @staticmethod
    def high_frequency_streaming_config() -> Dict[str, str]:
        """Configuration for high-frequency streaming tables."""
        return {
            "autoOptimize.optimizeWrite": "true",
            "autoOptimize.autoCompact": "true",
            "enableDeletionVectors": "true",
            "checkpointInterval": "5",  # More frequent checkpoints
            "logRetentionDuration": "interval 3 days",
            "deletedFileRetentionDuration": "interval 1 day",
            "tuneFileSizesForRewrites": "true",
            "targetFileSize": "128MB",
        }

    @staticmethod
    def batch_processing_config() -> Dict[str, str]:
        """Configuration for batch processing tables."""
        return {
            "autoOptimize.optimizeWrite": "true",
            "autoOptimize.autoCompact": "false",  # Manual optimization
            "enableDeletionVectors": "true",
            "checkpointInterval": "20",  # Less frequent checkpoints
            "logRetentionDuration": "interval 30 days",
            "deletedFileRetentionDuration": "interval 7 days",
            "targetFileSize": "256MB",  # Larger files for batch
        }

    @staticmethod
    def analytical_workload_config() -> Dict[str, str]:
        """Configuration for analytical workload tables."""
        return {
            "autoOptimize.optimizeWrite": "false",  # Manual control
            "autoOptimize.autoCompact": "false",
            "enableDeletionVectors": "true",
            "checkpointInterval": "50",  # Infrequent checkpoints
            "logRetentionDuration": "interval 90 days",
            "deletedFileRetentionDuration": "interval 30 days",
            "targetFileSize": "512MB",  # Large files for analytics
            "dataSkippingNumIndexedCols": "20",  # More columns for skipping
        }


class DeltaMaintenanceScheduler:
    """Scheduling utilities for Delta Lake maintenance operations."""

    @staticmethod
    def get_optimization_schedule() -> Dict[str, Dict[str, str]]:
        """Get recommended optimization schedules by table type."""
        return {
            "high_frequency_streaming": {
                "optimize_frequency": "hourly",
                "vacuum_frequency": "daily",
                "retention_hours": "24",
                "z_order_frequency": "daily",
            },
            "moderate_frequency_streaming": {
                "optimize_frequency": "4_hourly",
                "vacuum_frequency": "daily",
                "retention_hours": "72",
                "z_order_frequency": "weekly",
            },
            "batch_processing": {
                "optimize_frequency": "daily",
                "vacuum_frequency": "weekly",
                "retention_hours": "168",  # 7 days
                "z_order_frequency": "weekly",
            },
            "analytical_workload": {
                "optimize_frequency": "weekly",
                "vacuum_frequency": "monthly",
                "retention_hours": "720",  # 30 days
                "z_order_frequency": "monthly",
            },
        }

    @staticmethod
    def create_maintenance_plan(
        table_type: str, table_names: List[str]
    ) -> Dict[str, any]:
        """Create maintenance plan for specific tables."""
        schedules = DeltaMaintenanceScheduler.get_optimization_schedule()

        if table_type not in schedules:
            raise ValueError(f"Unknown table type: {table_type}")

        schedule = schedules[table_type]

        return {
            "table_type": table_type,
            "tables": table_names,
            "schedule": schedule,
            "created_at": datetime.now(),
            "next_optimize": DeltaMaintenanceScheduler._calculate_next_run(
                schedule["optimize_frequency"]
            ),
            "next_vacuum": DeltaMaintenanceScheduler._calculate_next_run(
                schedule["vacuum_frequency"]
            ),
        }

    @staticmethod
    def _calculate_next_run(frequency: str) -> datetime:
        """Calculate next run time based on frequency."""
        from datetime import datetime, timedelta

        frequency_map = {
            "hourly": timedelta(hours=1),
            "4_hourly": timedelta(hours=4),
            "daily": timedelta(days=1),
            "weekly": timedelta(weeks=1),
            "monthly": timedelta(days=30),
        }

        if frequency not in frequency_map:
            raise ValueError(f"Unknown frequency: {frequency}")

        return datetime.now() + frequency_map[frequency]


# Utility functions for Delta Lake operations
def create_checkpoint_location(
    table_name: str, base_path: str = "/tmp/delta_checkpoints"
) -> str:
    """Create checkpoint location path for streaming operations."""
    return f"{base_path}/{table_name}_{datetime.now().strftime('%Y%m%d')}"


def validate_delta_configuration() -> bool:
    """Validate Delta Lake configuration and dependencies."""
    try:
        from delta import configure_spark_with_delta_pip

        return True
    except ImportError:
        logger.error("Delta Lake dependencies not found. Install delta-spark package.")
        return False


def get_recommended_partition_strategy(
    table_type: str, expected_daily_volume: int
) -> List[str]:
    """Get recommended partitioning strategy based on table type and volume."""

    strategies = {
        "transactions": {
            "low": ["year", "month"],  # < 1M records/day
            "medium": ["year", "month", "day"],  # 1M-10M records/day
            "high": ["year", "month", "day", "hour"],  # > 10M records/day
        },
        "user_events": {
            "low": ["year", "month", "event_type"],
            "medium": ["year", "month", "day", "event_type"],
            "high": ["year", "month", "day", "hour", "event_type"],
        },
        "customer_profiles": {
            "low": ["customer_segment"],
            "medium": ["customer_segment", "country"],
            "high": ["customer_segment", "country", "customer_tier"],
        },
    }

    # Determine volume level
    if expected_daily_volume < 1_000_000:
        volume_level = "low"
    elif expected_daily_volume < 10_000_000:
        volume_level = "medium"
    else:
        volume_level = "high"

    return strategies.get(table_type, {}).get(volume_level, ["year", "month"])


logger.info("Delta Lake configuration module loaded successfully")

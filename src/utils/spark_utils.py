"""
Spark utilities for the e-commerce analytics platform.
"""
import os
import tempfile
from typing import Optional

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType
except ImportError:
    # Handle gracefully when PySpark is not available
    SparkSession = None
    StructType = None

from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_secure_temp_dir(prefix: str = "spark") -> str:
    """
    Get a secure temporary directory path.

    Args:
        prefix: Prefix for the temporary directory name

    Returns:
        str: Secure temporary directory path
    """
    # Use system temporary directory instead of hardcoded /tmp
    # This respects TMPDIR environment variable and system settings
    temp_dir = tempfile.gettempdir()
    secure_path = os.path.join(temp_dir, f"{prefix}_{os.getpid()}")
    return secure_path


def create_spark_session(
    app_name: str = "ECommerceAnalytics",
    master: str = "local[*]",
    config: Optional[dict] = None,
) -> "SparkSession":
    """
    Create a Spark session with standard configuration.

    Args:
        app_name: Name of the Spark application
        master: Spark master URL (default: local[*])
        config: Additional Spark configuration options

    Returns:
        SparkSession: Configured Spark session

    Raises:
        ImportError: If PySpark is not available
        RuntimeError: If Spark session creation fails
    """
    if SparkSession is None:
        raise ImportError(
            "PySpark is not available. Install it with: pip install pyspark"
        )

    try:
        builder = SparkSession.builder.appName(app_name).master(master)

        # Default configurations for local development
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.warehouse.dir": get_secure_temp_dir("spark_warehouse"),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        }

        # Merge with user-provided config
        if config:
            default_config.update(config)

        # Apply configurations
        for key, value in default_config.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        logger.info(f"Created Spark session: {app_name}")
        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise RuntimeError(f"Spark session creation failed: {e}") from e


def validate_schema(df_schema: "StructType", expected_fields: list) -> bool:
    """
    Validate that a DataFrame schema contains expected fields.

    Args:
        df_schema: PySpark DataFrame schema
        expected_fields: List of expected field names

    Returns:
        bool: True if all expected fields are present
    """
    if StructType is None:
        raise ImportError("PySpark is not available")

    schema_fields = [field.name for field in df_schema.fields]
    missing_fields = set(expected_fields) - set(schema_fields)

    if missing_fields:
        logger.warning(f"Missing fields in schema: {missing_fields}")
        return False

    return True


def stop_spark_session(spark: "SparkSession") -> None:
    """
    Safely stop a Spark session.

    Args:
        spark: SparkSession to stop
    """
    try:
        if spark:
            spark.stop()
            logger.info("Spark session stopped successfully")
    except Exception as e:
        logger.error(f"Error stopping Spark session: {e}")

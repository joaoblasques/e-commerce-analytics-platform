"""
Example usage of the Streaming Data Quality Framework.

This example demonstrates how to integrate data quality checks
into streaming data pipelines for comprehensive quality monitoring.
"""

import logging
from datetime import datetime
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Import data quality framework
from src.streaming.data_quality import (
    CompletenessChecker,
    DataQualityEngine,
    StreamingAnomalyDetector,
    StreamingDataProfiler,
    StreamingDataValidator,
)

# Import existing transformations
from src.streaming.transformations import DataEnrichmentPipeline
from src.utils.logger import get_logger


def create_sample_transaction_data(spark: SparkSession) -> DataFrame:
    """Create sample transaction data for demonstration."""

    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )

    # Sample data with various quality issues for demonstration
    sample_data = [
        (
            "tx_001",
            1001,
            150.50,
            "USD",
            "credit_card",
            2001,
            "New York",
            datetime.now(),
        ),
        ("tx_002", 1002, 75.25, "USD", "paypal", 2002, "California", datetime.now()),
        (
            "tx_003",
            None,
            300.00,
            "USD",
            "debit_card",
            2003,
            "Texas",
            datetime.now(),
        ),  # Missing user_id
        (
            "",
            1004,
            -50.00,
            "USD",
            "credit_card",
            2004,
            "Florida",
            datetime.now(),
        ),  # Empty transaction_id, negative amount
        (
            "tx_005",
            1005,
            None,
            "USD",
            "bank_transfer",
            None,
            "",
            datetime.now(),
        ),  # Missing amount and product_id
        (
            "tx_006",
            1006,
            25000.00,
            "EU",
            "credit_card",
            2006,
            "Nevada",
            datetime.now(),
        ),  # Invalid currency, high amount
        (
            "tx_007",
            1007,
            99.99,
            "USD",
            "unknown_method",
            2007,
            "Oregon",
            datetime.now(),
        ),  # Invalid payment method
        ("tx_008", 1008, 45.75, "USD", "cash", 2008, "Washington", datetime.now()),
        (
            "tx_009",
            1009,
            0.00,
            "USD",
            "credit_card",
            2009,
            "Colorado",
            datetime.now(),
        ),  # Zero amount
        ("tx_010", 1010, 125.30, "USD", "debit_card", 2010, "Arizona", datetime.now()),
    ]

    return spark.createDataFrame(sample_data, schema)


def create_sample_user_behavior_data(spark: SparkSession) -> DataFrame:
    """Create sample user behavior data for demonstration."""

    schema = StructType(
        [
            StructField("user_id", IntegerType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )

    sample_data = [
        (
            1001,
            "sess_001",
            "page_view",
            "https://shop.com/home",
            "Mozilla/5.0",
            None,
            datetime.now(),
        ),
        (
            1002,
            "sess_002",
            "product_view",
            "https://shop.com/product/2002",
            "Chrome/91.0",
            2002,
            datetime.now(),
        ),
        (
            None,
            "sess_003",
            "add_to_cart",
            "https://shop.com/cart",
            "Safari/14.0",
            2003,
            datetime.now(),
        ),  # Missing user_id
        (
            1004,
            "",
            "checkout",
            "https://shop.com/checkout",
            "Edge/91.0",
            2004,
            datetime.now(),
        ),  # Empty session_id
        (
            1005,
            "sess_005",
            "unknown_event",
            "invalid_url",
            "bot_crawler_v1.0",
            2005,
            datetime.now(),
        ),  # Invalid event, suspicious user agent
        (
            1006,
            "sess_006",
            "purchase",
            "https://shop.com/success",
            "Mozilla/5.0",
            2006,
            datetime.now(),
        ),
        (
            1007,
            "sess_007",
            "search",
            "https://shop.com/search?q=laptop",
            "Chrome/91.0",
            None,
            datetime.now(),
        ),
        (
            1008,
            "sess_008",
            "page_view",
            None,
            "Safari/14.0",
            None,
            datetime.now(),
        ),  # Missing page_url
        (
            1009,
            "sess_009",
            "logout",
            "https://shop.com/logout",
            "Firefox/89.0",
            None,
            datetime.now(),
        ),
        (
            1010,
            "sess_010",
            "login",
            "https://shop.com/login",
            "Chrome/91.0",
            None,
            datetime.now(),
        ),
    ]

    return spark.createDataFrame(sample_data, schema)


def demonstrate_individual_components(spark: SparkSession):
    """Demonstrate individual data quality components."""

    logger = get_logger("data_quality_demo")
    logger.info("=== Demonstrating Individual Data Quality Components ===")

    # Create sample data
    transaction_df = create_sample_transaction_data(spark)

    print("Sample Transaction Data:")
    transaction_df.show(truncate=False)

    # 1. Data Validation Demo
    logger.info("1. Data Validation Demo")
    validator = StreamingDataValidator(spark)
    validated_df, validation_results = validator.validate_stream(
        transaction_df, "transaction"
    )

    print(
        f"Validation Results: {validation_results['valid_count']}/{validation_results['total_count']} records passed"
    )
    print("Validation Summary:")
    for rule_result in validation_results.get("rule_results", []):
        print(
            f"  - {rule_result['rule_name']}: {rule_result['pass_rate']:.2%} pass rate"
        )

    # 2. Anomaly Detection Demo
    logger.info("2. Anomaly Detection Demo")
    anomaly_detector = StreamingAnomalyDetector(spark)
    anomaly_df, anomaly_results = anomaly_detector.detect_anomalies(
        validated_df, "transaction"
    )

    print(
        f"Anomaly Detection Results: {anomaly_results['anomaly_count']}/{anomaly_results['total_count']} anomalies found"
    )
    print("Anomalies detected:")
    anomaly_df.filter(col("is_anomaly")).select(
        "transaction_id", "user_id", "total_amount", "is_anomaly"
    ).show()

    # 3. Completeness Check Demo
    logger.info("3. Completeness Check Demo")
    completeness_checker = CompletenessChecker(spark)
    completeness_df, completeness_results = completeness_checker.check_completeness(
        anomaly_df, "transaction"
    )

    print(
        f"Completeness Results: {completeness_results['overall_completeness']:.2%} overall completeness"
    )
    print("Field completeness scores:")
    for field, stats in completeness_results.get("field_completeness", {}).items():
        print(f"  - {field}: {stats['completeness_score']:.2%}")

    # 4. Data Profiling Demo
    logger.info("4. Data Profiling Demo")
    profiler = StreamingDataProfiler(spark)
    profile_results = profiler.profile_stream(completeness_df, "transaction")

    print("Data Profile Summary:")
    print(
        f"  - Dataset: {profile_results['dataset_profile']['row_count']} rows, {profile_results['dataset_profile']['column_count']} columns"
    )
    print("  - Quality Insights:")
    for insight in profile_results.get("quality_insights", []):
        print(f"    * {insight}")
    print("  - Recommendations:")
    for rec in profile_results.get("recommendations", []):
        print(f"    * {rec}")


def demonstrate_integrated_quality_engine(spark: SparkSession):
    """Demonstrate the integrated data quality engine."""

    logger = get_logger("data_quality_demo")
    logger.info("=== Demonstrating Integrated Data Quality Engine ===")

    # Create sample data
    transaction_df = create_sample_transaction_data(spark)
    user_behavior_df = create_sample_user_behavior_data(spark)

    # Configure data quality engine
    quality_config = {
        "validation": {"enable_custom_rules": True},
        "anomaly": {"z_score_threshold": 2.5, "enable_pattern_detection": True},
        "completeness": {
            "excellent_threshold": 0.98,
            "good_threshold": 0.95,
            "fair_threshold": 0.85,
        },
        "profiling": {"enable_pattern_detection": True, "max_distinct_values": 50},
    }

    # Initialize quality engine
    quality_engine = DataQualityEngine(spark, quality_config)

    # Process transaction stream
    logger.info("Processing Transaction Stream")
    tx_quality_df, tx_quality_report = quality_engine.assess_data_quality(
        transaction_df, stream_type="transaction", enable_profiling=True
    )

    print("Transaction Quality Report:")
    print(f"  - Quality Level: {tx_quality_report.quality_level.value}")
    print(
        f"  - Valid Records: {tx_quality_report.valid_records}/{tx_quality_report.total_records}"
    )
    print(f"  - Completeness Score: {tx_quality_report.completeness_score:.2%}")
    print(f"  - Anomalies Found: {tx_quality_report.anomaly_count}")
    print("  - Recommendations:")
    for rec in tx_quality_report.recommendations:
        print(f"    * {rec}")

    # Process user behavior stream
    logger.info("Processing User Behavior Stream")
    ub_quality_df, ub_quality_report = quality_engine.assess_data_quality(
        user_behavior_df, stream_type="user_behavior", enable_profiling=True
    )

    print("User Behavior Quality Report:")
    print(f"  - Quality Level: {ub_quality_report.quality_level.value}")
    print(
        f"  - Valid Records: {ub_quality_report.valid_records}/{ub_quality_report.total_records}"
    )
    print(f"  - Completeness Score: {ub_quality_report.completeness_score:.2%}")
    print(f"  - Anomalies Found: {ub_quality_report.anomaly_count}")

    # Show quality-enhanced data
    print("Transaction Data with Quality Flags:")
    tx_quality_df.select(
        "transaction_id",
        "user_id",
        "total_amount",
        "validation_passed",
        "is_anomaly",
        "completeness_passed",
        "data_quality_score",
        "quality_level",
    ).show()

    print("User Behavior Data with Quality Flags:")
    ub_quality_df.select(
        "user_id",
        "session_id",
        "event_type",
        "validation_passed",
        "is_anomaly",
        "completeness_passed",
        "data_quality_score",
        "quality_level",
    ).show()


def demonstrate_quality_monitoring_streams(spark: SparkSession):
    """Demonstrate quality monitoring stream creation."""

    logger = get_logger("data_quality_demo")
    logger.info("=== Demonstrating Quality Monitoring Streams ===")

    # Create sample data and process with quality engine
    transaction_df = create_sample_transaction_data(spark)
    quality_engine = DataQualityEngine(spark)

    quality_df, quality_report = quality_engine.assess_data_quality(
        transaction_df, "transaction"
    )

    # Create monitoring streams
    quality_monitoring_stream = quality_engine.create_quality_monitoring_stream(
        quality_df
    )

    print("Quality Monitoring Stream:")
    quality_monitoring_stream.show(truncate=False)

    # Generate dashboard metrics
    dashboard_metrics = quality_engine.get_quality_dashboard_metrics(quality_df)

    print("Dashboard Metrics:")
    print(f"  - Total Records: {dashboard_metrics.get('total_records', 0)}")
    print(
        f"  - Average Quality Score: {dashboard_metrics.get('average_quality_score', 0)}"
    )
    print(
        f"  - Validation Pass Rate: {dashboard_metrics.get('validation_pass_rate', 0)}%"
    )
    print(f"  - Anomaly Rate: {dashboard_metrics.get('anomaly_rate', 0)}%")
    print("  - Quality Distribution:")
    for level, stats in dashboard_metrics.get("quality_distribution", {}).items():
        print(f"    * {level}: {stats['count']} records ({stats['percentage']}%)")


def demonstrate_integration_with_transformations(spark: SparkSession):
    """Demonstrate integration with existing transformation pipeline."""

    logger = get_logger("data_quality_demo")
    logger.info("=== Demonstrating Integration with Transformation Pipeline ===")

    # Create sample data
    transaction_df = create_sample_transaction_data(spark)

    # Step 1: Data Quality Assessment
    quality_engine = DataQualityEngine(spark)
    quality_df, quality_report = quality_engine.assess_data_quality(
        transaction_df, "transaction"
    )

    # Step 2: Filter out poor quality data
    high_quality_df = quality_df.filter(
        col("validation_passed")
        & col("completeness_passed")
        & (col("data_quality_score") >= 75)
    )

    print(
        f"Filtered Data: {high_quality_df.count()}/{quality_df.count()} high-quality records retained"
    )

    # Step 3: Apply existing transformations to high-quality data
    enrichment_pipeline = DataEnrichmentPipeline(spark)
    enriched_df = enrichment_pipeline.enrich_transaction_stream(high_quality_df)

    print("Enriched High-Quality Data:")
    enriched_df.select(
        "transaction_id",
        "user_id",
        "total_amount",
        "customer_tier",
        "transaction_size",
        "risk_category",
        "data_quality_score",
    ).show()

    # Step 4: Create quality alert stream for problematic data
    problematic_df = quality_df.filter(
        ~col("validation_passed") | ~col("completeness_passed") | col("is_anomaly")
    )

    quality_alerts = (
        problematic_df.withColumn("alert_timestamp", current_timestamp())
        .withColumn("alert_type", lit("data_quality_issue"))
        .select(
            "alert_timestamp",
            "alert_type",
            "transaction_id",
            "user_id",
            "validation_passed",
            "completeness_passed",
            "is_anomaly",
            "data_quality_score",
        )
    )

    print("Quality Alert Stream:")
    quality_alerts.show()


def main():
    """Main demonstration function."""

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("DataQualityFrameworkDemo")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        print("🔍 E-Commerce Analytics Platform - Data Quality Framework Demo")
        print("=" * 70)

        # Run demonstrations
        demonstrate_individual_components(spark)
        print("\n" + "=" * 70)

        demonstrate_integrated_quality_engine(spark)
        print("\n" + "=" * 70)

        demonstrate_quality_monitoring_streams(spark)
        print("\n" + "=" * 70)

        demonstrate_integration_with_transformations(spark)
        print("\n" + "=" * 70)

        print("✅ Data Quality Framework demonstration completed successfully!")

    except Exception as e:
        print(f"❌ Error in demonstration: {e}")
        import traceback

        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

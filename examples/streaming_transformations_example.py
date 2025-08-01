#!/usr/bin/env python3
"""
Example usage of streaming transformations for real-time data processing.

This example demonstrates:
1. Data enrichment pipelines
2. Real-time aggregations with windowing
3. Stream-to-stream joins
4. Data deduplication
"""

import logging
from datetime import datetime
from typing import Optional

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, lit
    from pyspark.sql.types import *

    PYSPARK_AVAILABLE = True
except ImportError:
    print("PySpark not available. This example requires PySpark to run.")
    PYSPARK_AVAILABLE = False

from src.streaming.transformations import (
    DataEnrichmentPipeline,
    StreamDeduplicator,
    StreamingAggregator,
    StreamJoinEngine,
)
from src.utils.logger import get_logger


class StreamingTransformationsDemo:
    """Demonstration of streaming transformations capabilities."""

    def __init__(self):
        """Initialize the demo environment."""
        self.logger = get_logger(__name__)
        self.spark = None

        if PYSPARK_AVAILABLE:
            self._setup_spark()

    def _setup_spark(self):
        """Set up Spark session for the demo."""
        try:
            self.spark = (
                SparkSession.builder.appName("StreamingTransformationsDemo")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate()
            )

            # Reduce log verbosity
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark session created successfully")

        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {e}")
            raise

    def demonstrate_data_enrichment(self):
        """Demonstrate data enrichment pipeline."""
        print("\\n=== Data Enrichment Pipeline Demo ===")

        if not PYSPARK_AVAILABLE:
            print("PySpark not available - showing conceptual flow")
            self._show_enrichment_concept()
            return

        try:
            # Create sample transaction data
            transaction_schema = StructType(
                [
                    StructField("user_id", StringType(), True),
                    StructField("transaction_id", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("total_amount", DoubleType(), True),
                    StructField("payment_method", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("customer_tier", StringType(), True),
                    StructField("customer_lifetime_value", DoubleType(), True),
                ]
            )

            sample_data = [
                (
                    "user_001",
                    "tx_001",
                    "prod_123",
                    datetime.now(),
                    99.99,
                    "credit_card",
                    "New York",
                    "Electronics/Phones",
                    99.99,
                    1,
                    "Gold",
                    1500.0,
                ),
                (
                    "user_002",
                    "tx_002",
                    "prod_456",
                    datetime.now(),
                    249.50,
                    "paypal",
                    "California",
                    "Clothing/Shoes",
                    249.50,
                    1,
                    "Silver",
                    800.0,
                ),
                (
                    "user_003",
                    "tx_003",
                    "prod_789",
                    datetime.now(),
                    1299.99,
                    "credit_card",
                    "Texas",
                    "Electronics/Computers",
                    1299.99,
                    1,
                    "Premium",
                    3000.0,
                ),
            ]

            transaction_df = self.spark.createDataFrame(sample_data, transaction_schema)

            # Initialize enrichment pipeline
            enrichment_pipeline = DataEnrichmentPipeline(self.spark)

            # Apply enrichment
            enriched_df = enrichment_pipeline.enrich_transaction_stream(transaction_df)

            print("Sample enriched transaction data:")
            enriched_df.select(
                "user_id",
                "total_amount",
                "customer_tier_normalized",
                "ltv_category",
                "price_category",
                "time_period",
                "risk_category",
                "transaction_size",
            ).show(truncate=False)

            print("Enrichment completed successfully!")

        except Exception as e:
            self.logger.error(f"Error in enrichment demo: {e}")
            print(f"Demo error: {e}")

    def demonstrate_real_time_aggregations(self):
        """Demonstrate real-time aggregations with windowing."""
        print("\\n=== Real-time Aggregations Demo ===")

        if not PYSPARK_AVAILABLE:
            print("PySpark not available - showing conceptual flow")
            self._show_aggregation_concept()
            return

        try:
            # Create sample transaction stream
            transaction_schema = StructType(
                [
                    StructField("user_id", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("total_amount", DoubleType(), True),
                    StructField("location", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("customer_lifetime_value", DoubleType(), True),
                    StructField("customer_tier", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("risk_category", StringType(), True),
                    StructField("payment_method", StringType(), True),
                ]
            )

            # Generate sample data with different timestamps
            import random
            from datetime import timedelta

            base_time = datetime.now()
            sample_data = []
            for i in range(10):
                sample_data.append(
                    (
                        f"user_{i%5:03d}",
                        base_time + timedelta(minutes=i),
                        round(random.uniform(10, 500), 2),
                        random.choice(["New York", "California", "Texas"]),
                        f"prod_{random.randint(100, 999)}",
                        random.randint(1, 5),
                        round(random.uniform(100, 2000), 2),
                        random.choice(["Bronze", "Silver", "Gold", "Premium"]),
                        random.choice(["Electronics", "Clothing", "Books"]),
                        round(random.uniform(10, 200), 2),
                        random.choice(["Low", "Medium", "High"]),
                        random.choice(["credit_card", "paypal", "cash"]),
                    )
                )

            transaction_df = self.spark.createDataFrame(sample_data, transaction_schema)

            # Initialize aggregator
            aggregator = StreamingAggregator(self.spark)

            # Create real-time KPIs (simulate streaming with batch for demo)
            kpi_df = aggregator.create_real_time_kpis(
                transaction_df, window_duration="5 minutes", slide_duration="1 minute"
            )

            print("Real-time KPI aggregations:")
            kpi_df.select(
                "window",
                "region",
                "total_revenue",
                "total_orders",
                "unique_customers",
                "avg_order_value",
                "revenue_per_customer",
            ).show(truncate=False)

            print("Aggregation completed successfully!")

        except Exception as e:
            self.logger.error(f"Error in aggregation demo: {e}")
            print(f"Demo error: {e}")

    def demonstrate_stream_joins(self):
        """Demonstrate stream-to-stream joins."""
        print("\\n=== Stream-to-Stream Joins Demo ===")

        if not PYSPARK_AVAILABLE:
            print("PySpark not available - showing conceptual flow")
            self._show_join_concept()
            return

        try:
            # Create transaction stream
            transaction_schema = StructType(
                [
                    StructField("user_id", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("transaction_id", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("total_amount", DoubleType(), True),
                    StructField("payment_method", StringType(), True),
                    StructField("location", StringType(), True),
                ]
            )

            # Create behavior stream
            behavior_schema = StructType(
                [
                    StructField("user_id", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("session_id", StringType(), True),
                    StructField("event_type", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("engagement_score", IntegerType(), True),
                    StructField("purchase_intent_score", IntegerType(), True),
                ]
            )

            base_time = datetime.now()

            # Sample transaction data
            tx_data = [
                (
                    "user_001",
                    base_time + timedelta(minutes=5),
                    "tx_001",
                    "prod_123",
                    99.99,
                    "credit_card",
                    "New York",
                ),
                (
                    "user_002",
                    base_time + timedelta(minutes=8),
                    "tx_002",
                    "prod_456",
                    149.50,
                    "paypal",
                    "California",
                ),
            ]

            # Sample behavior data (events leading to transactions)
            behavior_data = [
                (
                    "user_001",
                    base_time + timedelta(minutes=2),
                    "sess_001",
                    "product_view",
                    "prod_123",
                    60,
                    70,
                ),
                (
                    "user_001",
                    base_time + timedelta(minutes=3),
                    "sess_001",
                    "add_to_cart",
                    "prod_123",
                    80,
                    85,
                ),
                (
                    "user_002",
                    base_time + timedelta(minutes=6),
                    "sess_002",
                    "search",
                    None,
                    40,
                    30,
                ),
                (
                    "user_002",
                    base_time + timedelta(minutes=7),
                    "sess_002",
                    "product_view",
                    "prod_456",
                    65,
                    75,
                ),
            ]

            transaction_df = self.spark.createDataFrame(tx_data, transaction_schema)
            behavior_df = self.spark.createDataFrame(behavior_data, behavior_schema)

            # Initialize join engine
            join_engine = StreamJoinEngine(self.spark)

            # Join transaction and behavior streams
            correlated_df = join_engine.join_transaction_behavior_streams(
                transaction_df, behavior_df, join_window="10 minutes"
            )

            print("Correlated transaction and behavior data:")
            correlated_df.select(
                "tx_user_id",
                "tx_amount",
                "behavior_event_type",
                "time_to_purchase_minutes",
                "product_match",
                "conversion_path",
            ).show(truncate=False)

            print("Stream join completed successfully!")

        except Exception as e:
            self.logger.error(f"Error in join demo: {e}")
            print(f"Demo error: {e}")

    def demonstrate_deduplication(self):
        """Demonstrate data deduplication."""
        print("\\n=== Data Deduplication Demo ===")

        if not PYSPARK_AVAILABLE:
            print("PySpark not available - showing conceptual flow")
            self._show_deduplication_concept()
            return

        try:
            # Create sample data with duplicates
            schema = StructType(
                [
                    StructField("user_id", StringType(), True),
                    StructField("transaction_id", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("total_amount", DoubleType(), True),
                    StructField("payment_method", StringType(), True),
                ]
            )

            base_time = datetime.now()

            # Include duplicate and near-duplicate transactions
            sample_data = [
                ("user_001", "tx_001", base_time, "prod_123", 99.99, "credit_card"),
                (
                    "user_001",
                    "tx_001",
                    base_time + timedelta(seconds=5),
                    "prod_123",
                    99.99,
                    "credit_card",
                ),  # Exact duplicate
                (
                    "user_001",
                    "tx_002",
                    base_time + timedelta(minutes=1),
                    "prod_123",
                    99.95,
                    "credit_card",
                ),  # Similar amount
                (
                    "user_002",
                    "tx_003",
                    base_time + timedelta(minutes=2),
                    "prod_456",
                    149.50,
                    "paypal",
                ),
                (
                    "user_002",
                    "tx_004",
                    base_time + timedelta(minutes=3),
                    "prod_789",
                    75.00,
                    "cash",
                ),
                (
                    "user_001",
                    "tx_005",
                    base_time + timedelta(minutes=4),
                    "prod_123",
                    99.99,
                    "credit_card",
                ),  # Potential duplicate
            ]

            df_with_duplicates = self.spark.createDataFrame(sample_data, schema)

            print("Original data (with duplicates):")
            df_with_duplicates.show(truncate=False)
            print(f"Original count: {df_with_duplicates.count()}")

            # Initialize deduplicator
            deduplicator = StreamDeduplicator(self.spark)

            # Apply exact deduplication
            exact_dedup_df = deduplicator.deduplicate_exact_events(
                df_with_duplicates,
                dedup_keys=["user_id", "transaction_id", "product_id", "total_amount"],
                keep_strategy="first",
            )

            print("\\nAfter exact deduplication:")
            exact_dedup_df.select(
                "user_id",
                "transaction_id",
                "total_amount",
                "duplicate_type",
                "deduplication_method",
            ).show(truncate=False)
            print(f"Deduplicated count: {exact_dedup_df.count()}")

            # Apply transaction-specific deduplication with fuzzy matching
            tx_dedup_df = deduplicator.deduplicate_transactions(
                df_with_duplicates, similarity_threshold=0.95
            )

            print("\\nAfter transaction deduplication with fuzzy matching:")
            tx_dedup_df.select(
                "user_id",
                "transaction_id",
                "total_amount",
                "duplicate_type",
                "overall_similarity",
            ).show(truncate=False)
            print(f"Final count: {tx_dedup_df.count()}")

            print("Deduplication completed successfully!")

        except Exception as e:
            self.logger.error(f"Error in deduplication demo: {e}")
            print(f"Demo error: {e}")

    def _show_enrichment_concept(self):
        """Show enrichment concept without PySpark."""
        print(
            """
        Data Enrichment Pipeline Concept:

        Input: Raw transaction stream
        ├── Customer enrichment (tier, LTV category)
        ├── Product enrichment (category hierarchy, price category)
        ├── Temporal enrichment (time periods, business hours)
        ├── Geographic enrichment (region mapping, area type)
        ├── Business metrics (revenue impact, transaction size)
        └── Risk indicators (risk score, anomaly flags)

        Output: Enriched stream with 20+ additional features
        """
        )

    def _show_aggregation_concept(self):
        """Show aggregation concept without PySpark."""
        print(
            """
        Real-time Aggregations Concept:

        Windowed KPIs (5-minute tumbling windows):
        ├── Revenue metrics (total, average, min/max)
        ├── Volume metrics (orders, customers, products)
        ├── Customer metrics (LTV, tiers active)
        ├── Quality metrics (risk %, payment methods)
        └── Calculated ratios (revenue per customer, items per order)

        Output: Real-time business dashboards and alerts
        """
        )

    def _show_join_concept(self):
        """Show join concept without PySpark."""
        print(
            """
        Stream-to-Stream Joins Concept:

        Transaction Stream + Behavior Stream:
        ├── Time-based correlation (within 10-minute window)
        ├── User journey reconstruction
        ├── Purchase attribution to marketing touchpoints
        ├── Conversion path analysis
        └── Product recommendation effectiveness

        Output: Complete customer journey with purchase correlation
        """
        )

    def _show_deduplication_concept(self):
        """Show deduplication concept without PySpark."""
        print(
            """
        Data Deduplication Concept:

        Duplicate Detection Strategies:
        ├── Exact duplicates (identical key fields)
        ├── Fuzzy duplicates (similarity threshold matching)
        ├── Temporal duplicates (same user, rapid succession)
        ├── Business logic duplicates (transaction-specific rules)
        └── Late arrival handling (watermark-based)

        Output: Clean, deduplicated stream with quality metadata
        """
        )

    def run_complete_demo(self):
        """Run the complete transformations demonstration."""
        print("=== Streaming Transformations Comprehensive Demo ===")
        print("Demonstrating real-time data processing capabilities...")

        try:
            self.demonstrate_data_enrichment()
            self.demonstrate_real_time_aggregations()
            self.demonstrate_stream_joins()
            self.demonstrate_deduplication()

            print("\\n=== Demo Completed Successfully ===")
            print("All transformation capabilities demonstrated!")

        except Exception as e:
            self.logger.error(f"Demo failed: {e}")
            print(f"Demo failed: {e}")

        finally:
            if self.spark:
                self.spark.stop()
                print("Spark session stopped.")


def main():
    """Main entry point for the demo."""
    print("Starting Streaming Transformations Demo...")

    demo = StreamingTransformationsDemo()
    demo.run_complete_demo()


if __name__ == "__main__":
    main()

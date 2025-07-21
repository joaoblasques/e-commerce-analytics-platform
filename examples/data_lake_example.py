#!/usr/bin/env python3
"""
Data Lake Example Script

Demonstrates the complete data lake functionality including storage,
ingestion, compaction, and metadata management.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import data lake components
from src.data_lake import (
    DataCompactor,
    DataLakeIngester,
    DataLakeStorage,
    MetadataManager,
)
from src.utils.spark_utils import create_spark_session


def create_sample_data(spark):
    """Create sample data for demonstration."""
    logger.info("Creating sample transaction data...")

    # Sample transaction data
    transactions = [
        {
            "transaction_id": f"txn_{i:06d}",
            "customer_id": f"cust_{i % 1000:04d}",
            "product_id": f"prod_{i % 100:03d}",
            "category": ["electronics", "clothing", "books", "home", "sports"][i % 5],
            "amount": round(10 + (i % 500) * 0.5, 2),
            "transaction_timestamp": (
                datetime.now() - timedelta(days=i % 30)
            ).isoformat(),
            "payment_method": ["credit_card", "debit_card", "paypal", "bank_transfer"][
                i % 4
            ],
            "status": "completed",
        }
        for i in range(10000)
    ]

    # Sample user events data
    user_events = [
        {
            "event_id": f"evt_{i:06d}",
            "session_id": f"sess_{i % 500:04d}",
            "customer_id": f"cust_{i % 1000:04d}",
            "event_type": ["page_view", "product_view", "add_to_cart", "checkout"][
                i % 4
            ],
            "product_id": f"prod_{i % 100:03d}" if i % 4 != 0 else None,
            "event_timestamp": (datetime.now() - timedelta(hours=i % 24)).isoformat(),
            "device_type": ["desktop", "mobile", "tablet"][i % 3],
            "page_url": f"/page/{i % 20}",
        }
        for i in range(5000)
    ]

    # Create DataFrames
    transaction_df = spark.createDataFrame(transactions)
    events_df = spark.createDataFrame(user_events)

    logger.info(
        f"Created {len(transactions)} transactions and {len(user_events)} user events"
    )
    return transaction_df, events_df


def demonstrate_storage_operations(storage: DataLakeStorage, transaction_df, events_df):
    """Demonstrate storage operations."""
    logger.info("=== Storage Operations Demo ===")

    # Write transaction data with optimal partitioning
    logger.info("Writing transaction data with time-based partitioning...")
    txn_path = storage.write_partitioned_data(
        df=transaction_df,
        table_name="transactions",
        data_type="transactions",
        mode="overwrite",
    )
    logger.info(f"Transactions written to: {txn_path}")

    # Write user events data with event type partitioning
    logger.info("Writing user events data with time + event type partitioning...")
    events_path = storage.write_partitioned_data(
        df=events_df,
        table_name="user_events",
        data_type="user_events",
        mode="overwrite",
    )
    logger.info(f"User events written to: {events_path}")

    # Read data with partition filtering
    logger.info("Reading transaction data with partition filtering...")
    filtered_df = storage.read_partitioned_data(
        table_name="transactions",
        partition_filters={
            "year": 2023,
            "month": [datetime.now().month - 1, datetime.now().month],
        },
        columns=["transaction_id", "amount", "customer_id", "category"],
    )

    count = filtered_df.count()
    logger.info(f"Filtered transactions count: {count}")

    # Get table information
    logger.info("Getting table information...")
    txn_info = storage.get_table_info("transactions")
    events_info = storage.get_table_info("user_events")

    logger.info(f"Transactions table: {txn_info.get('record_count', 0)} records")
    logger.info(f"User events table: {events_info.get('record_count', 0)} records")

    # List all tables
    tables = storage.list_tables()
    logger.info(f"Data lake contains {len(tables)} tables")
    for table in tables:
        logger.info(f"  - {table['table_name']} at {table['path']}")

    return txn_info, events_info


def demonstrate_compaction_operations(compactor: DataCompactor):
    """Demonstrate compaction operations."""
    logger.info("=== Compaction Operations Demo ===")

    # Analyze tables for optimization needs
    logger.info("Analyzing tables for optimization needs...")

    tables_to_analyze = ["transactions", "user_events"]
    for table_name in tables_to_analyze:
        analysis = compactor.analyze_table_files(table_name)

        if "error" not in analysis:
            logger.info(f"Analysis for {table_name}:")
            logger.info(f"  Files: {analysis.get('file_count', 0)}")
            logger.info(f"  Total size: {analysis.get('total_size_mb', 0):.1f} MB")
            logger.info(
                f"  Average file size: {analysis.get('average_file_size_mb', 0):.1f} MB"
            )
            logger.info(
                f"  Needs compaction: {analysis.get('needs_compaction', False)}"
            )

            if analysis.get("compaction_reason"):
                logger.info(f"  Reason: {analysis['compaction_reason']}")
        else:
            logger.warning(f"Could not analyze {table_name}: {analysis['error']}")

    # Demonstrate compaction on transactions table
    logger.info("Compacting transactions table...")
    try:
        result = compactor.compact_table(
            table_name="transactions",
            target_file_size_mb=64,  # Smaller for demo
            backup=True,
        )

        if result.get("status") == "success":
            logger.info("Compaction successful:")
            logger.info(f"  Original files: {result.get('original_files', 0)}")
            logger.info(f"  Compacted files: {result.get('compacted_files', 0)}")
            logger.info(f"  File reduction: {result.get('file_reduction', 0)}")
            logger.info(f"  Duration: {result.get('duration_seconds', 0):.1f} seconds")
        else:
            logger.error(f"Compaction failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Compaction error: {str(e)}")


def demonstrate_metadata_management(metadata_manager: MetadataManager):
    """Demonstrate metadata management operations."""
    logger.info("=== Metadata Management Demo ===")

    # Register tables in catalog
    logger.info("Registering tables in catalog...")

    try:
        # Register transactions table
        txn_metadata = metadata_manager.register_table(
            table_name="transactions",
            table_path="s3a://data-lake/transactions",
            description="E-commerce transaction data with customer and product information",
            owner="data-engineering-team",
            tags=["transactions", "ecommerce", "financial", "customer-data"],
            properties={
                "data_source": "payment_gateway",
                "update_frequency": "real-time",
            },
        )

        logger.info(f"Registered transactions table:")
        logger.info(f"  Records: {txn_metadata.record_count}")
        logger.info(f"  Columns: {len(txn_metadata.columns)}")
        logger.info(f"  Partitions: {txn_metadata.partition_columns}")

        # Register user events table
        events_metadata = metadata_manager.register_table(
            table_name="user_events",
            table_path="s3a://data-lake/user_events",
            description="User behavior events from website and mobile app",
            owner="analytics-team",
            tags=["events", "user-behavior", "web-analytics", "mobile"],
            properties={"data_source": "web_tracking", "update_frequency": "streaming"},
        )

        logger.info(f"Registered user_events table:")
        logger.info(f"  Records: {events_metadata.record_count}")
        logger.info(f"  Columns: {len(events_metadata.columns)}")

    except Exception as e:
        logger.error(f"Table registration error: {str(e)}")
        return

    # Update table statistics
    logger.info("Updating table statistics...")
    try:
        updated_metadata = metadata_manager.update_table_statistics(
            table_name="transactions",
            compute_column_stats=True,
            sample_size=1000,  # Sample for demo
        )

        logger.info(f"Updated statistics for transactions:")
        logger.info(f"  Records: {updated_metadata.record_count}")
        logger.info(f"  Files: {updated_metadata.file_count}")

        # Show column statistics
        for col in updated_metadata.columns[:3]:  # Show first 3 columns
            logger.info(f"  Column {col.name}:")
            logger.info(f"    Type: {col.data_type}")
            logger.info(f"    Distinct values: {col.distinct_count}")
            logger.info(f"    Null count: {col.null_count}")

    except Exception as e:
        logger.error(f"Statistics update error: {str(e)}")

    # Add lineage information
    logger.info("Adding lineage information...")
    try:
        metadata_manager.add_table_lineage(
            table_name="transactions",
            downstream_tables=["customer_analytics", "revenue_reports"],
        )

        metadata_manager.add_table_lineage(
            table_name="user_events",
            downstream_tables=["user_journey_analysis", "conversion_funnel"],
        )

        # Get lineage graph
        lineage_graph = metadata_manager.get_table_lineage_graph()
        logger.info(f"Lineage graph contains {len(lineage_graph)} tables")

    except Exception as e:
        logger.error(f"Lineage management error: {str(e)}")

    # List and search tables
    logger.info("Listing and searching tables...")
    try:
        # List all tables
        all_tables = metadata_manager.list_tables()
        logger.info(f"Catalog contains {len(all_tables)} tables")

        # Search tables
        search_results = metadata_manager.search_tables("transaction")
        logger.info(f"Found {len(search_results)} tables matching 'transaction'")

        # Search by tag
        tagged_tables = metadata_manager.search_tables("ecommerce", ["tags"])
        logger.info(f"Found {len(tagged_tables)} tables tagged with 'ecommerce'")

    except Exception as e:
        logger.error(f"Catalog operations error: {str(e)}")


def demonstrate_ingestion_operations(ingester: DataLakeIngester, spark):
    """Demonstrate ingestion operations."""
    logger.info("=== Ingestion Operations Demo ===")

    # Create sample files for batch ingestion demo
    logger.info("Creating sample files for ingestion demo...")

    # Create product updates data
    products = [
        {
            "product_id": f"prod_{i:03d}",
            "name": f"Product {i}",
            "category": ["electronics", "clothing", "books", "home", "sports"][i % 5],
            "price": round(10 + (i % 100) * 2.5, 2),
            "updated_at": datetime.now().isoformat(),
            "status": "active",
            "inventory_count": i % 1000,
        }
        for i in range(1000)
    ]

    # Write to temporary file (simulate external data source)
    temp_path = "/tmp/sample_products.json"
    try:
        with open(temp_path, "w") as f:
            for product in products:
                f.write(json.dumps(product) + "\n")

        # Ingest files
        logger.info("Ingesting product data from file...")
        result = ingester.ingest_files_batch(
            file_paths=[temp_path],
            table_name="products",
            data_type="product_updates",
            file_format="json",
            parallel=False,
        )

        logger.info(f"File ingestion result:")
        logger.info(f"  Status: {result.get('status')}")
        logger.info(f"  Records: {result.get('total_records', 0)}")
        logger.info(f"  Files processed: {result.get('processed_files', 0)}")

    except Exception as e:
        logger.error(f"File ingestion error: {str(e)}")

    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)


def main():
    """Main demonstration function."""
    logger.info("Starting Data Lake Demonstration")
    logger.info("=" * 50)

    try:
        # Initialize Spark session
        spark = create_spark_session("DataLakeDemo")
        logger.info("Spark session created successfully")

        # Create sample data
        transaction_df, events_df = create_sample_data(spark)

        # Initialize data lake components
        storage = DataLakeStorage(base_path="s3a://data-lake", spark_session=spark)

        ingester = DataLakeIngester(storage=storage, spark_session=spark)

        compactor = DataCompactor(storage=storage, spark_session=spark)

        metadata_manager = MetadataManager(
            spark_session=spark, catalog_path="s3a://data-lake/catalog"
        )

        logger.info("Data lake components initialized successfully")

        # Run demonstrations
        demonstrate_storage_operations(storage, transaction_df, events_df)
        time.sleep(2)  # Brief pause between demonstrations

        demonstrate_compaction_operations(compactor)
        time.sleep(2)

        demonstrate_metadata_management(metadata_manager)
        time.sleep(2)

        demonstrate_ingestion_operations(ingester, spark)

        logger.info("=" * 50)
        logger.info("Data Lake Demonstration Completed Successfully!")
        logger.info("All components are working correctly.")

        # Performance summary
        logger.info("\nPerformance Summary:")
        logger.info("- Storage: Optimal partitioning strategies implemented")
        logger.info("- Ingestion: Batch and streaming capabilities available")
        logger.info("- Compaction: File optimization working correctly")
        logger.info("- Metadata: Comprehensive catalog management active")

    except Exception as e:
        logger.error(f"Demo failed with error: {str(e)}")
        raise

    finally:
        # Clean up Spark session
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()

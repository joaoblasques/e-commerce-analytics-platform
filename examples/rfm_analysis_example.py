"""
RFM Customer Segmentation Example

Demonstrates how to use the RFM segmentation system with sample data
and integrate it with the existing data pipeline.
"""

import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import when
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from src.analytics.rfm_segmentation import RFMSegmentationEngine
from src.data_lake.delta import DeltaLakeManager
from src.utils.logger import setup_logging

logger = setup_logging(__name__)


def create_sample_transaction_data(
    spark: SparkSession, num_customers: int = 1000, num_transactions: int = 5000
):
    """
    Create realistic sample e-commerce transaction data for RFM analysis.

    Args:
        spark: Spark session
        num_customers: Number of unique customers to generate
        num_transactions: Total number of transactions to generate

    Returns:
        DataFrame with sample transaction data
    """
    logger.info(
        f"Generating {num_transactions} transactions for {num_customers} customers"
    )

    # Define customer behavior patterns
    customer_patterns = [
        {
            "type": "champion",
            "frequency_mult": 3.0,
            "amount_mult": 2.5,
            "recent_bias": 0.8,
            "weight": 0.05,
        },
        {
            "type": "loyal",
            "frequency_mult": 2.0,
            "amount_mult": 2.0,
            "recent_bias": 0.6,
            "weight": 0.15,
        },
        {
            "type": "potential",
            "frequency_mult": 1.5,
            "amount_mult": 1.5,
            "recent_bias": 0.7,
            "weight": 0.20,
        },
        {
            "type": "new",
            "frequency_mult": 0.8,
            "amount_mult": 1.2,
            "recent_bias": 0.9,
            "weight": 0.15,
        },
        {
            "type": "at_risk",
            "frequency_mult": 2.0,
            "amount_mult": 2.2,
            "recent_bias": 0.2,
            "weight": 0.10,
        },
        {
            "type": "hibernating",
            "frequency_mult": 1.2,
            "amount_mult": 1.5,
            "recent_bias": 0.1,
            "weight": 0.15,
        },
        {
            "type": "lost",
            "frequency_mult": 0.8,
            "amount_mult": 1.0,
            "recent_bias": 0.05,
            "weight": 0.20,
        },
    ]

    # Create customer base
    customers_data = []
    current_customer_id = 1

    for pattern in customer_patterns:
        pattern_customers = int(num_customers * pattern["weight"])
        for _ in range(pattern_customers):
            customers_data.append(
                {
                    "customer_id": f"customer_{current_customer_id:06d}",
                    "customer_type": pattern["type"],
                    "frequency_mult": pattern["frequency_mult"],
                    "amount_mult": pattern["amount_mult"],
                    "recent_bias": pattern["recent_bias"],
                }
            )
            current_customer_id += 1

    # Generate transactions
    transactions_data = []
    reference_date = datetime.now()

    for _ in range(num_transactions):
        customer = random.choice(customers_data)

        # Generate transaction date based on customer pattern
        days_back = random.randint(1, 365)
        if random.random() < customer["recent_bias"]:
            # Bias toward recent dates for this customer type
            days_back = random.randint(1, 60)

        transaction_date = reference_date - timedelta(days=days_back)

        # Generate transaction amount based on customer pattern
        base_amount = random.uniform(25, 500)
        transaction_amount = base_amount * customer["amount_mult"]

        # Add some randomness to make it realistic
        transaction_amount *= random.uniform(0.5, 2.0)
        transaction_amount = round(transaction_amount, 2)

        transactions_data.append(
            {
                "customer_id": customer["customer_id"],
                "transaction_date": transaction_date.date(),
                "amount": transaction_amount,
                "customer_type": customer["customer_type"],  # For validation
            }
        )

    # Create DataFrame
    schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", DateType(), True),
            StructField("amount", DoubleType(), True),
            StructField("customer_type", StringType(), True),
        ]
    )

    transactions_df = spark.createDataFrame(transactions_data, schema)
    logger.info(f"Generated {transactions_df.count()} transactions")

    return transactions_df


def run_basic_rfm_analysis():
    """Run basic RFM analysis example."""
    logger.info("Starting basic RFM analysis example")

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("RFM-Analysis-Example")
        .master("local[4]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        # Generate sample data
        transactions_df = create_sample_transaction_data(
            spark, num_customers=500, num_transactions=2000
        )

        # Show sample data
        print("\nSample Transaction Data:")
        transactions_df.show(10)
        print(f"Total transactions: {transactions_df.count()}")
        print(
            f"Unique customers: {transactions_df.select('customer_id').distinct().count()}"
        )

        # Initialize RFM engine
        reference_date = datetime.now()
        rfm_engine = RFMSegmentationEngine(
            spark_session=spark, reference_date=reference_date, quintiles=True
        )

        # Generate customer profiles
        logger.info("Generating customer profiles with RFM analysis")
        customer_profiles = rfm_engine.generate_customer_profiles(
            transactions_df.drop("customer_type")  # Remove validation column
        )

        # Show sample profiles
        print("\nCustomer Profiles with RFM Scores:")
        customer_profiles.select(
            "customer_id",
            "recency_days",
            "frequency",
            "monetary",
            "recency_score",
            "frequency_score",
            "monetary_score",
            "rfm_score",
            "segment",
            "customer_value_tier",
        ).show(15, truncate=False)

        # Generate segment summary
        segment_summary = rfm_engine.get_segment_summary(customer_profiles)
        print("\nSegment Summary:")
        segment_summary.orderBy("customer_count", ascending=False).show(truncate=False)

        # Identify high-value customers
        high_value_customers = rfm_engine.identify_high_value_customers(
            customer_profiles, top_percentage=0.2
        )

        print(
            f"\nHigh-Value Customers (Top 20%): {high_value_customers.count()} customers"
        )
        high_value_customers.select(
            "customer_id",
            "segment",
            "monetary",
            "frequency",
            "composite_score",
            "customer_rank",
        ).orderBy("composite_score", ascending=False).show(10)

        # Generate action recommendations
        profiles_with_actions = rfm_engine.recommend_actions(customer_profiles)
        print("\nSample Action Recommendations:")
        profiles_with_actions.select(
            "customer_id", "segment", "recommended_action"
        ).distinct().show(10, truncate=False)

        # Business insights
        print("\n" + "=" * 80)
        print("BUSINESS INSIGHTS")
        print("=" * 80)

        total_customers = customer_profiles.count()
        total_revenue = customer_profiles.agg({"monetary": "sum"}).collect()[0][0]
        avg_customer_value = total_revenue / total_customers

        print(f"Total Customers: {total_customers:,}")
        print(f"Total Revenue: ${total_revenue:,.2f}")
        print(f"Average Customer Value: ${avg_customer_value:.2f}")

        # At-risk customer analysis
        at_risk_customers = customer_profiles.filter(
            col("segment").isin(["At Risk", "Cannot Lose Them", "About to Sleep"])
        )
        at_risk_count = at_risk_customers.count()
        at_risk_revenue = (
            at_risk_customers.agg({"monetary": "sum"}).collect()[0][0] or 0
        )

        print(f"\nAt-Risk Analysis:")
        print(
            f"At-Risk Customers: {at_risk_count} ({at_risk_count/total_customers*100:.1f}%)"
        )
        print(
            f"Revenue at Risk: ${at_risk_revenue:,.2f} ({at_risk_revenue/total_revenue*100:.1f}%)"
        )

        logger.info("Basic RFM analysis example completed successfully")

    finally:
        spark.stop()


def run_delta_lake_integration_example():
    """Demonstrate integration with Delta Lake."""
    logger.info("Starting Delta Lake integration example")

    # Initialize Spark with Delta Lake
    spark = (
        SparkSession.builder.appName("RFM-Delta-Integration")
        .master("local[4]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    try:
        # Initialize Delta Lake manager
        delta_manager = DeltaLakeManager(
            base_path="/tmp/delta-lake-rfm", spark_session=spark
        )

        # Generate and store sample transaction data
        transactions_df = create_sample_transaction_data(
            spark, num_customers=300, num_transactions=1500
        )

        # Create Delta table for transactions
        logger.info("Creating Delta Lake table for transactions")
        table_properties = delta_manager.create_table_properties(
            auto_optimize=True, auto_compact=True
        )

        transactions_table_path = delta_manager.create_delta_table(
            table_name="ecommerce_transactions",
            schema=transactions_df.schema,
            partition_columns=["customer_type"],
            properties=table_properties,
        )

        # Write transaction data to Delta Lake
        delta_manager.write_to_delta(
            transactions_df, "ecommerce_transactions", mode="overwrite"
        )

        # Read transactions from Delta Lake
        stored_transactions = delta_manager.time_travel_read("ecommerce_transactions")
        print(f"Transactions stored in Delta Lake: {stored_transactions.count()}")

        # Run RFM analysis on Delta Lake data
        rfm_engine = RFMSegmentationEngine(
            spark_session=spark, reference_date=datetime.now(), quintiles=True
        )

        customer_profiles = rfm_engine.generate_customer_profiles(
            stored_transactions.drop("customer_type")
        )

        # Store RFM results in Delta Lake
        logger.info("Storing RFM analysis results in Delta Lake")

        # Create customer profiles table
        profiles_table_path = delta_manager.create_delta_table(
            table_name="customer_profiles",
            schema=customer_profiles.schema,
            partition_columns=["segment"],
            properties=table_properties,
        )

        delta_manager.write_to_delta(
            customer_profiles, "customer_profiles", mode="overwrite"
        )

        # Create segment summary table
        segment_summary = rfm_engine.get_segment_summary(customer_profiles)

        summary_table_path = delta_manager.create_delta_table(
            table_name="segment_summary",
            schema=segment_summary.schema,
            properties=table_properties,
        )

        delta_manager.write_to_delta(
            segment_summary, "segment_summary", mode="overwrite"
        )

        # Demonstrate time travel capabilities
        print("\nDemonstrating Delta Lake time travel:")
        current_profiles = delta_manager.time_travel_read("customer_profiles")
        print(f"Current profiles count: {current_profiles.count()}")

        # Show table information
        table_info = delta_manager.get_table_info("customer_profiles")
        print(f"\nCustomer profiles table info:")
        for key, value in table_info.items():
            print(f"  {key}: {value}")

        # List all tables
        tables = delta_manager.list_tables()
        print(f"\nDelta Lake tables created:")
        for table in tables:
            print(f"  - {table['name']}: {table['path']}")

        logger.info("Delta Lake integration example completed successfully")

    finally:
        delta_manager.close()
        spark.stop()


def run_streaming_rfm_example():
    """Demonstrate streaming RFM updates."""
    logger.info("Starting streaming RFM example")

    spark = (
        SparkSession.builder.appName("RFM-Streaming-Example")
        .master("local[4]")
        .config(
            "spark.sql.streaming.checkpointLocation", "/tmp/rfm-streaming-checkpoint"
        )
        .getOrCreate()
    )

    try:
        # Create base customer profiles
        transactions_df = create_sample_transaction_data(
            spark, num_customers=200, num_transactions=1000
        )

        rfm_engine = RFMSegmentationEngine(
            spark_session=spark, reference_date=datetime.now(), quintiles=True
        )

        base_profiles = rfm_engine.generate_customer_profiles(
            transactions_df.drop("customer_type")
        )

        print("Base Customer Segments Distribution:")
        base_profiles.groupBy("segment").count().orderBy(
            "count", ascending=False
        ).show()

        # Simulate new transactions (streaming data)
        new_transactions = create_sample_transaction_data(
            spark, num_customers=50, num_transactions=200
        ).drop("customer_type")

        # Update RFM analysis with new data
        print("\nProcessing new transactions...")
        combined_transactions = transactions_df.drop("customer_type").union(
            new_transactions
        )

        updated_profiles = rfm_engine.generate_customer_profiles(combined_transactions)

        print("Updated Customer Segments Distribution:")
        updated_profiles.groupBy("segment").count().orderBy(
            "count", ascending=False
        ).show()

        # Identify segment changes
        segment_changes = (
            base_profiles.select("customer_id", "segment")
            .withColumnRenamed("segment", "old_segment")
            .join(
                updated_profiles.select("customer_id", "segment").withColumnRenamed(
                    "segment", "new_segment"
                ),
                "customer_id",
                "inner",
            )
            .filter(col("old_segment") != col("new_segment"))
        )

        print(f"\nCustomers with segment changes: {segment_changes.count()}")
        if segment_changes.count() > 0:
            segment_changes.show(10, truncate=False)

        logger.info("Streaming RFM example completed successfully")

    finally:
        spark.stop()


def run_business_scenario_analysis():
    """Run business scenario analysis with RFM segments."""
    logger.info("Starting business scenario analysis")

    spark = (
        SparkSession.builder.appName("RFM-Business-Analysis")
        .master("local[4]")
        .getOrCreate()
    )

    try:
        # Create scenario-based data
        transactions_df = create_sample_transaction_data(
            spark, num_customers=800, num_transactions=4000
        )

        rfm_engine = RFMSegmentationEngine(
            spark_session=spark, reference_date=datetime.now(), quintiles=True
        )

        customer_profiles = rfm_engine.generate_customer_profiles(
            transactions_df.drop("customer_type")
        )

        # Business Scenario 1: Campaign Budget Allocation
        print("\n" + "=" * 60)
        print("SCENARIO 1: CAMPAIGN BUDGET ALLOCATION")
        print("=" * 60)

        segment_roi = {
            "Champions": 8.5,
            "Loyal Customers": 6.2,
            "Potential Loyalists": 4.8,
            "New Customers": 3.5,
            "At Risk": 7.0,
            "Cannot Lose Them": 9.2,
            "About to Sleep": 4.1,
            "Need Attention": 3.8,
            "Hibernating": 2.1,
            "Lost": 1.2,
            "Promising": 3.2,
        }

        segment_summary = rfm_engine.get_segment_summary(customer_profiles)

        # Add ROI data
        roi_data = [(segment, roi) for segment, roi in segment_roi.items()]
        roi_schema = StructType(
            [
                StructField("segment", StringType(), True),
                StructField("expected_roi", DoubleType(), True),
            ]
        )
        roi_df = spark.createDataFrame(roi_data, roi_schema)

        budget_analysis = (
            segment_summary.join(roi_df, "segment", "left")
            .withColumn(
                "expected_roi",
                when(col("expected_roi").isNull(), 2.0).otherwise(col("expected_roi")),
            )
            .withColumn(
                "budget_priority", col("revenue_percentage") * col("expected_roi")
            )
            .orderBy("budget_priority", ascending=False)
        )

        print("Campaign Budget Allocation Priority (Revenue % × Expected ROI):")
        budget_analysis.select(
            "segment",
            "customer_count",
            "revenue_percentage",
            "expected_roi",
            "budget_priority",
        ).show(truncate=False)

        # Business Scenario 2: Churn Risk Assessment
        print("\n" + "=" * 60)
        print("SCENARIO 2: CHURN RISK ASSESSMENT")
        print("=" * 60)

        at_risk_segments = [
            "At Risk",
            "Cannot Lose Them",
            "About to Sleep",
            "Hibernating",
        ]
        churn_risk_customers = customer_profiles.filter(
            col("segment").isin(at_risk_segments)
        )

        churn_analysis = (
            churn_risk_customers.groupBy("segment")
            .agg({"customer_id": "count", "monetary": "sum", "recency_days": "avg"})
            .withColumnRenamed("count(customer_id)", "at_risk_count")
            .withColumnRenamed("sum(monetary)", "revenue_at_risk")
            .withColumnRenamed("avg(recency_days)", "avg_days_since_purchase")
            .orderBy("revenue_at_risk", ascending=False)
        )

        print("Churn Risk Analysis:")
        churn_analysis.show(truncate=False)

        total_at_risk_revenue = (
            churn_risk_customers.agg({"monetary": "sum"}).collect()[0][0] or 0
        )
        total_revenue = customer_profiles.agg({"monetary": "sum"}).collect()[0][0]

        print(
            f"Total revenue at risk: ${total_at_risk_revenue:,.2f} ({total_at_risk_revenue/total_revenue*100:.1f}%)"
        )

        # Business Scenario 3: Personalization Strategy
        print("\n" + "=" * 60)
        print("SCENARIO 3: PERSONALIZATION STRATEGY")
        print("=" * 60)

        # Define personalization strategies per segment
        personalization_strategies = [
            (
                "Champions",
                "Premium product recommendations, exclusive previews, VIP support",
            ),
            (
                "Loyal Customers",
                "Cross-sell complementary products, loyalty rewards, referral programs",
            ),
            (
                "Potential Loyalists",
                "Targeted promotions, product education, engagement campaigns",
            ),
            ("New Customers", "Onboarding series, starter kits, welcome offers"),
            (
                "At Risk",
                "Win-back campaigns, special discounts, customer service outreach",
            ),
            (
                "Cannot Lose Them",
                "Executive attention, custom solutions, retention specialists",
            ),
            (
                "About to Sleep",
                "Reminder emails, product updates, gentle re-engagement",
            ),
            (
                "Need Attention",
                "Survey feedback, limited-time offers, product recommendations",
            ),
            ("Hibernating", "Broad reactivation campaigns, new product introductions"),
            ("Lost", "Last-chance offers, market research, minimal investment"),
            ("Promising", "Educational content, free trials, social proof"),
        ]

        strategy_schema = StructType(
            [
                StructField("segment", StringType(), True),
                StructField("personalization_strategy", StringType(), True),
            ]
        )

        strategy_df = spark.createDataFrame(personalization_strategies, strategy_schema)

        personalization_plan = (
            customer_profiles.groupBy("segment")
            .agg({"customer_id": "count"})
            .withColumnRenamed("count(customer_id)", "customer_count")
            .join(strategy_df, "segment", "left")
            .orderBy("customer_count", ascending=False)
        )

        print("Personalization Strategy by Segment:")
        personalization_plan.show(truncate=False)

        logger.info("Business scenario analysis completed successfully")

    finally:
        spark.stop()


def main():
    """Main function to run RFM examples."""
    print("RFM Customer Segmentation Examples")
    print("=" * 50)

    examples = [
        ("1", "Basic RFM Analysis", run_basic_rfm_analysis),
        ("2", "Delta Lake Integration", run_delta_lake_integration_example),
        ("3", "Streaming RFM Updates", run_streaming_rfm_example),
        ("4", "Business Scenario Analysis", run_business_scenario_analysis),
        ("5", "All Examples", None),
    ]

    print("\nAvailable examples:")
    for code, name, _ in examples:
        print(f"  {code}. {name}")

    choice = input("\nSelect example to run (1-5): ").strip()

    if choice == "1":
        run_basic_rfm_analysis()
    elif choice == "2":
        run_delta_lake_integration_example()
    elif choice == "3":
        run_streaming_rfm_example()
    elif choice == "4":
        run_business_scenario_analysis()
    elif choice == "5":
        print("\nRunning all examples...")
        run_basic_rfm_analysis()
        print("\n" + "-" * 80)
        run_delta_lake_integration_example()
        print("\n" + "-" * 80)
        run_streaming_rfm_example()
        print("\n" + "-" * 80)
        run_business_scenario_analysis()
    else:
        print("Invalid choice. Exiting.")
        return

    print("\n" + "=" * 50)
    print("RFM Analysis Examples Completed")
    print("=" * 50)


if __name__ == "__main__":
    main()

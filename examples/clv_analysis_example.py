"""
Customer Lifetime Value (CLV) Analysis Examples

This module demonstrates comprehensive CLV analysis capabilities including:
- Historical CLV calculation and analysis
- Predictive CLV modeling with multiple algorithms
- Cohort analysis for customer behavior tracking
- Integration with RFM customer segmentation
- Business intelligence and actionable insights
- Real-world e-commerce scenarios and use cases

Run examples:
    python examples/clv_analysis_example.py --example basic
    python examples/clv_analysis_example.py --example advanced
    python examples/clv_analysis_example.py --example business_scenarios
    python examples/clv_analysis_example.py --example all
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc, lit
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from src.analytics.clv_model import CLVModelEngine, CLVModelType, CohortPeriod


def setup_spark_session(app_name: str = "CLV-Examples") -> SparkSession:
    """Initialize Spark session for examples."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def generate_sample_ecommerce_data(
    spark: SparkSession, scenario: str = "realistic"
) -> tuple:
    """
    Generate comprehensive sample e-commerce data for CLV analysis.

    Args:
        spark: Spark session
        scenario: Data scenario type (realistic, diverse, seasonal)

    Returns:
        Tuple of (transactions_df, customers_df, rfm_segments_df)
    """
    import random

    transactions_data = []
    customers_data = []
    rfm_segments_data = []

    # Define customer archetypes for realistic scenarios
    customer_archetypes = {
        "high_value_loyal": {
            "count": 50,
            "avg_order_value": (800, 1500),
            "frequency_range": (8, 15),
            "seasonality": 1.2,  # 20% holiday boost
        },
        "medium_value_regular": {
            "count": 200,
            "avg_order_value": (150, 400),
            "frequency_range": (4, 8),
            "seasonality": 1.1,
        },
        "low_value_frequent": {
            "count": 300,
            "avg_order_value": (25, 100),
            "frequency_range": (6, 12),
            "seasonality": 1.0,
        },
        "high_value_infrequent": {
            "count": 80,
            "avg_order_value": (1000, 3000),
            "frequency_range": (1, 3),
            "seasonality": 1.3,
        },
        "churned_customers": {
            "count": 100,
            "avg_order_value": (100, 300),
            "frequency_range": (2, 5),
            "seasonality": 1.0,
        },
        "new_customers": {
            "count": 150,
            "avg_order_value": (200, 500),
            "frequency_range": (2, 4),
            "seasonality": 1.1,
        },
    }

    base_date = datetime(2022, 1, 1)
    current_date = datetime(2023, 12, 31)

    customer_id = 1

    for archetype, config in customer_archetypes.items():
        for _ in range(config["count"]):
            customer_str = f"customer_{customer_id:05d}"

            # Generate customer acquisition data
            if archetype == "new_customers":
                acquisition_date = base_date + timedelta(days=random.randint(600, 730))
            elif archetype == "churned_customers":
                acquisition_date = base_date + timedelta(days=random.randint(0, 200))
            else:
                acquisition_date = base_date + timedelta(days=random.randint(0, 500))

            # Customer metadata
            acquisition_channels = [
                "Email",
                "Social Media",
                "Referral",
                "Organic",
                "Paid Ads",
                "Direct",
            ]
            tiers = ["Premium", "Standard", "Basic"]

            customers_data.append(
                (
                    customer_str,
                    acquisition_date.date(),
                    random.choice(acquisition_channels),
                    random.choice(tiers),
                    archetype,
                )
            )

            # Generate RFM segment data
            rfm_mapping = {
                "high_value_loyal": (
                    "Champions",
                    "555",
                    "High Value",
                    "Highly Engaged",
                ),
                "medium_value_regular": (
                    "Loyal Customers",
                    "454",
                    "Medium Value",
                    "Engaged",
                ),
                "low_value_frequent": (
                    "Need Attention",
                    "255",
                    "Low Value",
                    "Frequent",
                ),
                "high_value_infrequent": (
                    "Cannot Lose Them",
                    "155",
                    "High Value",
                    "At Risk",
                ),
                "churned_customers": ("Lost", "122", "Low Value", "Lost"),
                "new_customers": ("New Customers", "511", "Medium Value", "New"),
            }

            segment_info = rfm_mapping[archetype]
            rfm_segments_data.append(
                (
                    customer_str,
                    segment_info[0],  # segment
                    segment_info[1],  # rfm_score
                    segment_info[2],  # customer_value_tier
                    segment_info[3],  # engagement_level
                )
            )

            # Generate transactions for this customer
            start_date = acquisition_date

            if archetype == "churned_customers":
                # Churned customers stop purchasing after initial period
                end_date = min(
                    start_date + timedelta(days=random.randint(90, 300)),
                    datetime(2023, 6, 1),
                )
            elif archetype == "new_customers":
                # New customers have recent activity only
                end_date = current_date
            else:
                # Regular customers have activity throughout the period
                end_date = current_date

            num_transactions = random.randint(*config["frequency_range"])
            transaction_date = start_date

            for transaction_num in range(num_transactions):
                if transaction_date > end_date:
                    break

                # Calculate time gap between purchases based on archetype
                if archetype == "high_value_infrequent":
                    days_gap = random.randint(60, 120)
                elif archetype == "low_value_frequent":
                    days_gap = random.randint(15, 45)
                elif archetype == "churned_customers":
                    days_gap = random.randint(30, 60)
                else:
                    days_gap = random.randint(20, 80)

                transaction_date += timedelta(days=days_gap)

                if transaction_date > end_date:
                    break

                # Calculate order amount with seasonality
                base_amount = random.uniform(*config["avg_order_value"])

                # Apply seasonality (holiday boost in Nov/Dec)
                if transaction_date.month in [11, 12]:
                    seasonal_factor = config["seasonality"]
                else:
                    seasonal_factor = 1.0

                # Add some randomness
                variance_factor = random.uniform(0.8, 1.3)
                final_amount = round(base_amount * seasonal_factor * variance_factor, 2)

                # Product categories and channels
                categories = [
                    "Electronics",
                    "Clothing",
                    "Books",
                    "Home",
                    "Sports",
                    "Beauty",
                    "Toys",
                ]
                channels = ["Online", "Store", "Mobile App"]

                transactions_data.append(
                    (
                        customer_str,
                        transaction_date.date(),
                        final_amount,
                        random.choice(categories),
                        random.choice(channels),
                        archetype,  # For analysis purposes
                    )
                )

            customer_id += 1

    # Create DataFrames
    transactions_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", DateType(), True),
            StructField("amount", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("customer_archetype", StringType(), True),
        ]
    )

    customers_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("acquisition_date", DateType(), True),
            StructField("acquisition_channel", StringType(), True),
            StructField("tier", StringType(), True),
            StructField("archetype", StringType(), True),
        ]
    )

    rfm_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("rfm_score", StringType(), True),
            StructField("customer_value_tier", StringType(), True),
            StructField("engagement_level", StringType(), True),
        ]
    )

    transactions_df = spark.createDataFrame(transactions_data, transactions_schema)
    customers_df = spark.createDataFrame(customers_data, customers_schema)
    rfm_segments_df = spark.createDataFrame(rfm_segments_data, rfm_schema)

    return transactions_df, customers_df, rfm_segments_df


def basic_clv_example(spark: SparkSession):
    """Basic CLV analysis example."""
    print("🔍 Basic CLV Analysis Example")
    print("=" * 50)

    # Generate sample data
    transactions_df, customers_df, _ = generate_sample_ecommerce_data(spark)

    print(f"📊 Dataset Overview:")
    print(f"Total Transactions: {transactions_df.count():,}")
    print(
        f"Unique Customers: {transactions_df.select('customer_id').distinct().count():,}"
    )
    print(
        f"Date Range: {transactions_df.agg({'transaction_date': 'min'}).collect()[0][0]} to {transactions_df.agg({'transaction_date': 'max'}).collect()[0][0]}"
    )

    # Initialize CLV engine
    clv_engine = CLVModelEngine(
        spark_session=spark, prediction_horizon_months=12, profit_margin=0.25
    )

    # Step 1: Calculate Historical CLV
    print("\n📈 Step 1: Historical CLV Calculation")
    historical_clv = clv_engine.calculate_historical_clv(transactions_df)

    print("Historical CLV Summary:")
    summary_stats = clv_engine.get_clv_summary_statistics(historical_clv)
    summary_data = summary_stats.collect()[0]

    print(f"Total Customers: {summary_data.total_customers:,}")
    print(f"Average Historical CLV: ${summary_data.avg_historical_clv:.2f}")
    print(f"Total Business Value: ${summary_data.total_historical_value:,.2f}")

    # Top customers by historical CLV
    print("\n🏆 Top 10 Customers by Historical CLV:")
    top_customers = historical_clv.orderBy(desc("historical_clv")).limit(10)
    top_customers.select(
        "customer_id",
        "historical_clv",
        "total_revenue",
        "total_orders",
        "avg_order_value",
        "customer_lifespan_days",
    ).show(truncate=False)

    # Step 2: Predictive CLV Analysis
    print("\n🔮 Step 2: Predictive CLV Analysis")

    # Prepare features and train model
    features_df = clv_engine.prepare_features_for_prediction(historical_clv)
    model_results = clv_engine.train_clv_prediction_model(features_df)

    print(f"Model Performance:")
    print(f"R² Score: {model_results['r2']:.3f}")
    print(f"RMSE: ${model_results['rmse']:.2f}")
    print(f"MAE: ${model_results['mae']:.2f}")

    # Generate predictions
    predictions_df = clv_engine.predict_clv(features_df)

    print("\n🎯 CLV Predictions vs Historical:")
    comparison = (
        predictions_df.select(
            "customer_id",
            "historical_clv",
            "predicted_clv",
            "clv_confidence",
            (
                (col("predicted_clv") - col("historical_clv"))
                / col("historical_clv")
                * 100
            ).alias("growth_percentage"),
        )
        .orderBy(desc("predicted_clv"))
        .limit(10)
    )

    comparison.show(truncate=False)

    print("✅ Basic CLV analysis completed!")
    return transactions_df, customers_df, predictions_df


def advanced_clv_example(spark: SparkSession):
    """Advanced CLV analysis with cohort analysis and model comparison."""
    print("\n🎯 Advanced CLV Analysis Example")
    print("=" * 50)

    # Generate sample data
    transactions_df, customers_df, rfm_segments_df = generate_sample_ecommerce_data(
        spark
    )

    # Step 1: Model Comparison
    print("🤖 Step 1: Model Type Comparison")

    model_types = [
        CLVModelType.LINEAR_REGRESSION,
        CLVModelType.RANDOM_FOREST,
        CLVModelType.GRADIENT_BOOSTING,
    ]

    model_performance = []

    for model_type in model_types:
        print(f"\nTraining {model_type.value}...")

        clv_engine = CLVModelEngine(
            spark_session=spark, model_type=model_type, prediction_horizon_months=12
        )

        historical_clv = clv_engine.calculate_historical_clv(transactions_df)
        features_df = clv_engine.prepare_features_for_prediction(historical_clv)
        model_results = clv_engine.train_clv_prediction_model(features_df)

        model_performance.append(
            {
                "model_type": model_type.value,
                "r2": model_results["r2"],
                "rmse": model_results["rmse"],
                "mae": model_results["mae"],
            }
        )

        print(
            f"R² Score: {model_results['r2']:.3f}, RMSE: ${model_results['rmse']:.2f}"
        )

    # Display model comparison
    print("\n📊 Model Performance Comparison:")
    print("Model Type".ljust(20) + "R² Score".ljust(10) + "RMSE".ljust(10) + "MAE")
    print("-" * 50)
    for perf in model_performance:
        print(
            f"{perf['model_type']:<20} {perf['r2']:<10.3f} ${perf['rmse']:<9.2f} ${perf['mae']:.2f}"
        )

    # Use best performing model for further analysis
    best_model = max(model_performance, key=lambda x: x["r2"])
    print(f"\n🥇 Best performing model: {best_model['model_type']}")

    # Step 2: Comprehensive CLV Analysis with RFM Integration
    print("\n🎯 Step 2: Integrated CLV & RFM Analysis")

    clv_engine = CLVModelEngine(
        spark_session=spark,
        model_type=CLVModelType.RANDOM_FOREST,  # Use consistently good performer
        prediction_horizon_months=12,
    )

    # Generate comprehensive insights
    comprehensive_insights = clv_engine.generate_clv_insights(
        transactions_df, rfm_segments_df=rfm_segments_df
    )

    # Analyze CLV by customer segments
    print("\n📈 CLV Analysis by Customer Segment:")
    segment_analysis = (
        comprehensive_insights.groupBy("segment")
        .agg(
            count("*").alias("customer_count"),
            avg("predicted_clv").alias("avg_predicted_clv"),
            avg("historical_clv").alias("avg_historical_clv"),
            spark_sum("predicted_clv").alias("total_predicted_value"),
            avg("clv_confidence").alias("avg_confidence"),
        )
        .orderBy(desc("avg_predicted_clv"))
    )

    segment_analysis.show(truncate=False)

    # Identify segment misalignments
    print("\n⚠️ Segment-CLV Misalignments:")
    misalignments = (
        comprehensive_insights.filter(col("clv_segment_alignment").contains("Mismatch"))
        .select(
            "customer_id",
            "segment",
            "predicted_clv",
            "clv_segment_alignment",
            "retention_priority",
        )
        .orderBy(desc("predicted_clv"))
    )

    if misalignments.count() > 0:
        misalignments.show(truncate=False)
    else:
        print("No significant segment misalignments detected.")

    # Step 3: Cohort Analysis
    print("\n👥 Step 3: Cohort Analysis")

    # Monthly cohort analysis
    monthly_cohorts = clv_engine.calculate_cohort_analysis(
        transactions_df, cohort_period=CohortPeriod.MONTHLY
    )

    print("Monthly Cohort Performance:")
    monthly_cohorts.orderBy("cohort_period").show()

    # Quarterly cohort analysis
    quarterly_cohorts = clv_engine.calculate_cohort_analysis(
        transactions_df, cohort_period=CohortPeriod.QUARTERLY
    )

    print("\nQuarterly Cohort Performance:")
    quarterly_cohorts.orderBy("cohort_period").show()

    # Step 4: Business Recommendations Analysis
    print("\n💡 Step 4: Business Recommendations Analysis")

    recommendation_summary = (
        comprehensive_insights.groupBy("recommended_action")
        .agg(
            count("*").alias("customer_count"),
            avg("predicted_clv").alias("avg_clv"),
            spark_sum("predicted_clv").alias("total_value"),
        )
        .orderBy(desc("total_value"))
    )

    print("Recommended Actions by Customer Value:")
    recommendation_summary.show(truncate=False)

    print("✅ Advanced CLV analysis completed!")
    return comprehensive_insights


def business_scenarios_example(spark: SparkSession):
    """Business scenario-focused CLV analysis."""
    print("\n💼 Business Scenarios CLV Analysis")
    print("=" * 50)

    # Generate sample data
    transactions_df, customers_df, rfm_segments_df = generate_sample_ecommerce_data(
        spark
    )

    # Initialize CLV engine
    clv_engine = CLVModelEngine(
        spark_session=spark,
        model_type=CLVModelType.RANDOM_FOREST,
        prediction_horizon_months=12,
        profit_margin=0.25,
    )

    # Generate comprehensive insights
    clv_insights = clv_engine.generate_clv_insights(
        transactions_df, rfm_segments_df=rfm_segments_df
    )

    # Scenario 1: Customer Acquisition Optimization
    print("🎯 Scenario 1: Customer Acquisition Optimization")
    print("-" * 50)

    # Analyze CLV by acquisition channel
    acquisition_clv = (
        clv_insights.join(customers_df, "customer_id")
        .groupBy("acquisition_channel")
        .agg(
            count("*").alias("customers_acquired"),
            avg("predicted_clv").alias("avg_clv"),
            spark_sum("predicted_clv").alias("total_clv_potential"),
            avg("clv_confidence").alias("avg_confidence"),
        )
        .orderBy(desc("avg_clv"))
    )

    print("CLV by Acquisition Channel:")
    acquisition_clv.show(truncate=False)

    # Calculate acquisition efficiency (CLV per customer)
    print("\n💡 Acquisition Strategy Recommendations:")
    top_channels = acquisition_clv.orderBy(desc("avg_clv")).limit(3).collect()

    for i, channel in enumerate(top_channels, 1):
        print(
            f"{i}. {channel.acquisition_channel}: Avg CLV ${channel.avg_clv:.2f}, "
            f"{channel.customers_acquired} customers, Total Value ${channel.total_clv_potential:,.2f}"
        )

    # Scenario 2: Customer Retention Prioritization
    print("\n🔒 Scenario 2: Customer Retention Prioritization")
    print("-" * 50)

    # Identify high-value at-risk customers
    retention_priorities = clv_insights.filter(
        (col("predicted_clv") > 500)
        & (col("recency_days") > 90)  # High value  # Haven't purchased recently
    ).orderBy(desc("predicted_clv"))

    print(f"High-Value At-Risk Customers: {retention_priorities.count()}")

    if retention_priorities.count() > 0:
        print("\nTop 10 Retention Priorities:")
        retention_priorities.select(
            "customer_id",
            "segment",
            "predicted_clv",
            "recency_days",
            "retention_priority",
            "recommended_action",
        ).limit(10).show(truncate=False)

        # Calculate potential revenue at risk
        total_at_risk = retention_priorities.agg(spark_sum("predicted_clv")).collect()[
            0
        ][0]
        print(f"\n💰 Total Revenue at Risk: ${total_at_risk:,.2f}")

    # Scenario 3: Marketing Budget Allocation
    print("\n📊 Scenario 3: Marketing Budget Allocation")
    print("-" * 50)

    # Create CLV-based customer tiers
    clv_tiers = clv_insights.withColumn(
        "clv_tier",
        when(col("predicted_clv") > 2000, "Platinum")
        .when(col("predicted_clv") > 1000, "Gold")
        .when(col("predicted_clv") > 500, "Silver")
        .otherwise("Bronze"),
    )

    # Analyze tier distribution
    tier_analysis = (
        clv_tiers.groupBy("clv_tier")
        .agg(
            count("*").alias("customer_count"),
            avg("predicted_clv").alias("avg_clv"),
            spark_sum("predicted_clv").alias("total_tier_value"),
            (
                spark_sum("predicted_clv")
                / clv_tiers.agg(spark_sum("predicted_clv")).collect()[0][0]
                * 100
            ).alias("value_percentage"),
        )
        .orderBy(desc("total_tier_value"))
    )

    print("Customer Tier Analysis:")
    tier_analysis.show(truncate=False)

    # Marketing budget recommendations
    print("\n💡 Marketing Budget Allocation Recommendations:")
    total_value = clv_tiers.agg(spark_sum("predicted_clv")).collect()[0][0]

    for tier_data in tier_analysis.collect():
        percentage = tier_data.total_tier_value / total_value * 100
        print(
            f"{tier_data.clv_tier} Tier: {tier_data.customer_count:,} customers "
            f"({percentage:.1f}% of total value) - Recommended budget allocation: {percentage:.1f}%"
        )

    # Scenario 4: Product Strategy Optimization
    print("\n🛍️ Scenario 4: Product Strategy Optimization")
    print("-" * 50)

    # Analyze CLV by product category
    product_clv = (
        transactions_df.join(clv_insights, "customer_id")
        .groupBy("category")
        .agg(
            count("customer_id").alias("customers"),
            avg("predicted_clv").alias("avg_customer_clv"),
            avg("amount").alias("avg_order_value"),
            spark_sum("amount").alias("total_revenue"),
        )
        .orderBy(desc("avg_customer_clv"))
    )

    print("CLV Analysis by Product Category:")
    product_clv.show(truncate=False)

    # Identify high-CLV product categories
    print("\n💡 Product Strategy Recommendations:")
    top_categories = product_clv.orderBy(desc("avg_customer_clv")).limit(3).collect()

    for i, category in enumerate(top_categories, 1):
        print(
            f"{i}. Focus on {category.category}: Customers have avg CLV of ${category.avg_customer_clv:.2f}"
        )

    # Scenario 5: Churn Prevention Strategy
    print("\n⚠️ Scenario 5: Churn Prevention Strategy")
    print("-" * 50)

    # Identify customers with declining value trends
    churn_risk = clv_insights.filter(
        (col("segment").isin(["At Risk", "Cannot Lose Them", "About to Sleep"]))
        | (col("recency_days") > 120)
    ).orderBy(desc("predicted_clv"))

    print(f"Customers at Churn Risk: {churn_risk.count()}")

    if churn_risk.count() > 0:
        churn_analysis = (
            churn_risk.groupBy("segment")
            .agg(
                count("*").alias("at_risk_count"),
                avg("predicted_clv").alias("avg_clv"),
                spark_sum("predicted_clv").alias("total_at_risk_value"),
            )
            .orderBy(desc("total_at_risk_value"))
        )

        print("\nChurn Risk by Segment:")
        churn_analysis.show(truncate=False)

        # Calculate churn impact
        total_churn_risk = churn_risk.agg(spark_sum("predicted_clv")).collect()[0][0]
        print(f"\n💰 Total Value at Churn Risk: ${total_churn_risk:,.2f}")

    print("✅ Business scenarios analysis completed!")
    return clv_insights


def performance_optimization_example(spark: SparkSession):
    """Demonstrate performance optimization techniques."""
    print("\n⚡ Performance Optimization Example")
    print("=" * 50)

    # Generate larger dataset for performance testing
    print("📊 Generating larger dataset for performance testing...")

    # Use diverse scenario for more realistic data
    transactions_df, customers_df, rfm_segments_df = generate_sample_ecommerce_data(
        spark, "diverse"
    )

    # Cache frequently accessed data
    print("💾 Caching frequently accessed data...")
    transactions_df.cache()
    customers_df.cache()
    rfm_segments_df.cache()

    # Initialize CLV engine with optimization settings
    clv_engine = CLVModelEngine(
        spark_session=spark,
        model_type=CLVModelType.RANDOM_FOREST,
        prediction_horizon_months=12,
    )

    # Demonstrate incremental processing
    print("🔄 Incremental CLV Processing Example:")

    # Process customers in batches
    unique_customers = transactions_df.select("customer_id").distinct()
    total_customers = unique_customers.count()
    batch_size = max(100, total_customers // 5)  # Process in 5 batches

    print(f"Processing {total_customers:,} customers in batches of {batch_size}")

    all_clv_results = []

    # Process first batch to train model
    customer_sample = unique_customers.limit(batch_size)
    batch_transactions = transactions_df.join(customer_sample, "customer_id")

    print("Training model on first batch...")
    historical_clv = clv_engine.calculate_historical_clv(batch_transactions)
    features_df = clv_engine.prepare_features_for_prediction(historical_clv)
    model_results = clv_engine.train_clv_prediction_model(features_df)

    print(f"Model trained - R² Score: {model_results['r2']:.3f}")

    # Process remaining customers using trained model
    remaining_customers = unique_customers.subtract(customer_sample)
    remaining_transactions = transactions_df.join(remaining_customers, "customer_id")

    if remaining_transactions.count() > 0:
        print("Processing remaining customers with trained model...")
        remaining_historical = clv_engine.calculate_historical_clv(
            remaining_transactions
        )
        remaining_features = clv_engine.prepare_features_for_prediction(
            remaining_historical
        )
        remaining_predictions = clv_engine.predict_clv(remaining_features)

        print(f"Processed {remaining_predictions.count():,} additional customers")

    # Demonstrate data partitioning optimization
    print("\n🗂️ Data Partitioning Optimization:")

    # Repartition data for optimal processing
    optimized_transactions = transactions_df.repartition("customer_id")
    print(
        f"Optimized partitioning - Partitions: {optimized_transactions.rdd.getNumPartitions()}"
    )

    # Demonstrate memory management
    print("\n🧠 Memory Management:")
    print("Unpersisting cached data to free memory...")
    transactions_df.unpersist()
    customers_df.unpersist()
    rfm_segments_df.unpersist()

    print("✅ Performance optimization examples completed!")


def main():
    """Main function to run CLV examples."""
    parser = argparse.ArgumentParser(
        description="Customer Lifetime Value (CLV) Analysis Examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--example",
        choices=["basic", "advanced", "business_scenarios", "performance", "all"],
        default="basic",
        help="Which example to run (default: basic)",
    )

    args = parser.parse_args()

    # Setup Spark
    spark = setup_spark_session("CLV-Analysis-Examples")

    try:
        print("🚀 Customer Lifetime Value (CLV) Analysis Examples")
        print("=" * 60)

        if args.example == "basic" or args.example == "all":
            basic_clv_example(spark)

        if args.example == "advanced" or args.example == "all":
            advanced_clv_example(spark)

        if args.example == "business_scenarios" or args.example == "all":
            business_scenarios_example(spark)

        if args.example == "performance" or args.example == "all":
            performance_optimization_example(spark)

        print("\n🎉 All examples completed successfully!")

        # Summary recommendations
        print("\n💡 Key Takeaways:")
        print("1. Historical CLV provides baseline customer value understanding")
        print("2. Predictive CLV enables proactive customer management")
        print("3. Cohort analysis reveals customer behavior trends")
        print("4. RFM integration enhances segmentation accuracy")
        print("5. Business scenarios drive actionable insights")
        print("\n📚 Next Steps:")
        print("1. Integrate CLV analysis into your analytics pipeline")
        print("2. Set up automated model retraining")
        print("3. Create business dashboards with CLV insights")
        print("4. Implement retention campaigns based on CLV predictions")

    except Exception as e:
        print(f"❌ Error running examples: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

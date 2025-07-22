"""
Customer Lifetime Value (CLV) CLI Interface

Command-line interface for CLV analysis operations including historical calculations,
predictive modeling, cohort analysis, and integration with customer segmentation.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from src.analytics.clv_model import CLVModelEngine, CLVModelType, CohortPeriod


def setup_spark_session(app_name: str = "CLV-Analysis") -> SparkSession:
    """Initialize Spark session for CLV analysis."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def load_sample_data(spark: SparkSession, data_size: str = "small") -> tuple:
    """
    Generate sample transaction and customer data for CLV analysis.

    Args:
        spark: Active Spark session
        data_size: Size of sample data (small, medium, large)

    Returns:
        Tuple of (transactions_df, customers_df)
    """
    # Define data sizes
    size_configs = {
        "small": {"customers": 1000, "transactions_per_customer": 5},
        "medium": {"customers": 10000, "transactions_per_customer": 8},
        "large": {"customers": 100000, "transactions_per_customer": 12},
    }

    config = size_configs.get(data_size, size_configs["small"])

    # Generate sample transaction data
    import random
    from datetime import datetime, timedelta

    transactions_data = []
    customers_data = []

    base_date = datetime(2022, 1, 1)

    for customer_id in range(1, config["customers"] + 1):
        customer_str = f"customer_{customer_id:05d}"

        # Generate customer data
        acquisition_date = base_date + timedelta(days=random.randint(0, 700))
        customers_data.append(
            (
                customer_str,
                acquisition_date.date(),
                random.choice(
                    ["Email", "Social Media", "Referral", "Organic", "Paid Ads"]
                ),
                random.choice(["Premium", "Standard", "Basic"]),
            )
        )

        # Generate transactions for this customer
        num_transactions = random.randint(1, config["transactions_per_customer"])
        current_date = acquisition_date

        for _ in range(num_transactions):
            # Simulate purchase patterns
            days_gap = random.randint(1, 120)
            current_date += timedelta(days=days_gap)

            # Vary amount based on customer behavior
            base_amount = random.uniform(25, 500)
            amount = round(base_amount * random.uniform(0.8, 1.5), 2)

            transactions_data.append(
                (
                    customer_str,
                    current_date.date(),
                    amount,
                    random.choice(
                        ["Electronics", "Clothing", "Books", "Home", "Sports"]
                    ),
                    random.choice(["Online", "Store"]),
                )
            )

    # Create DataFrames
    transactions_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", DateType(), True),
            StructField("amount", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("channel", StringType(), True),
        ]
    )

    customers_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("acquisition_date", DateType(), True),
            StructField("acquisition_channel", StringType(), True),
            StructField("tier", StringType(), True),
        ]
    )

    transactions_df = spark.createDataFrame(transactions_data, transactions_schema)
    customers_df = spark.createDataFrame(customers_data, customers_schema)

    return transactions_df, customers_df


def run_historical_clv_analysis(args):
    """Run historical CLV analysis."""
    print("🔍 Starting Historical CLV Analysis...")

    # Setup Spark
    spark = setup_spark_session("Historical-CLV-Analysis")

    try:
        # Load data
        if args.input_path:
            print(f"📁 Loading data from: {args.input_path}")
            transactions_df = spark.read.parquet(args.input_path)
        else:
            print(f"🎲 Generating sample data (size: {args.data_size})")
            transactions_df, _ = load_sample_data(spark, args.data_size)

        print(f"📊 Processing {transactions_df.count():,} transactions...")

        # Initialize CLV engine
        clv_engine = CLVModelEngine(
            spark_session=spark, profit_margin=args.profit_margin
        )

        # Calculate historical CLV
        historical_clv = clv_engine.calculate_historical_clv(
            transactions_df,
            customer_id_col=args.customer_id_col,
            transaction_date_col=args.transaction_date_col,
            amount_col=args.amount_col,
        )

        # Display results
        print("\n📈 Historical CLV Analysis Results:")
        print("=" * 50)

        # Summary statistics
        summary = clv_engine.get_clv_summary_statistics(historical_clv)
        summary_data = summary.collect()[0]

        print(f"Total Customers: {summary_data['total_customers']:,}")
        print(f"Average Historical CLV: ${summary_data['avg_historical_clv']:.2f}")
        print(f"Total Historical Value: ${summary_data['total_historical_value']:,.2f}")
        print(f"Max CLV: ${summary_data['max_predicted_clv']:.2f}")
        print(f"Min CLV: ${summary_data['min_predicted_clv']:.2f}")

        # Top customers
        print("\n🏆 Top 10 Customers by Historical CLV:")
        top_customers = historical_clv.orderBy(
            historical_clv.historical_clv.desc()
        ).limit(10)
        top_customers.select(
            "customer_id",
            "historical_clv",
            "total_revenue",
            "total_orders",
            "avg_order_value",
        ).show(truncate=False)

        # Save results if specified
        if args.output_path:
            print(f"💾 Saving results to: {args.output_path}")
            historical_clv.write.mode("overwrite").parquet(args.output_path)

        print("✅ Historical CLV analysis completed successfully!")

    except Exception as e:
        print(f"❌ Error in historical CLV analysis: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


def run_predictive_clv_analysis(args):
    """Run predictive CLV analysis."""
    print("🔮 Starting Predictive CLV Analysis...")

    # Setup Spark
    spark = setup_spark_session("Predictive-CLV-Analysis")

    try:
        # Load data
        if args.input_path:
            print(f"📁 Loading data from: {args.input_path}")
            transactions_df = spark.read.parquet(args.input_path)
        else:
            print(f"🎲 Generating sample data (size: {args.data_size})")
            transactions_df, _ = load_sample_data(spark, args.data_size)

        print(f"📊 Processing {transactions_df.count():,} transactions...")

        # Initialize CLV engine
        model_type = CLVModelType(args.model_type)
        clv_engine = CLVModelEngine(
            spark_session=spark,
            prediction_horizon_months=args.prediction_horizon,
            profit_margin=args.profit_margin,
            model_type=model_type,
        )

        # Generate comprehensive CLV insights
        clv_insights = clv_engine.generate_clv_insights(
            transactions_df,
            customer_id_col=args.customer_id_col,
            transaction_date_col=args.transaction_date_col,
            amount_col=args.amount_col,
        )

        # Display results
        print("\n🔮 Predictive CLV Analysis Results:")
        print("=" * 50)

        # Model performance (if training was done)
        if clv_engine.trained_model:
            print("🤖 Model Performance:")
            # Note: In real implementation, we'd store and display model metrics
            print(f"Model Type: {model_type.value}")
            print(f"Prediction Horizon: {args.prediction_horizon} months")

        # Summary statistics
        summary = clv_engine.get_clv_summary_statistics(clv_insights)
        summary_data = summary.collect()[0]

        print(f"\n📊 CLV Summary:")
        print(f"Total Customers: {summary_data['total_customers']:,}")
        print(f"Average Predicted CLV: ${summary_data['avg_predicted_clv']:.2f}")
        print(f"Total Predicted Value: ${summary_data['total_predicted_value']:,.2f}")

        # CLV distribution
        print("\n📈 CLV Distribution:")
        clv_insights.groupBy(
            (clv_insights.predicted_clv / 500).cast("int") * 500
        ).count().orderBy("(CAST((predicted_clv / 500) AS INT) * 500)").show()

        # Top predicted value customers
        print("\n🎯 Top 10 Customers by Predicted CLV:")
        top_predicted = clv_insights.orderBy(clv_insights.predicted_clv.desc()).limit(
            10
        )
        top_predicted.select(
            "customer_id",
            "predicted_clv",
            "clv_confidence",
            "historical_clv",
            "recommended_action",
        ).show(truncate=False)

        # Save results if specified
        if args.output_path:
            print(f"💾 Saving results to: {args.output_path}")
            clv_insights.write.mode("overwrite").parquet(args.output_path)

        print("✅ Predictive CLV analysis completed successfully!")

    except Exception as e:
        print(f"❌ Error in predictive CLV analysis: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


def run_cohort_analysis(args):
    """Run cohort analysis."""
    print("👥 Starting Cohort Analysis...")

    # Setup Spark
    spark = setup_spark_session("Cohort-Analysis")

    try:
        # Load data
        if args.input_path:
            print(f"📁 Loading data from: {args.input_path}")
            transactions_df = spark.read.parquet(args.input_path)
        else:
            print(f"🎲 Generating sample data (size: {args.data_size})")
            transactions_df, _ = load_sample_data(spark, args.data_size)

        print(f"📊 Processing {transactions_df.count():,} transactions...")

        # Initialize CLV engine
        clv_engine = CLVModelEngine(spark_session=spark)

        # Run cohort analysis
        cohort_period = CohortPeriod(args.cohort_period)
        cohort_results = clv_engine.calculate_cohort_analysis(
            transactions_df,
            cohort_period=cohort_period,
            customer_id_col=args.customer_id_col,
            transaction_date_col=args.transaction_date_col,
            amount_col=args.amount_col,
        )

        # Display results
        print(f"\n👥 Cohort Analysis Results ({cohort_period.value}):")
        print("=" * 60)

        cohort_results.show(truncate=False)

        # Save results if specified
        if args.output_path:
            print(f"💾 Saving results to: {args.output_path}")
            cohort_results.write.mode("overwrite").parquet(args.output_path)

        print("✅ Cohort analysis completed successfully!")

    except Exception as e:
        print(f"❌ Error in cohort analysis: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


def run_integrated_analysis(args):
    """Run complete integrated CLV analysis."""
    print("🎯 Starting Integrated CLV Analysis...")

    # Setup Spark
    spark = setup_spark_session("Integrated-CLV-Analysis")

    try:
        # Load transaction data
        if args.input_path:
            print(f"📁 Loading transaction data from: {args.input_path}")
            transactions_df = spark.read.parquet(args.input_path)
        else:
            print(f"🎲 Generating sample data (size: {args.data_size})")
            transactions_df, _ = load_sample_data(spark, args.data_size)

        # Load RFM segments if available
        rfm_segments_df = None
        if args.rfm_segments_path:
            print(f"📁 Loading RFM segments from: {args.rfm_segments_path}")
            rfm_segments_df = spark.read.parquet(args.rfm_segments_path)

        print(f"📊 Processing {transactions_df.count():,} transactions...")

        # Initialize CLV engine
        model_type = CLVModelType(args.model_type)
        clv_engine = CLVModelEngine(
            spark_session=spark,
            prediction_horizon_months=args.prediction_horizon,
            profit_margin=args.profit_margin,
            model_type=model_type,
        )

        # Generate comprehensive insights
        clv_insights = clv_engine.generate_clv_insights(
            transactions_df,
            rfm_segments_df=rfm_segments_df,
            customer_id_col=args.customer_id_col,
            transaction_date_col=args.transaction_date_col,
            amount_col=args.amount_col,
        )

        # Run cohort analysis
        cohort_period = CohortPeriod(args.cohort_period)
        cohort_results = clv_engine.calculate_cohort_analysis(
            transactions_df,
            cohort_period=cohort_period,
            customer_id_col=args.customer_id_col,
            transaction_date_col=args.transaction_date_col,
            amount_col=args.amount_col,
        )

        # Display comprehensive results
        print("\n🎯 Integrated CLV Analysis Results:")
        print("=" * 60)

        # Overall summary
        summary = clv_engine.get_clv_summary_statistics(clv_insights)
        summary_data = summary.collect()[0]

        print(f"📊 Overall Summary:")
        print(f"Total Customers: {summary_data['total_customers']:,}")
        print(f"Average Historical CLV: ${summary_data['avg_historical_clv']:.2f}")
        print(f"Average Predicted CLV: ${summary_data['avg_predicted_clv']:.2f}")
        print(f"Total Business Value: ${summary_data['total_predicted_value']:,.2f}")

        # Segment breakdown (if RFM integration available)
        if rfm_segments_df:
            print(f"\n🎯 CLV by Customer Segment:")
            segment_summary = (
                clv_insights.groupBy("segment")
                .agg({"predicted_clv": "avg", "predicted_clv": "sum", "*": "count"})
                .orderBy("avg(predicted_clv)", ascending=False)
            )
            segment_summary.show()

        # Cohort insights
        print(f"\n👥 Key Cohort Insights:")
        cohort_results.orderBy("cohort_period").show(10)

        # Business recommendations
        print(f"\n💡 Top Business Recommendations:")
        recommendations = (
            clv_insights.groupBy("recommended_action")
            .count()
            .orderBy("count", ascending=False)
        )
        recommendations.show(truncate=False)

        # Save comprehensive results
        if args.output_path:
            print(f"💾 Saving comprehensive results to: {args.output_path}")

            # Save main insights
            clv_insights.write.mode("overwrite").parquet(
                f"{args.output_path}/clv_insights"
            )

            # Save cohort analysis
            cohort_results.write.mode("overwrite").parquet(
                f"{args.output_path}/cohort_analysis"
            )

            # Save summary report
            summary.write.mode("overwrite").parquet(f"{args.output_path}/summary_stats")

        print("✅ Integrated CLV analysis completed successfully!")

    except Exception as e:
        print(f"❌ Error in integrated CLV analysis: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(
        description="Customer Lifetime Value (CLV) Analysis CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run historical CLV analysis with sample data
  python clv_cli.py historical --data-size medium --profit-margin 0.25

  # Run predictive CLV analysis with custom data
  python clv_cli.py predictive --input-path /path/to/transactions.parquet --model-type random_forest

  # Run cohort analysis
  python clv_cli.py cohort --data-size large --cohort-period monthly

  # Run complete integrated analysis
  python clv_cli.py integrated --data-size medium --rfm-segments-path /path/to/rfm_segments.parquet
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="CLV analysis commands")

    # Common arguments
    def add_common_args(sub_parser):
        sub_parser.add_argument(
            "--input-path", type=str, help="Input transactions data path"
        )
        sub_parser.add_argument("--output-path", type=str, help="Output results path")
        sub_parser.add_argument(
            "--data-size",
            choices=["small", "medium", "large"],
            default="small",
            help="Sample data size",
        )
        sub_parser.add_argument(
            "--customer-id-col", default="customer_id", help="Customer ID column name"
        )
        sub_parser.add_argument(
            "--transaction-date-col",
            default="transaction_date",
            help="Transaction date column name",
        )
        sub_parser.add_argument(
            "--amount-col", default="amount", help="Transaction amount column name"
        )
        sub_parser.add_argument(
            "--profit-margin",
            type=float,
            default=0.2,
            help="Profit margin for CLV calculations",
        )

    # Historical CLV analysis
    historical_parser = subparsers.add_parser(
        "historical", help="Run historical CLV analysis"
    )
    add_common_args(historical_parser)

    # Predictive CLV analysis
    predictive_parser = subparsers.add_parser(
        "predictive", help="Run predictive CLV analysis"
    )
    add_common_args(predictive_parser)
    predictive_parser.add_argument(
        "--model-type",
        choices=["linear_regression", "random_forest", "gradient_boosting"],
        default="random_forest",
        help="ML model type",
    )
    predictive_parser.add_argument(
        "--prediction-horizon",
        type=int,
        default=12,
        help="Prediction horizon in months",
    )

    # Cohort analysis
    cohort_parser = subparsers.add_parser("cohort", help="Run cohort analysis")
    add_common_args(cohort_parser)
    cohort_parser.add_argument(
        "--cohort-period",
        choices=["monthly", "quarterly", "yearly"],
        default="monthly",
        help="Cohort grouping period",
    )

    # Integrated analysis
    integrated_parser = subparsers.add_parser(
        "integrated", help="Run complete integrated analysis"
    )
    add_common_args(integrated_parser)
    integrated_parser.add_argument(
        "--model-type",
        choices=["linear_regression", "random_forest", "gradient_boosting"],
        default="random_forest",
        help="ML model type",
    )
    integrated_parser.add_argument(
        "--prediction-horizon",
        type=int,
        default=12,
        help="Prediction horizon in months",
    )
    integrated_parser.add_argument(
        "--cohort-period",
        choices=["monthly", "quarterly", "yearly"],
        default="monthly",
        help="Cohort grouping period",
    )
    integrated_parser.add_argument(
        "--rfm-segments-path",
        type=str,
        help="Path to RFM segments data for integration",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Route to appropriate function
    if args.command == "historical":
        run_historical_clv_analysis(args)
    elif args.command == "predictive":
        run_predictive_clv_analysis(args)
    elif args.command == "cohort":
        run_cohort_analysis(args)
    elif args.command == "integrated":
        run_integrated_analysis(args)


if __name__ == "__main__":
    main()

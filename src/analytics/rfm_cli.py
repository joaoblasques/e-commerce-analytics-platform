"""
RFM Customer Segmentation CLI

Command-line interface for running RFM customer segmentation analysis.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max, min, stddev, sum
from pyspark.sql.window import Window

from ..utils.logger import setup_logging
from .rfm_segmentation import RFMSegmentationEngine

logger = setup_logging(__name__)


class RFMSegmentationCLI:
    """CLI interface for RFM customer segmentation."""

    def __init__(self):
        """Initialize the CLI."""
        self.spark = None
        self.rfm_engine = None

    def setup_spark(self, app_name: str = "RFM-Customer-Segmentation") -> SparkSession:
        """Setup Spark session for RFM analysis."""
        try:
            self.spark = SparkSession.builder.appName(app_name).getOrCreate()
            logger.info(f"Spark session created: {app_name}")
            return self.spark
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            sys.exit(1)

    def load_transaction_data(self, input_path: str, format: str = "parquet"):
        """Load transaction data from specified path."""
        try:
            if format.lower() == "parquet":
                df = self.spark.read.parquet(input_path)
            elif format.lower() == "csv":
                df = (
                    self.spark.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(input_path)
                )
            elif format.lower() == "json":
                df = self.spark.read.json(input_path)
            else:
                raise ValueError(f"Unsupported format: {format}")

            logger.info(f"Loaded {df.count()} transactions from {input_path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load transaction data: {e}")
            sys.exit(1)

    def run_rfm_analysis(
        self,
        input_path: str,
        output_path: str,
        format: str = "parquet",
        customer_id_col: str = "customer_id",
        transaction_date_col: str = "transaction_date",
        amount_col: str = "amount",
        reference_date: Optional[str] = None,
        quintiles: bool = True,
    ):
        """Run complete RFM analysis."""
        logger.info("Starting RFM customer segmentation analysis")

        # Setup Spark
        self.setup_spark()

        # Parse reference date
        ref_date = None
        if reference_date:
            try:
                ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            except ValueError:
                logger.error(
                    f"Invalid reference date format: {reference_date}. Use YYYY-MM-DD"
                )
                sys.exit(1)

        # Initialize RFM engine
        self.rfm_engine = RFMSegmentationEngine(
            spark_session=self.spark, reference_date=ref_date, quintiles=quintiles
        )

        # Load transaction data
        transactions_df = self.load_transaction_data(input_path, format)

        # Validate required columns
        required_cols = [customer_id_col, transaction_date_col, amount_col]
        missing_cols = [
            col for col in required_cols if col not in transactions_df.columns
        ]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            sys.exit(1)

        # Generate customer profiles
        logger.info("Generating customer profiles with RFM analysis")
        customer_profiles = self.rfm_engine.generate_customer_profiles(
            transactions_df,
            customer_id_col=customer_id_col,
            transaction_date_col=transaction_date_col,
            amount_col=amount_col,
        )

        # Add action recommendations
        customer_profiles_with_actions = self.rfm_engine.recommend_actions(
            customer_profiles
        )

        # Save results
        logger.info(f"Saving customer profiles to {output_path}")
        customer_profiles_with_actions.coalesce(1).write.mode("overwrite").parquet(
            f"{output_path}/customer_profiles"
        )

        # Generate and save segment summary
        segment_summary = self.rfm_engine.get_segment_summary(customer_profiles)
        segment_summary.coalesce(1).write.mode("overwrite").parquet(
            f"{output_path}/segment_summary"
        )

        # Identify high-value customers
        high_value_customers = self.rfm_engine.identify_high_value_customers(
            customer_profiles
        )
        high_value_customers.coalesce(1).write.mode("overwrite").parquet(
            f"{output_path}/high_value_customers"
        )

        # Print summary statistics
        self._print_summary_stats(customer_profiles, segment_summary)

        logger.info("RFM analysis completed successfully")

    def _print_summary_stats(self, customer_profiles, segment_summary):
        """Print summary statistics to console."""
        total_customers = customer_profiles.count()
        total_revenue = customer_profiles.agg({"monetary": "sum"}).collect()[0][0]

        print("\n" + "=" * 60)
        print("RFM CUSTOMER SEGMENTATION ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"Total Customers Analyzed: {total_customers:,}")
        print(f"Total Revenue: ${total_revenue:,.2f}")
        print(f"Average Customer Value: ${total_revenue/total_customers:,.2f}")

        print("\nSEGMENT DISTRIBUTION:")
        print("-" * 40)

        segments_data = segment_summary.orderBy(
            "customer_count", ascending=False
        ).collect()
        for row in segments_data:
            print(
                f"{row.segment:<25} {row.customer_count:>6,} customers ({row.revenue_percentage:>5.1f}% revenue)"
            )

        print("\n" + "=" * 60)

    def generate_segment_report(
        self, customer_profiles_path: str, output_path: str, format: str = "csv"
    ):
        """Generate detailed segment analysis report."""
        logger.info("Generating detailed segment report")

        # Setup Spark if not already done
        if not self.spark:
            self.setup_spark("RFM-Report-Generator")

        # Load customer profiles
        customer_profiles = self.spark.read.parquet(customer_profiles_path)

        # Create detailed report
        detailed_report = (
            customer_profiles.groupBy("segment")
            .agg(
                count("*").alias("customer_count"),
                avg("monetary").alias("avg_monetary_value"),
                avg("frequency").alias("avg_frequency"),
                avg("recency_days").alias("avg_recency_days"),
                avg("avg_order_value").alias("avg_order_value"),
                sum("monetary").alias("total_revenue"),
                max("monetary").alias("max_customer_value"),
                min("monetary").alias("min_customer_value"),
                stddev("monetary").alias("monetary_std_dev"),
            )
            .withColumn(
                "revenue_percentage",
                col("total_revenue")
                / sum("total_revenue").over(Window.partitionBy())
                * 100,
            )
        )

        # Save report
        if format.lower() == "csv":
            detailed_report.coalesce(1).write.mode("overwrite").option(
                "header", "true"
            ).csv(output_path)
        else:
            detailed_report.coalesce(1).write.mode("overwrite").parquet(output_path)

        logger.info(f"Detailed segment report saved to {output_path}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="RFM Customer Segmentation Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic RFM analysis
  python -m src.analytics.rfm_cli analyze --input /path/to/transactions.parquet --output /path/to/output

  # RFM analysis with custom date and column names
  python -m src.analytics.rfm_cli analyze \\
    --input /path/to/transactions.csv \\
    --output /path/to/output \\
    --format csv \\
    --customer-id user_id \\
    --transaction-date date \\
    --amount total_amount \\
    --reference-date 2023-12-31

  # Generate detailed report
  python -m src.analytics.rfm_cli report \\
    --input /path/to/customer_profiles \\
    --output /path/to/report.csv
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Analyze command
    analyze_parser = subparsers.add_parser(
        "analyze", help="Run RFM analysis on transaction data"
    )
    analyze_parser.add_argument(
        "--input", "-i", required=True, help="Input transaction data path"
    )
    analyze_parser.add_argument(
        "--output", "-o", required=True, help="Output directory path"
    )
    analyze_parser.add_argument(
        "--format",
        "-f",
        default="parquet",
        choices=["parquet", "csv", "json"],
        help="Input data format (default: parquet)",
    )
    analyze_parser.add_argument(
        "--customer-id", default="customer_id", help="Customer ID column name"
    )
    analyze_parser.add_argument(
        "--transaction-date",
        default="transaction_date",
        help="Transaction date column name",
    )
    analyze_parser.add_argument(
        "--amount", default="amount", help="Transaction amount column name"
    )
    analyze_parser.add_argument(
        "--reference-date", help="Reference date for recency calculation (YYYY-MM-DD)"
    )
    analyze_parser.add_argument(
        "--quartiles",
        action="store_true",
        help="Use quartile scoring instead of quintiles",
    )

    # Report command
    report_parser = subparsers.add_parser(
        "report", help="Generate detailed segment report"
    )
    report_parser.add_argument(
        "--input", "-i", required=True, help="Customer profiles input path"
    )
    report_parser.add_argument(
        "--output", "-o", required=True, help="Report output path"
    )
    report_parser.add_argument(
        "--format",
        "-f",
        default="csv",
        choices=["csv", "parquet"],
        help="Output format (default: csv)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    cli = RFMSegmentationCLI()

    try:
        if args.command == "analyze":
            cli.run_rfm_analysis(
                input_path=args.input,
                output_path=args.output,
                format=args.format,
                customer_id_col=args.customer_id,
                transaction_date_col=args.transaction_date,
                amount_col=args.amount,
                reference_date=args.reference_date,
                quintiles=not args.quartiles,
            )
        elif args.command == "report":
            cli.generate_segment_report(
                customer_profiles_path=args.input,
                output_path=args.output,
                format=args.format,
            )
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        sys.exit(1)
    finally:
        if cli.spark:
            cli.spark.stop()


if __name__ == "__main__":
    main()

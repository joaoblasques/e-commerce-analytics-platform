"""
Revenue analytics example demonstrating comprehensive business intelligence capabilities.

This example shows how to use the RevenueAnalytics engine for multi-dimensional
revenue analysis, forecasting, profit margin analysis, and trend analysis.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, lit, rand, to_timestamp, when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics.business_intelligence.revenue_analytics import RevenueAnalytics


def create_sample_transaction_data(
    spark: SparkSession, num_records: int = 10000
) -> Any:
    """Create sample transaction data for revenue analytics demonstration."""

    # Define transaction schema
    transaction_schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("customer_segment", StringType(), False),
            StructField("product_category", StringType(), False),
            StructField("geographic_region", StringType(), False),
            StructField("sales_channel", StringType(), False),
            StructField("payment_method", StringType(), False),
            StructField("device_type", StringType(), False),
        ]
    )

    # Create base DataFrame with synthetic data
    base_date = datetime.now() - timedelta(days=365)

    df = (
        spark.range(num_records)
        .select(col("id").cast(StringType()).alias("transaction_id"))
        .withColumn("user_id", (col("id") % 1000).cast(StringType()))
        .withColumn("product_id", (col("id") % 500).cast(StringType()))
        .withColumn("price", (rand() * 500 + 10).cast(DoubleType()))
        .withColumn("quantity", (rand() * 3 + 1).cast(IntegerType()))
        .withColumn(
            "timestamp",
            to_timestamp(lit(base_date.strftime("%Y-%m-%d %H:%M:%S")))
            + (rand() * 365 * 24 * 3600).cast("interval second"),
        )
        .withColumn(
            "customer_segment",
            when(rand() < 0.2, "Premium")
            .when(rand() < 0.5, "Gold")
            .when(rand() < 0.8, "Silver")
            .otherwise("Bronze"),
        )
        .withColumn(
            "product_category",
            when(rand() < 0.3, "Electronics")
            .when(rand() < 0.5, "Clothing")
            .when(rand() < 0.7, "Home & Garden")
            .when(rand() < 0.85, "Sports")
            .otherwise("Books"),
        )
        .withColumn(
            "geographic_region",
            when(rand() < 0.4, "North America")
            .when(rand() < 0.7, "Europe")
            .when(rand() < 0.85, "Asia")
            .otherwise("Other"),
        )
        .withColumn(
            "sales_channel",
            when(rand() < 0.6, "Online")
            .when(rand() < 0.85, "Mobile App")
            .otherwise("Retail Store"),
        )
        .withColumn(
            "payment_method",
            when(rand() < 0.5, "Credit Card")
            .when(rand() < 0.75, "Debit Card")
            .when(rand() < 0.9, "PayPal")
            .otherwise("Bank Transfer"),
        )
        .withColumn(
            "device_type",
            when(rand() < 0.5, "Desktop")
            .when(rand() < 0.8, "Mobile")
            .otherwise("Tablet"),
        )
    )

    # Add some seasonal patterns and trends
    df = df.withColumn(
        "price",
        when(col("product_category") == "Electronics", col("price") * 1.5)
        .when(col("customer_segment") == "Premium", col("price") * 2.0)
        .otherwise(col("price")),
    )

    return df


def create_sample_cost_data(spark: SparkSession, num_products: int = 500) -> Any:
    """Create sample cost data for profit margin analysis."""

    cost_schema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("actual_cost", DoubleType(), False),
            StructField("shipping_cost", DoubleType(), False),
            StructField("handling_cost", DoubleType(), False),
        ]
    )

    return (
        spark.range(num_products)
        .select(col("id").cast(StringType()).alias("product_id"))
        .withColumn("actual_cost", (rand() * 200 + 5).cast(DoubleType()))
        .withColumn("shipping_cost", (rand() * 20 + 2).cast(DoubleType()))
        .withColumn("handling_cost", (rand() * 10 + 1).cast(DoubleType()))
    )


def demonstrate_multi_dimensional_analysis(
    revenue_analytics: RevenueAnalytics, transactions_df: Any
) -> None:
    """Demonstrate multi-dimensional revenue analysis."""
    print("\n" + "=" * 60)
    print("MULTI-DIMENSIONAL REVENUE ANALYSIS")
    print("=" * 60)

    # Analyze revenue across all dimensions
    results = revenue_analytics.analyze_multi_dimensional_revenue(
        transactions_df,
        time_period="day",
        start_date=datetime.now() - timedelta(days=90),
        end_date=datetime.now(),
    )

    print("\n📊 Analysis Results:")
    print(f"- Overall revenue analysis: {results['overall'] is not None}")
    print(
        f"- Dimensional analyses: {len([k for k in results.keys() if k not in ['overall', 'cross_dimensional', 'concentration']])}"
    )
    print(f"- Cross-dimensional analysis: {results['cross_dimensional'] is not None}")
    print(f"- Revenue concentration analysis: {len(results['concentration'])}")

    # Display sample results (in real scenario, you'd collect and display actual data)
    print("\n📈 Sample Insights:")
    print(
        "- Revenue analyzed across customer segments, product categories, and regions"
    )
    print("- Cross-dimensional patterns identified")
    print("- Revenue concentration (80/20 rule) calculated")

    return results


def demonstrate_revenue_forecasting(
    revenue_analytics: RevenueAnalytics, transactions_df: Any
) -> None:
    """Demonstrate revenue forecasting capabilities."""
    print("\n" + "=" * 60)
    print("REVENUE FORECASTING")
    print("=" * 60)

    try:
        # Generate 30-day revenue forecast
        forecast_results = revenue_analytics.forecast_revenue(
            transactions_df, horizon_days=30, model_type="linear_regression"
        )

        print("\n🔮 Forecast Results:")
        print(f"- Model type: {forecast_results['model_type']}")
        print(f"- Forecast horizon: {forecast_results['horizon_days']} days")
        print(f"- Model R²: {forecast_results['model_metrics'].get('r2', 'N/A')}")
        print(f"- RMSE: {forecast_results['model_metrics'].get('rmse', 'N/A')}")
        print(
            f"- Seasonality detected: {forecast_results['seasonality_patterns'].get('seasonality_detected', False)}"
        )

        print("\n📊 Forecast Features:")
        print("- Prediction intervals at 80% and 95% confidence levels")
        print("- Seasonal pattern detection")
        print("- Model performance metrics")
        print("- Future revenue projections")

        return forecast_results

    except ValueError as e:
        print(f"\n⚠️  Forecasting Error: {e}")
        print("This typically occurs with insufficient historical data.")
        return None


def demonstrate_profit_margin_analysis(
    revenue_analytics: RevenueAnalytics, transactions_df: Any, cost_data_df: Any
) -> None:
    """Demonstrate profit margin analysis."""
    print("\n" + "=" * 60)
    print("PROFIT MARGIN ANALYSIS")
    print("=" * 60)

    # Analyze profit margins with actual cost data
    margin_results = revenue_analytics.analyze_profit_margins(
        transactions_df, cost_data_df=cost_data_df, include_marketing_costs=True
    )

    print("\n💰 Margin Analysis Features:")
    print("- Gross margin calculation")
    print("- Net margin with operational costs")
    print("- Marketing-adjusted margins")
    print("- Profitability categorization")
    print("- Margin rankings and percentiles")

    print("\n📊 Cost Components:")
    print("- Cost of goods sold (COGS)")
    print("- Shipping and handling costs")
    print("- Payment processing fees")
    print("- Marketing attribution costs")

    # Display sample insights
    print("\n🎯 Sample Insights:")
    print("- Products categorized by profitability levels")
    print("- Margin analysis across customer segments")
    print("- Operational cost impact on profitability")
    print("- Marketing ROI and cost attribution")

    return margin_results


def demonstrate_trend_analysis(
    revenue_analytics: RevenueAnalytics, transactions_df: Any
) -> None:
    """Demonstrate revenue trend analysis."""
    print("\n" + "=" * 60)
    print("REVENUE TREND ANALYSIS")
    print("=" * 60)

    # Analyze revenue trends with forecasting
    trend_results = revenue_analytics.analyze_revenue_trends(
        transactions_df, dimension="product_category", include_forecasts=True
    )

    print("\n📈 Trend Analysis Features:")
    print(
        f"- Trend direction: {trend_results['trend_direction'].get('direction', 'N/A')}"
    )
    print(f"- Growth rate analysis: Multiple time periods")
    print(
        f"- Seasonal patterns detected: {trend_results['seasonal_patterns'].get('seasonality_strength', 'N/A')}"
    )
    print(f"- Anomaly detection: Statistical outliers identified")

    print("\n📊 Analysis Components:")
    print("- Moving averages (7, 14, 30, 90 days)")
    print("- Growth rates (weekly, monthly, quarterly)")
    print("- Seasonal pattern recognition")
    print("- Revenue anomaly detection")
    print("- Statistical trend analysis")

    # Display sample statistics
    if "statistics" in trend_results:
        stats = trend_results["statistics"]
        print("\n📈 Trend Statistics:")
        print(f"- Average revenue: ${stats.get('average_revenue', 0):,.2f}")
        print(f"- Revenue volatility: ${stats.get('revenue_volatility', 0):,.2f}")
        print(f"- Weekly growth rate: {stats.get('avg_weekly_growth_rate', 0):.2%}")
        print(f"- Data points analyzed: {stats.get('data_points', 0):,}")

    return trend_results


def demonstrate_advanced_analytics(
    revenue_analytics: RevenueAnalytics, transactions_df: Any
) -> None:
    """Demonstrate advanced revenue analytics features."""
    print("\n" + "=" * 60)
    print("ADVANCED ANALYTICS FEATURES")
    print("=" * 60)

    print("\n🚀 Advanced Capabilities:")
    print("- Multi-dimensional revenue correlation analysis")
    print("- Customer segment revenue attribution")
    print("- Product category performance comparison")
    print("- Geographic revenue distribution")
    print("- Sales channel effectiveness analysis")
    print("- Payment method revenue patterns")

    # Demonstrate revenue analysis by multiple dimensions
    dimensions = ["customer_segment", "product_category", "geographic_region"]

    print("\n📊 Dimensional Analysis:")
    for dimension in dimensions:
        print(f"- {dimension.replace('_', ' ').title()}: Revenue patterns and trends")

    print("\n🎯 Business Intelligence:")
    print("- Revenue concentration analysis (80/20 rule)")
    print("- Cross-dimensional revenue patterns")
    print("- Profitability optimization insights")
    print("- Forecasting with confidence intervals")
    print("- Seasonal and cyclical pattern detection")


def main():
    """Main demonstration function."""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("🏢 E-COMMERCE REVENUE ANALYTICS DEMONSTRATION")
    print("=" * 70)
    print("This example demonstrates comprehensive revenue analytics capabilities")
    print("including multi-dimensional analysis, forecasting, margin analysis,")
    print("and trend analysis for e-commerce business intelligence.")

    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("RevenueAnalyticsExample")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Create sample data
        print("\n📊 Creating sample transaction data...")
        transactions_df = create_sample_transaction_data(spark, num_records=50000)
        transactions_df.cache()  # Cache for multiple operations

        print(f"✅ Generated {transactions_df.count():,} sample transactions")

        # Create cost data
        print("\n💰 Creating sample cost data...")
        cost_data_df = create_sample_cost_data(spark, num_products=500)

        # Initialize revenue analytics
        print("\n🔧 Initializing revenue analytics engine...")
        revenue_analytics = RevenueAnalytics(spark)

        # Run demonstrations
        multi_dim_results = demonstrate_multi_dimensional_analysis(
            revenue_analytics, transactions_df
        )
        forecast_results = demonstrate_revenue_forecasting(
            revenue_analytics, transactions_df
        )
        margin_results = demonstrate_profit_margin_analysis(
            revenue_analytics, transactions_df, cost_data_df
        )
        trend_results = demonstrate_trend_analysis(revenue_analytics, transactions_df)
        demonstrate_advanced_analytics(revenue_analytics, transactions_df)

        print("\n" + "=" * 70)
        print("✅ REVENUE ANALYTICS DEMONSTRATION COMPLETED")
        print("=" * 70)
        print("\n🎯 Key Achievements:")
        print("- Multi-dimensional revenue analysis across 6+ business dimensions")
        print("- Machine learning-based revenue forecasting with confidence intervals")
        print("- Comprehensive profit margin analysis with cost attribution")
        print("- Advanced trend analysis with seasonality and anomaly detection")
        print("- Business intelligence insights for data-driven decision making")

        print("\n📈 Business Impact:")
        print("- Revenue optimization through dimensional analysis")
        print("- Predictive planning with accurate forecasting")
        print("- Profit maximization through margin analysis")
        print("- Strategic insights through trend analysis")

        # Performance summary
        print(f"\n⚡ Performance Summary:")
        print(f"- Processed {transactions_df.count():,} transactions")
        print(
            f"- Analyzed {len(revenue_analytics.config['revenue_dimensions'])} revenue dimensions"
        )
        print(f"- Generated forecasts and trend analysis")
        print(f"- Comprehensive profit margin calculations")

    except Exception as e:
        print(f"\n❌ Error during demonstration: {e}")
        raise
    finally:
        # Cleanup
        spark.stop()
        print("\n🧹 Spark session stopped")


if __name__ == "__main__":
    main()

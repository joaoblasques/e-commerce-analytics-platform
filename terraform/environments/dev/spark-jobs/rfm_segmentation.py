#!/usr/bin/env python3
"""
RFM Customer Segmentation Spark Job
Performs Recency, Frequency, Monetary analysis for customer segmentation

This job calculates:
1. Recency: Days since last purchase
2. Frequency: Number of transactions
3. Monetary: Total amount spent
4. RFM scores and customer segments
"""

import argparse
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


def create_spark_session(app_name: str) -> SparkSession:
    """Create optimized Spark session for RFM analysis"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def calculate_rfm_metrics(transactions_df, analysis_date, lookback_days):
    """Calculate RFM metrics for each customer"""

    # Convert analysis_date to timestamp
    analysis_timestamp = to_timestamp(lit(analysis_date), "yyyy-MM-dd")
    lookback_timestamp = analysis_timestamp - expr(f"interval {lookback_days} days")

    # Filter transactions within lookback period
    filtered_transactions = transactions_df.filter(
        (col("timestamp") >= lookback_timestamp)
        & (col("timestamp") <= analysis_timestamp)
    )

    # Calculate RFM metrics
    rfm_metrics = filtered_transactions.groupBy("user_id").agg(
        # Recency: Days since last purchase
        datediff(analysis_timestamp, max("timestamp")).alias("recency"),
        # Frequency: Number of unique transactions
        countDistinct("transaction_id").alias("frequency"),
        # Monetary: Total amount spent
        sum("amount").alias("monetary"),
        # Additional metrics
        avg("amount").alias("avg_order_value"),
        min("timestamp").alias("first_purchase"),
        max("timestamp").alias("last_purchase"),
        count("transaction_id").alias("total_transactions"),
    )

    return rfm_metrics


def calculate_rfm_scores(rfm_df):
    """Calculate RFM scores using quintile-based scoring"""

    # Define windows for quintile calculation
    recency_window = Window.orderBy(
        col("recency").asc()
    )  # Lower recency = higher score
    frequency_window = Window.orderBy(
        col("frequency").desc()
    )  # Higher frequency = higher score
    monetary_window = Window.orderBy(
        col("monetary").desc()
    )  # Higher monetary = higher score

    # Calculate quintiles and scores
    rfm_scores = (
        rfm_df.withColumn("recency_score", ntile(5).over(recency_window))
        .withColumn("frequency_score", ntile(5).over(frequency_window))
        .withColumn("monetary_score", ntile(5).over(monetary_window))
    )

    # Calculate combined RFM score
    rfm_scores = rfm_scores.withColumn(
        "rfm_score",
        concat(
            col("recency_score").cast("string"),
            col("frequency_score").cast("string"),
            col("monetary_score").cast("string"),
        ),
    )

    # Calculate overall customer value score (weighted average)
    rfm_scores = rfm_scores.withColumn(
        "customer_value_score",
        round(
            (
                col("recency_score") * 0.3
                + col("frequency_score") * 0.4
                + col("monetary_score") * 0.3
            ),
            2,
        ),
    )

    return rfm_scores


def assign_customer_segments(rfm_scores_df):
    """Assign customer segments based on RFM scores"""

    # Define segmentation logic
    segmentation_conditions = [
        # Champions: High value, recent, frequent customers
        (
            (col("recency_score") >= 4)
            & (col("frequency_score") >= 4)
            & (col("monetary_score") >= 4),
            "Champions",
        ),
        # Loyal Customers: High frequency and monetary, decent recency
        (
            (col("frequency_score") >= 4) & (col("monetary_score") >= 4),
            "Loyal Customers",
        ),
        # Potential Loyalists: Recent customers with good frequency or monetary
        (
            (col("recency_score") >= 4)
            & ((col("frequency_score") >= 3) | (col("monetary_score") >= 3)),
            "Potential Loyalists",
        ),
        # New Customers: Recent but low frequency/monetary
        (
            (col("recency_score") >= 4)
            & (col("frequency_score") <= 2)
            & (col("monetary_score") <= 2),
            "New Customers",
        ),
        # Promising: Recent moderate spenders
        (
            (col("recency_score") >= 3)
            & (col("frequency_score") >= 2)
            & (col("monetary_score") >= 2),
            "Promising",
        ),
        # Need Attention: Above average recency, frequency, and monetary
        (
            (col("recency_score") >= 3)
            & (col("frequency_score") >= 3)
            & (col("monetary_score") >= 3),
            "Need Attention",
        ),
        # About to Sleep: Below average recency, good frequency and monetary
        (
            (col("recency_score") <= 2)
            & (col("frequency_score") >= 3)
            & (col("monetary_score") >= 3),
            "About to Sleep",
        ),
        # At Risk: Good monetary and frequency but haven't purchased recently
        (
            (col("recency_score") <= 2)
            & (col("frequency_score") >= 2)
            & (col("monetary_score") >= 3),
            "At Risk",
        ),
        # Cannot Lose Them: High monetary but low recency and frequency
        (
            (col("recency_score") <= 2)
            & (col("frequency_score") <= 2)
            & (col("monetary_score") >= 4),
            "Cannot Lose Them",
        ),
        # Hibernating: Low frequency and monetary, low recency
        (
            (col("recency_score") <= 2)
            & (col("frequency_score") <= 2)
            & (col("monetary_score") <= 2),
            "Hibernating",
        ),
    ]

    # Apply segmentation logic
    segmented_df = rfm_scores_df
    for condition, segment in segmentation_conditions:
        segmented_df = segmented_df.withColumn(
            "customer_segment",
            when(condition & col("customer_segment").isNull(), segment).otherwise(
                col("customer_segment")
            ),
        )

    # Default segment for any remaining customers
    segmented_df = segmented_df.withColumn(
        "customer_segment",
        when(col("customer_segment").isNull(), "Others").otherwise(
            col("customer_segment")
        ),
    )

    return segmented_df


def calculate_segment_statistics(segmented_df):
    """Calculate statistics for each customer segment"""

    segment_stats = segmented_df.groupBy("customer_segment").agg(
        count("user_id").alias("customer_count"),
        avg("recency").alias("avg_recency"),
        avg("frequency").alias("avg_frequency"),
        avg("monetary").alias("avg_monetary"),
        avg("avg_order_value").alias("avg_order_value"),
        avg("customer_value_score").alias("avg_customer_value_score"),
        sum("monetary").alias("total_revenue"),
        min("monetary").alias("min_monetary"),
        max("monetary").alias("max_monetary"),
        stddev("monetary").alias("stddev_monetary"),
    )

    # Calculate percentage of total customers
    total_customers = segmented_df.count()
    segment_stats = segment_stats.withColumn(
        "customer_percentage", round((col("customer_count") / total_customers) * 100, 2)
    )

    # Calculate revenue percentage
    total_revenue = segmented_df.agg(sum("monetary")).collect()[0][0]
    segment_stats = segment_stats.withColumn(
        "revenue_percentage", round((col("total_revenue") / total_revenue) * 100, 2)
    )

    return segment_stats.orderBy(col("total_revenue").desc())


def generate_segment_insights(segment_stats_df):
    """Generate business insights for each segment"""

    # Collect segment statistics
    segment_data = segment_stats_df.collect()

    insights = []

    for row in segment_data:
        segment = row["customer_segment"]
        customer_count = row["customer_count"]
        customer_percentage = row["customer_percentage"]
        revenue_percentage = row["revenue_percentage"]
        avg_recency = row["avg_recency"]
        avg_frequency = row["avg_frequency"]
        avg_monetary = row["avg_monetary"]
        avg_order_value = row["avg_order_value"]

        # Generate segment-specific insights and recommendations
        if segment == "Champions":
            recommendation = "Reward these customers. Ask for reviews, referrals, or early access to new products."
        elif segment == "Loyal Customers":
            recommendation = (
                "Upsell higher value products. Engage them with loyalty programs."
            )
        elif segment == "Potential Loyalists":
            recommendation = (
                "Nurture with targeted campaigns. Offer membership programs."
            )
        elif segment == "New Customers":
            recommendation = (
                "Provide onboarding support. Create brand awareness campaigns."
            )
        elif segment == "Promising":
            recommendation = (
                "Create targeted offers. Build relationship through education."
            )
        elif segment == "Need Attention":
            recommendation = "Limited time offers. Recommend complementary products."
        elif segment == "About to Sleep":
            recommendation = "Share valuable resources. Recommend popular products."
        elif segment == "At Risk":
            recommendation = "Win-back campaigns. Survey for feedback. Special offers."
        elif segment == "Cannot Lose Them":
            recommendation = (
                "Aggressive win-back campaigns. Personal touch. Renewal discounts."
            )
        elif segment == "Hibernating":
            recommendation = "Ignore or very minimal spend. Focus on other segments."
        else:
            recommendation = "Monitor and reassess segment classification."

        insights.append(
            {
                "customer_segment": segment,
                "customer_count": customer_count,
                "customer_percentage": customer_percentage,
                "revenue_percentage": revenue_percentage,
                "avg_recency_days": avg_recency,
                "avg_frequency": avg_frequency,
                "avg_monetary_value": avg_monetary,
                "avg_order_value": avg_order_value,
                "business_recommendation": recommendation,
                "priority_level": "High"
                if revenue_percentage > 20
                else "Medium"
                if revenue_percentage > 5
                else "Low",
            }
        )

    return insights


def main():
    parser = argparse.ArgumentParser(description="RFM Customer Segmentation Spark Job")
    parser.add_argument(
        "--input-path", required=True, help="Input transactions data path"
    )
    parser.add_argument(
        "--output-path", required=True, help="Output path for RFM segments"
    )
    parser.add_argument("--date", required=True, help="Analysis date (YYYY-MM-DD)")
    parser.add_argument(
        "--lookback-days", type=int, default=365, help="Lookback period in days"
    )

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session(f"RFMSegmentation-{args.date}")

    try:
        print(
            f"Starting RFM analysis for {args.date} with {args.lookback_days} days lookback"
        )

        # Read transactions data
        transactions_df = spark.read.parquet(f"{args.input_path}/transactions/")

        print(f"Loaded {transactions_df.count()} transactions")

        # Calculate RFM metrics
        print("Calculating RFM metrics...")
        rfm_metrics = calculate_rfm_metrics(
            transactions_df, args.date, args.lookback_days
        )

        # Calculate RFM scores
        print("Calculating RFM scores...")
        rfm_scores = calculate_rfm_scores(rfm_metrics)

        # Assign customer segments
        print("Assigning customer segments...")
        segmented_customers = assign_customer_segments(rfm_scores)

        # Add analysis metadata
        segmented_customers = (
            segmented_customers.withColumn("analysis_date", lit(args.date))
            .withColumn("lookback_days", lit(args.lookback_days))
            .withColumn("created_at", current_timestamp())
        )

        # Calculate segment statistics
        print("Calculating segment statistics...")
        segment_stats = calculate_segment_statistics(segmented_customers)

        # Generate insights
        insights = generate_segment_insights(segment_stats)

        # Write segmented customers
        segmented_customers.coalesce(10).write.mode("overwrite").option(
            "compression", "snappy"
        ).parquet(f"{args.output_path}/customer_segments/")

        # Write segment statistics
        segment_stats.coalesce(1).write.mode("overwrite").option(
            "compression", "snappy"
        ).parquet(f"{args.output_path}/segment_statistics/")

        # Write insights as JSON
        insights_df = spark.createDataFrame(insights)
        insights_df.coalesce(1).write.mode("overwrite").json(
            f"{args.output_path}/segment_insights/"
        )

        # Create summary
        total_customers = segmented_customers.count()
        total_segments = (
            segmented_customers.select("customer_segment").distinct().count()
        )

        summary = {
            "analysis_date": args.date,
            "lookback_days": args.lookback_days,
            "total_customers_analyzed": total_customers,
            "total_segments_identified": total_segments,
            "processing_timestamp": datetime.now().isoformat(),
        }

        summary_df = spark.createDataFrame([summary])
        summary_df.coalesce(1).write.mode("overwrite").json(
            f"{args.output_path}/analysis_summary/"
        )

        print(f"✓ RFM analysis completed successfully")
        print(f"✓ Analyzed {total_customers} customers into {total_segments} segments")
        print(f"✓ Results written to {args.output_path}")

        # Display segment distribution
        print("\nSegment Distribution:")
        segment_stats.select(
            "customer_segment",
            "customer_count",
            "customer_percentage",
            "revenue_percentage",
        ).show(truncate=False)

    except Exception as e:
        print(f"Error in RFM analysis: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

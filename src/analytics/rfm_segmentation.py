"""
RFM Customer Segmentation Module

Implements Recency, Frequency, Monetary (RFM) analysis for customer segmentation.
RFM analysis segments customers based on:
- Recency: How recently a customer made a purchase
- Frequency: How often a customer makes purchases
- Monetary: How much money a customer spends
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import asc, col, concat, count, datediff, desc, lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import ntile, percent_rank
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from ..utils.logger import setup_logging

logger = setup_logging(__name__)


class CustomerSegment(Enum):
    """Customer segment classifications based on RFM scores."""

    CHAMPIONS = "Champions"
    LOYAL_CUSTOMERS = "Loyal Customers"
    POTENTIAL_LOYALISTS = "Potential Loyalists"
    NEW_CUSTOMERS = "New Customers"
    PROMISING = "Promising"
    NEED_ATTENTION = "Need Attention"
    ABOUT_TO_SLEEP = "About to Sleep"
    AT_RISK = "At Risk"
    CANNOT_LOSE_THEM = "Cannot Lose Them"
    HIBERNATING = "Hibernating"
    LOST = "Lost"


@dataclass
class RFMScores:
    """Container for individual RFM scores."""

    recency_score: int
    frequency_score: int
    monetary_score: int
    rfm_score: str
    segment: CustomerSegment

    def __post_init__(self):
        """Calculate combined RFM score."""
        if not self.rfm_score:
            self.rfm_score = (
                f"{self.recency_score}{self.frequency_score}{self.monetary_score}"
            )


@dataclass
class CustomerProfile:
    """Enhanced customer profile with RFM analysis."""

    customer_id: str
    first_purchase_date: datetime
    last_purchase_date: datetime
    days_since_last_purchase: int
    total_purchases: int
    total_spent: float
    avg_order_value: float
    rfm_scores: RFMScores
    segment: CustomerSegment
    customer_value_tier: str
    engagement_level: str


class RFMSegmentationEngine:
    """
    RFM (Recency, Frequency, Monetary) Customer Segmentation Engine.

    Analyzes customer transaction data to segment customers based on their
    purchasing behavior and calculate customer lifetime value indicators.
    """

    def __init__(
        self,
        spark_session: Optional[SparkSession] = None,
        reference_date: Optional[datetime] = None,
        quintiles: bool = True,
    ):
        """
        Initialize RFM Segmentation Engine.

        Args:
            spark_session: Spark session for distributed processing
            reference_date: Reference date for recency calculation (default: today)
            quintiles: Whether to use quintile scoring (1-5) or quartile (1-4)
        """
        self.spark = spark_session or SparkSession.getActiveSession()
        if not self.spark:
            raise ValueError("No active Spark session found. Please provide one.")

        self.reference_date = reference_date or datetime.now()
        self.quintiles = quintiles
        self.score_range = 5 if quintiles else 4

        # RFM segment mapping based on scores
        self.segment_map = self._create_segment_mapping()

        logger.info(
            f"RFM Engine initialized with reference date: {self.reference_date}"
        )
        logger.info(f"Using {'quintile' if quintiles else 'quartile'} scoring")

    def _create_segment_mapping(self) -> Dict[str, CustomerSegment]:
        """
        Create mapping from RFM scores to customer segments.

        Based on industry-standard RFM segmentation logic.
        """
        if self.quintiles:
            return {
                # Champions: High R, F, M
                "555": CustomerSegment.CHAMPIONS,
                "554": CustomerSegment.CHAMPIONS,
                "544": CustomerSegment.CHAMPIONS,
                "545": CustomerSegment.CHAMPIONS,
                "454": CustomerSegment.CHAMPIONS,
                "455": CustomerSegment.CHAMPIONS,
                "445": CustomerSegment.CHAMPIONS,
                # Loyal Customers: High F, M but lower R
                "543": CustomerSegment.LOYAL_CUSTOMERS,
                "444": CustomerSegment.LOYAL_CUSTOMERS,
                "435": CustomerSegment.LOYAL_CUSTOMERS,
                "355": CustomerSegment.LOYAL_CUSTOMERS,
                "354": CustomerSegment.LOYAL_CUSTOMERS,
                "345": CustomerSegment.LOYAL_CUSTOMERS,
                "344": CustomerSegment.LOYAL_CUSTOMERS,
                "335": CustomerSegment.LOYAL_CUSTOMERS,
                # Potential Loyalists: Recent customers with good frequency/monetary
                "512": CustomerSegment.POTENTIAL_LOYALISTS,
                "511": CustomerSegment.POTENTIAL_LOYALISTS,
                "422": CustomerSegment.POTENTIAL_LOYALISTS,
                "421": CustomerSegment.POTENTIAL_LOYALISTS,
                "412": CustomerSegment.POTENTIAL_LOYALISTS,
                "411": CustomerSegment.POTENTIAL_LOYALISTS,
                "311": CustomerSegment.POTENTIAL_LOYALISTS,
                # New Customers: High R, low F, varied M
                "551": CustomerSegment.NEW_CUSTOMERS,
                "552": CustomerSegment.NEW_CUSTOMERS,
                "541": CustomerSegment.NEW_CUSTOMERS,
                "542": CustomerSegment.NEW_CUSTOMERS,
                "533": CustomerSegment.NEW_CUSTOMERS,
                "532": CustomerSegment.NEW_CUSTOMERS,
                "531": CustomerSegment.NEW_CUSTOMERS,
                "521": CustomerSegment.NEW_CUSTOMERS,
                "515": CustomerSegment.NEW_CUSTOMERS,
                "514": CustomerSegment.NEW_CUSTOMERS,
                "513": CustomerSegment.NEW_CUSTOMERS,
                "425": CustomerSegment.NEW_CUSTOMERS,
                "424": CustomerSegment.NEW_CUSTOMERS,
                "413": CustomerSegment.NEW_CUSTOMERS,
                "414": CustomerSegment.NEW_CUSTOMERS,
                "415": CustomerSegment.NEW_CUSTOMERS,
                "315": CustomerSegment.NEW_CUSTOMERS,
                "314": CustomerSegment.NEW_CUSTOMERS,
                "313": CustomerSegment.NEW_CUSTOMERS,
                # Promising: Recent but low frequency customers
                "512": CustomerSegment.PROMISING,
                "511": CustomerSegment.PROMISING,
                "422": CustomerSegment.PROMISING,
                "421": CustomerSegment.PROMISING,
                "412": CustomerSegment.PROMISING,
                "411": CustomerSegment.PROMISING,
                "311": CustomerSegment.PROMISING,
                # Need Attention: Below average in all dimensions
                "323": CustomerSegment.NEED_ATTENTION,
                "322": CustomerSegment.NEED_ATTENTION,
                "231": CustomerSegment.NEED_ATTENTION,
                "241": CustomerSegment.NEED_ATTENTION,
                "251": CustomerSegment.NEED_ATTENTION,
                "233": CustomerSegment.NEED_ATTENTION,
                "232": CustomerSegment.NEED_ATTENTION,
                # About to Sleep: Low recency, moderate F/M
                "331": CustomerSegment.ABOUT_TO_SLEEP,
                "321": CustomerSegment.ABOUT_TO_SLEEP,
                "312": CustomerSegment.ABOUT_TO_SLEEP,
                "221": CustomerSegment.ABOUT_TO_SLEEP,
                "213": CustomerSegment.ABOUT_TO_SLEEP,
                "222": CustomerSegment.ABOUT_TO_SLEEP,
                "132": CustomerSegment.ABOUT_TO_SLEEP,
                # At Risk: Low recency but high F/M (valuable but disengaging)
                "155": CustomerSegment.AT_RISK,
                "154": CustomerSegment.AT_RISK,
                "144": CustomerSegment.AT_RISK,
                "214": CustomerSegment.AT_RISK,
                "215": CustomerSegment.AT_RISK,
                "115": CustomerSegment.AT_RISK,
                "114": CustomerSegment.AT_RISK,
                "113": CustomerSegment.AT_RISK,
                # Cannot Lose Them: Very low R but very high F/M
                "155": CustomerSegment.CANNOT_LOSE_THEM,
                "154": CustomerSegment.CANNOT_LOSE_THEM,
                "145": CustomerSegment.CANNOT_LOSE_THEM,
                "135": CustomerSegment.CANNOT_LOSE_THEM,
                "125": CustomerSegment.CANNOT_LOSE_THEM,
                "124": CustomerSegment.CANNOT_LOSE_THEM,
                # Hibernating: Low in all dimensions
                "332": CustomerSegment.HIBERNATING,
                "322": CustomerSegment.HIBERNATING,
                "231": CustomerSegment.HIBERNATING,
                "241": CustomerSegment.HIBERNATING,
                "251": CustomerSegment.HIBERNATING,
                "155": CustomerSegment.HIBERNATING,
                "154": CustomerSegment.HIBERNATING,
                "144": CustomerSegment.HIBERNATING,
                "214": CustomerSegment.HIBERNATING,
                "215": CustomerSegment.HIBERNATING,
                "115": CustomerSegment.HIBERNATING,
                "114": CustomerSegment.HIBERNATING,
                "113": CustomerSegment.HIBERNATING,
                # Lost: Very low in all dimensions
                "111": CustomerSegment.LOST,
                "112": CustomerSegment.LOST,
                "121": CustomerSegment.LOST,
                "131": CustomerSegment.LOST,
                "141": CustomerSegment.LOST,
                "151": CustomerSegment.LOST,
            }
        else:
            # Quartile-based mapping (simplified)
            return {
                "444": CustomerSegment.CHAMPIONS,
                "443": CustomerSegment.CHAMPIONS,
                "434": CustomerSegment.CHAMPIONS,
                "343": CustomerSegment.LOYAL_CUSTOMERS,
                "344": CustomerSegment.LOYAL_CUSTOMERS,
                "334": CustomerSegment.LOYAL_CUSTOMERS,
                "234": CustomerSegment.POTENTIAL_LOYALISTS,
                "244": CustomerSegment.POTENTIAL_LOYALISTS,
                "424": CustomerSegment.NEW_CUSTOMERS,
                "414": CustomerSegment.NEW_CUSTOMERS,
                "441": CustomerSegment.NEW_CUSTOMERS,
                "431": CustomerSegment.NEW_CUSTOMERS,
                "421": CustomerSegment.NEW_CUSTOMERS,
                "411": CustomerSegment.NEW_CUSTOMERS,
                "314": CustomerSegment.PROMISING,
                "313": CustomerSegment.PROMISING,
                "312": CustomerSegment.PROMISING,
                "311": CustomerSegment.PROMISING,
                "232": CustomerSegment.NEED_ATTENTION,
                "231": CustomerSegment.NEED_ATTENTION,
                "242": CustomerSegment.NEED_ATTENTION,
                "241": CustomerSegment.NEED_ATTENTION,
                "222": CustomerSegment.ABOUT_TO_SLEEP,
                "221": CustomerSegment.ABOUT_TO_SLEEP,
                "212": CustomerSegment.ABOUT_TO_SLEEP,
                "211": CustomerSegment.ABOUT_TO_SLEEP,
                "144": CustomerSegment.AT_RISK,
                "143": CustomerSegment.AT_RISK,
                "134": CustomerSegment.AT_RISK,
                "133": CustomerSegment.AT_RISK,
                "124": CustomerSegment.CANNOT_LOSE_THEM,
                "123": CustomerSegment.CANNOT_LOSE_THEM,
                "132": CustomerSegment.HIBERNATING,
                "131": CustomerSegment.HIBERNATING,
                "122": CustomerSegment.HIBERNATING,
                "121": CustomerSegment.HIBERNATING,
                "111": CustomerSegment.LOST,
                "112": CustomerSegment.LOST,
                "113": CustomerSegment.LOST,
                "114": CustomerSegment.LOST,
            }

    def calculate_rfm_metrics(
        self,
        transactions_df: DataFrame,
        customer_id_col: str = "customer_id",
        transaction_date_col: str = "transaction_date",
        amount_col: str = "amount",
    ) -> DataFrame:
        """
        Calculate RFM metrics from transaction data.

        Args:
            transactions_df: DataFrame with transaction data
            customer_id_col: Name of customer ID column
            transaction_date_col: Name of transaction date column
            amount_col: Name of transaction amount column

        Returns:
            DataFrame with RFM metrics per customer
        """
        logger.info("Calculating RFM metrics from transaction data")

        # Calculate RFM metrics
        rfm_metrics = transactions_df.groupBy(customer_id_col).agg(
            # Recency: Days since last purchase
            datediff(
                lit(self.reference_date.date()), spark_max(col(transaction_date_col))
            ).alias("recency_days"),
            # Frequency: Number of transactions
            count("*").alias("frequency"),
            # Monetary: Total amount spent
            spark_sum(col(amount_col)).alias("monetary"),
            # Additional metrics
            spark_min(col(transaction_date_col)).alias("first_purchase_date"),
            spark_max(col(transaction_date_col)).alias("last_purchase_date"),
            (spark_sum(col(amount_col)) / count("*")).alias("avg_order_value"),
        )

        logger.info(f"Calculated RFM metrics for {rfm_metrics.count()} customers")
        return rfm_metrics

    def assign_rfm_scores(self, rfm_metrics_df: DataFrame) -> DataFrame:
        """
        Assign RFM scores using quintile/quartile-based scoring.

        Args:
            rfm_metrics_df: DataFrame with RFM metrics

        Returns:
            DataFrame with RFM scores added
        """
        logger.info("Assigning RFM scores using statistical ranking")

        # Create window for ranking
        window = Window.orderBy(col("recency_days").asc())
        window_freq = Window.orderBy(col("frequency").desc())
        window_mon = Window.orderBy(col("monetary").desc())

        # Assign scores using ntile function
        scored_df = (
            rfm_metrics_df.withColumn(
                "recency_score", ntile(self.score_range).over(window)
            )
            .withColumn("frequency_score", ntile(self.score_range).over(window_freq))
            .withColumn("monetary_score", ntile(self.score_range).over(window_mon))
        )

        # Create combined RFM score
        scored_df = scored_df.withColumn(
            "rfm_score",
            concat(
                col("recency_score").cast("string"),
                col("frequency_score").cast("string"),
                col("monetary_score").cast("string"),
            ),
        )

        logger.info("RFM scores assigned successfully")
        return scored_df

    def assign_customer_segments(self, scored_df: DataFrame) -> DataFrame:
        """
        Assign customer segments based on RFM scores.

        Args:
            scored_df: DataFrame with RFM scores

        Returns:
            DataFrame with customer segments assigned
        """
        logger.info("Assigning customer segments based on RFM scores")

        # Create segment mapping as a Spark DataFrame
        segment_data = []
        for rfm_score, segment in self.segment_map.items():
            segment_data.append((rfm_score, segment.value))

        segment_schema = StructType(
            [
                StructField("rfm_score", StringType(), True),
                StructField("segment", StringType(), True),
            ]
        )

        segment_df = self.spark.createDataFrame(segment_data, segment_schema)

        # Join with scored data to assign segments
        segmented_df = scored_df.join(segment_df, on="rfm_score", how="left")

        # Handle unmapped scores with default segment
        segmented_df = segmented_df.withColumn(
            "segment",
            when(col("segment").isNull(), "Need Attention").otherwise(col("segment")),
        )

        # Add additional customer classifications
        segmented_df = self._add_customer_classifications(segmented_df)

        logger.info("Customer segments assigned successfully")
        return segmented_df

    def _add_customer_classifications(self, df: DataFrame) -> DataFrame:
        """Add additional customer value and engagement classifications."""

        # Customer Value Tier based on monetary score
        df = df.withColumn(
            "customer_value_tier",
            when(col("monetary_score") >= 4, "High Value")
            .when(col("monetary_score") >= 3, "Medium Value")
            .otherwise("Low Value"),
        )

        # Engagement Level based on recency and frequency
        df = df.withColumn(
            "engagement_level",
            when(
                (col("recency_score") >= 4) & (col("frequency_score") >= 4),
                "Highly Engaged",
            )
            .when(
                (col("recency_score") >= 3) & (col("frequency_score") >= 3),
                "Moderately Engaged",
            )
            .when(col("recency_score") >= 3, "Recently Active")
            .when(col("frequency_score") >= 3, "Frequent but Dormant")
            .otherwise("Low Engagement"),
        )

        return df

    def generate_customer_profiles(
        self,
        transactions_df: DataFrame,
        customer_id_col: str = "customer_id",
        transaction_date_col: str = "transaction_date",
        amount_col: str = "amount",
    ) -> DataFrame:
        """
        Generate complete customer profiles with RFM analysis.

        Args:
            transactions_df: Transaction data
            customer_id_col: Customer ID column name
            transaction_date_col: Transaction date column name
            amount_col: Amount column name

        Returns:
            DataFrame with complete customer profiles
        """
        logger.info("Generating complete customer profiles with RFM analysis")

        # Calculate RFM metrics
        rfm_metrics = self.calculate_rfm_metrics(
            transactions_df, customer_id_col, transaction_date_col, amount_col
        )

        # Assign scores and segments
        scored_df = self.assign_rfm_scores(rfm_metrics)
        segmented_df = self.assign_customer_segments(scored_df)

        # Add derived insights
        final_df = (
            segmented_df.withColumn(
                "customer_lifetime_days",
                datediff(col("last_purchase_date"), col("first_purchase_date")),
            )
            .withColumn(
                "purchase_frequency_per_month",
                col("frequency") / (col("customer_lifetime_days") / 30.0),
            )
            .withColumn("is_new_customer", col("customer_lifetime_days") <= 30)
            .withColumn(
                "is_at_risk",
                col("segment").isin(["At Risk", "Cannot Lose Them", "About to Sleep"]),
            )
        )

        logger.info(f"Generated {final_df.count()} customer profiles")
        return final_df

    def get_segment_summary(self, customer_profiles_df: DataFrame) -> DataFrame:
        """
        Get summary statistics for each customer segment.

        Args:
            customer_profiles_df: DataFrame with customer profiles

        Returns:
            DataFrame with segment summaries
        """
        logger.info("Generating segment summary statistics")

        summary_df = (
            customer_profiles_df.groupBy("segment")
            .agg(
                count("*").alias("customer_count"),
                spark_sum("monetary").alias("total_revenue"),
                (spark_sum("monetary") / count("*")).alias("avg_monetary_value"),
                (spark_sum("frequency") / count("*")).alias("avg_frequency"),
                (spark_sum("recency_days") / count("*")).alias("avg_recency_days"),
                spark_max("monetary").alias("max_monetary_value"),
                spark_min("monetary").alias("min_monetary_value"),
            )
            .withColumn(
                "revenue_percentage",
                col("total_revenue")
                / spark_sum("total_revenue").over(Window.partitionBy())
                * 100,
            )
            .orderBy(desc("total_revenue"))
        )

        logger.info("Segment summary generated successfully")
        return summary_df

    def identify_high_value_customers(
        self, customer_profiles_df: DataFrame, top_percentage: float = 0.2
    ) -> DataFrame:
        """
        Identify high-value customers based on RFM analysis.

        Args:
            customer_profiles_df: Customer profiles DataFrame
            top_percentage: Percentage of customers to classify as high-value

        Returns:
            DataFrame with high-value customers
        """
        logger.info(f"Identifying top {top_percentage*100}% high-value customers")

        # Calculate composite score for ranking
        composite_scored = customer_profiles_df.withColumn(
            "composite_score",
            (
                col("recency_score") * 0.3
                + col("frequency_score") * 0.3
                + col("monetary_score") * 0.4
            ),
        )

        # Rank customers by composite score
        window = Window.orderBy(desc("composite_score"))
        ranked_df = composite_scored.withColumn(
            "customer_rank", percent_rank().over(window)
        )

        # Filter top percentage
        high_value_customers = ranked_df.filter(
            col("customer_rank") >= (1.0 - top_percentage)
        )

        logger.info(f"Identified {high_value_customers.count()} high-value customers")
        return high_value_customers

    def recommend_actions(self, customer_profiles_df: DataFrame) -> DataFrame:
        """
        Recommend marketing actions based on customer segments.

        Args:
            customer_profiles_df: Customer profiles DataFrame

        Returns:
            DataFrame with recommended actions per segment
        """
        logger.info("Generating segment-based action recommendations")

        # Define action recommendations per segment
        action_map = {
            CustomerSegment.CHAMPIONS.value: "Reward them. Can be early adopters of new products. Will promote your brand.",
            CustomerSegment.LOYAL_CUSTOMERS.value: "Upsell higher value products. Ask for reviews. Engage them.",
            CustomerSegment.POTENTIAL_LOYALISTS.value: "Offer membership/loyalty program. Recommend related products.",
            CustomerSegment.NEW_CUSTOMERS.value: "Provide on-boarding support. Give them early success, start building relationship.",
            CustomerSegment.PROMISING.value: "Create brand awareness. Offer free trials.",
            CustomerSegment.NEED_ATTENTION.value: "Make limited time offers. Recommend based on past purchases. Reactivate them.",
            CustomerSegment.ABOUT_TO_SLEEP.value: "Share valuable resources. Recommend popular products. Reconnect with them.",
            CustomerSegment.AT_RISK.value: "Send personalized emails. Offer renewals. Provide helpful resources.",
            CustomerSegment.CANNOT_LOSE_THEM.value: "Win them back via renewals or newer products. Don't lose them to competition. Talk to them.",
            CustomerSegment.HIBERNATING.value: "Offer other product categories. Special customer reactivation campaigns.",
            CustomerSegment.LOST.value: "Revive interest with reach out campaign. Ignore otherwise.",
        }

        # Create recommendations DataFrame
        recommendations_data = []
        for segment, action in action_map.items():
            recommendations_data.append((segment, action))

        recommendations_schema = StructType(
            [
                StructField("segment", StringType(), True),
                StructField("recommended_action", StringType(), True),
            ]
        )

        recommendations_df = self.spark.createDataFrame(
            recommendations_data, recommendations_schema
        )

        # Join with customer profiles
        enriched_df = customer_profiles_df.join(
            recommendations_df, on="segment", how="left"
        )

        logger.info("Action recommendations generated successfully")
        return enriched_df

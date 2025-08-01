"""
Data enrichment pipelines for real-time streaming data.

This module provides comprehensive data enrichment capabilities:
- Customer profile enrichment
- Product catalog lookup
- Geolocation enrichment
- Real-time feature engineering
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_contains,
    broadcast,
    coalesce,
    col,
    current_timestamp,
    date_format,
    dayofweek,
    hour,
    lit,
    lower,
    regexp_extract,
    size,
    split,
    trim,
    udf,
    upper,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from src.utils.logger import get_logger


class DataEnrichmentPipeline:
    """
    Comprehensive data enrichment pipeline for streaming data.

    Provides real-time enrichment capabilities including:
    - Customer profile lookup and enrichment
    - Product catalog enrichment
    - Geolocation and timezone mapping
    - Session and behavior analysis
    - Risk scoring and fraud detection indicators
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize enrichment pipeline.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.logger = get_logger(__name__)

        # Cache for lookup tables
        self._customer_profiles = None
        self._product_catalog = None
        self._geo_mapping = None

    def enrich_transaction_stream(self, df: DataFrame) -> DataFrame:
        """
        Enrich transaction stream with comprehensive business context.

        Args:
            df: Raw transaction stream DataFrame

        Returns:
            Enriched DataFrame with additional business context
        """
        try:
            # Add processing metadata
            enriched_df = df.withColumn("enrichment_timestamp", current_timestamp())

            # Customer enrichment
            enriched_df = self._enrich_customer_data(enriched_df)

            # Product enrichment
            enriched_df = self._enrich_product_data(enriched_df)

            # Temporal enrichment
            enriched_df = self._enrich_temporal_features(enriched_df)

            # Geographic enrichment
            enriched_df = self._enrich_geographic_data(enriched_df)

            # Business logic enrichment
            enriched_df = self._enrich_business_metrics(enriched_df)

            # Risk scoring
            enriched_df = self._enrich_risk_indicators(enriched_df)

            self.logger.info("Successfully enriched transaction stream")
            return enriched_df

        except Exception as e:
            self.logger.error(f"Error enriching transaction stream: {e}")
            raise

    def enrich_user_behavior_stream(self, df: DataFrame) -> DataFrame:
        """
        Enrich user behavior stream with session and engagement context.

        Args:
            df: Raw user behavior stream DataFrame

        Returns:
            Enriched DataFrame with session and engagement metrics
        """
        try:
            # Add processing metadata
            enriched_df = df.withColumn("enrichment_timestamp", current_timestamp())

            # Session enrichment
            enriched_df = self._enrich_session_data(enriched_df)

            # User profile enrichment
            enriched_df = self._enrich_user_profile(enriched_df)

            # Device and browser enrichment
            enriched_df = self._enrich_device_data(enriched_df)

            # Engagement scoring
            enriched_df = self._enrich_engagement_metrics(enriched_df)

            # Funnel position analysis
            enriched_df = self._enrich_funnel_position(enriched_df)

            self.logger.info("Successfully enriched user behavior stream")
            return enriched_df

        except Exception as e:
            self.logger.error(f"Error enriching user behavior stream: {e}")
            raise

    def _enrich_customer_data(self, df: DataFrame) -> DataFrame:
        """Enrich with customer profile information."""
        # Customer tier mapping
        customer_tier_mapping = {
            "premium": "Premium",
            "gold": "Gold",
            "silver": "Silver",
            "bronze": "Bronze",
        }

        # Add customer tier normalization
        enriched_df = df.withColumn(
            "customer_tier_normalized",
            when(col("customer_tier").isNull(), lit("Unknown")).otherwise(
                upper(trim(col("customer_tier")))
            ),
        )

        # Add customer lifetime value category
        enriched_df = enriched_df.withColumn(
            "ltv_category",
            when(col("customer_lifetime_value") >= 1000, lit("High"))
            .when(col("customer_lifetime_value") >= 500, lit("Medium"))
            .when(col("customer_lifetime_value") >= 100, lit("Low"))
            .otherwise(lit("Unknown")),
        )

        return enriched_df

    def _enrich_product_data(self, df: DataFrame) -> DataFrame:
        """Enrich with product catalog information."""
        # Product category hierarchy
        enriched_df = df.withColumn(
            "product_category_main", split(col("category"), "/").getItem(0)
        ).withColumn("product_category_sub", split(col("category"), "/").getItem(1))

        # Price categorization
        enriched_df = enriched_df.withColumn(
            "price_category",
            when(col("price") >= 100, lit("High"))
            .when(col("price") >= 50, lit("Medium"))
            .when(col("price") >= 10, lit("Low"))
            .otherwise(lit("Very Low")),
        )

        # Product popularity score (mock calculation)
        enriched_df = enriched_df.withColumn(
            "popularity_score",
            when(
                col("product_id").isNotNull(),
                (col("price") * 0.1 + lit(50)).cast(IntegerType()),
            ).otherwise(lit(0)),
        )

        return enriched_df

    def _enrich_temporal_features(self, df: DataFrame) -> DataFrame:
        """Enrich with temporal features and business context."""
        enriched_df = (
            df.withColumn("hour_of_day", hour(col("timestamp")))
            .withColumn("day_of_week", dayofweek(col("timestamp")))
            .withColumn(
                "is_weekend",
                when(col("day_of_week").isin([1, 7]), lit(True)).otherwise(lit(False)),
            )
            .withColumn(
                "time_period",
                when(col("hour_of_day").between(6, 11), lit("Morning"))
                .when(col("hour_of_day").between(12, 17), lit("Afternoon"))
                .when(col("hour_of_day").between(18, 22), lit("Evening"))
                .otherwise(lit("Night")),
            )
            .withColumn(
                "is_business_hours",
                when(
                    col("hour_of_day").between(9, 17) & ~col("is_weekend"), lit(True)
                ).otherwise(lit(False)),
            )
        )

        return enriched_df

    def _enrich_geographic_data(self, df: DataFrame) -> DataFrame:
        """Enrich with geographic and location context."""
        # Region mapping based on common patterns
        enriched_df = df.withColumn(
            "region_category",
            when(col("location").rlike("(?i)new.york|ny|nyc"), lit("Northeast"))
            .when(
                col("location").rlike("(?i)california|ca|san.francisco|los.angeles"),
                lit("West"),
            )
            .when(col("location").rlike("(?i)texas|tx|houston|dallas"), lit("South"))
            .when(col("location").rlike("(?i)chicago|illinois|il"), lit("Midwest"))
            .otherwise(lit("Other")),
        )

        # Urban vs Rural classification (simplified)
        enriched_df = enriched_df.withColumn(
            "area_type",
            when(col("location").rlike("(?i)city|downtown|urban"), lit("Urban"))
            .when(col("location").rlike("(?i)suburb|suburban"), lit("Suburban"))
            .otherwise(lit("Rural")),
        )

        return enriched_df

    def _enrich_business_metrics(self, df: DataFrame) -> DataFrame:
        """Enrich with business-specific calculated metrics."""
        # Revenue impact calculation
        enriched_df = df.withColumn(
            "revenue_impact",
            when(
                col("total_amount").isNotNull(), col("total_amount") * lit(0.2)
            ).otherwise(  # 20% margin assumption
                lit(0.0)
            ),
        )

        # Transaction size category
        enriched_df = enriched_df.withColumn(
            "transaction_size",
            when(col("total_amount") >= 200, lit("Large"))
            .when(col("total_amount") >= 50, lit("Medium"))
            .when(col("total_amount") >= 10, lit("Small"))
            .otherwise(lit("Micro")),
        )

        # Quantity analysis
        enriched_df = enriched_df.withColumn(
            "bulk_purchase", when(col("quantity") >= 5, lit(True)).otherwise(lit(False))
        )

        return enriched_df

    def _enrich_risk_indicators(self, df: DataFrame) -> DataFrame:
        """Add risk scoring and fraud detection indicators."""
        # Basic risk score calculation
        enriched_df = df.withColumn(
            "risk_score",
            when(col("total_amount") > 500, lit(3))  # High amount = higher risk
            .when(
                col("is_weekend") & (col("hour_of_day") < 6), lit(2)
            )  # Late weekend = medium risk
            .when(col("payment_method") == "cash", lit(1))  # Cash = low risk
            .otherwise(lit(0)),
        )

        # Risk category
        enriched_df = enriched_df.withColumn(
            "risk_category",
            when(col("risk_score") >= 3, lit("High"))
            .when(col("risk_score") >= 2, lit("Medium"))
            .when(col("risk_score") >= 1, lit("Low"))
            .otherwise(lit("Minimal")),
        )

        # Anomaly flags
        enriched_df = enriched_df.withColumn(
            "anomaly_flags",
            when(col("total_amount") > 1000, lit("high_amount"))
            .when(col("hour_of_day") < 6, lit("unusual_time"))
            .otherwise(lit("none")),
        )

        return enriched_df

    def _enrich_session_data(self, df: DataFrame) -> DataFrame:
        """Enrich user behavior with session context."""
        # Session duration estimation (simplified)
        enriched_df = df.withColumn(
            "estimated_session_duration",
            when(col("event_type") == "page_view", lit(30))  # 30 seconds average
            .when(col("event_type") == "product_view", lit(45))  # 45 seconds average
            .when(col("event_type") == "add_to_cart", lit(60))  # 1 minute average
            .otherwise(lit(15)),  # 15 seconds default
        )

        # Page depth calculation
        enriched_df = enriched_df.withColumn(
            "page_depth",
            when(col("event_type") == "page_view", lit(1))
            .when(col("event_type") == "product_view", lit(2))
            .when(col("event_type") == "add_to_cart", lit(3))
            .when(col("event_type") == "checkout", lit(4))
            .otherwise(lit(0)),
        )

        return enriched_df

    def _enrich_user_profile(self, df: DataFrame) -> DataFrame:
        """Enrich with user profile and behavior patterns."""
        # User engagement level
        enriched_df = df.withColumn(
            "engagement_level",
            when(col("event_type").isin(["purchase", "add_to_cart"]), lit("High"))
            .when(col("event_type").isin(["product_view", "search"]), lit("Medium"))
            .when(col("event_type") == "page_view", lit("Low"))
            .otherwise(lit("Minimal")),
        )

        # Intent scoring
        enriched_df = enriched_df.withColumn(
            "purchase_intent_score",
            when(col("event_type") == "checkout", lit(90))
            .when(col("event_type") == "add_to_cart", lit(70))
            .when(col("event_type") == "product_view", lit(40))
            .when(col("event_type") == "search", lit(30))
            .otherwise(lit(10)),
        )

        return enriched_df

    def _enrich_device_data(self, df: DataFrame) -> DataFrame:
        """Enrich with device and browser context."""
        # Device category extraction
        enriched_df = df.withColumn(
            "device_category",
            when(col("user_agent").rlike("(?i)mobile|android|iphone"), lit("Mobile"))
            .when(col("user_agent").rlike("(?i)tablet|ipad"), lit("Tablet"))
            .otherwise(lit("Desktop")),
        )

        # Browser extraction
        enriched_df = enriched_df.withColumn(
            "browser_family",
            when(col("user_agent").rlike("(?i)chrome"), lit("Chrome"))
            .when(col("user_agent").rlike("(?i)firefox"), lit("Firefox"))
            .when(col("user_agent").rlike("(?i)safari"), lit("Safari"))
            .when(col("user_agent").rlike("(?i)edge"), lit("Edge"))
            .otherwise(lit("Other")),
        )

        return enriched_df

    def _enrich_engagement_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate engagement metrics."""
        # Engagement score
        enriched_df = df.withColumn(
            "engagement_score",
            when(col("event_type") == "purchase", lit(100))
            .when(col("event_type") == "add_to_cart", lit(80))
            .when(col("event_type") == "product_view", lit(60))
            .when(col("event_type") == "search", lit(40))
            .when(col("event_type") == "page_view", lit(20))
            .otherwise(lit(10)),
        )

        # Activity intensity
        enriched_df = enriched_df.withColumn(
            "activity_intensity",
            when(col("engagement_score") >= 80, lit("High"))
            .when(col("engagement_score") >= 40, lit("Medium"))
            .otherwise(lit("Low")),
        )

        return enriched_df

    def _enrich_funnel_position(self, df: DataFrame) -> DataFrame:
        """Analyze position in conversion funnel."""
        # Funnel stage mapping
        enriched_df = df.withColumn(
            "funnel_stage",
            when(col("event_type") == "page_view", lit("Awareness"))
            .when(col("event_type") == "search", lit("Interest"))
            .when(col("event_type") == "product_view", lit("Consideration"))
            .when(col("event_type") == "add_to_cart", lit("Intent"))
            .when(col("event_type") == "checkout", lit("Purchase"))
            .otherwise(lit("Unknown")),
        )

        # Conversion probability
        enriched_df = enriched_df.withColumn(
            "conversion_probability",
            when(col("funnel_stage") == "Purchase", lit(95))
            .when(col("funnel_stage") == "Intent", lit(60))
            .when(col("funnel_stage") == "Consideration", lit(25))
            .when(col("funnel_stage") == "Interest", lit(10))
            .when(col("funnel_stage") == "Awareness", lit(2))
            .otherwise(lit(0)),
        )

        return enriched_df

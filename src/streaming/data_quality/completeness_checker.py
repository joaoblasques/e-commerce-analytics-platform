"""
Data completeness checking for streaming data.

This module provides comprehensive data completeness analysis including
null value detection, missing field analysis, and data quality scoring.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_timestamp,
    isnan,
    isnull,
    length,
    lit,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import size, struct
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_json, trim, when

from src.utils.logger import get_logger


@dataclass
class CompletenessRule:
    """Data completeness rule definition."""

    column: str
    required: bool = True
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    allow_empty_strings: bool = False
    custom_validation: Optional[str] = None
    weight: float = 1.0  # Weight for completeness scoring


class CompletenessChecker:
    """
    Data completeness checker for streaming data.

    Provides comprehensive completeness analysis including null checking,
    empty value detection, and overall data quality scoring.
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize completeness checker.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary for completeness settings
        """
        self.spark = spark
        self.logger = get_logger(__name__)
        self.config = config or {}

        # Completeness thresholds
        self.excellent_threshold = self.config.get("excellent_threshold", 0.98)
        self.good_threshold = self.config.get("good_threshold", 0.95)
        self.fair_threshold = self.config.get("fair_threshold", 0.90)

        # Initialize completeness rules for different stream types
        self.completeness_rules = self._initialize_completeness_rules()

    def _initialize_completeness_rules(self) -> Dict[str, List[CompletenessRule]]:
        """Initialize completeness rules for different stream types."""

        return {
            "transaction": [
                CompletenessRule(
                    "transaction_id", required=True, min_length=10, weight=2.0
                ),
                CompletenessRule("user_id", required=True, weight=2.0),
                CompletenessRule("total_amount", required=True, weight=2.0),
                CompletenessRule(
                    "currency", required=True, min_length=3, max_length=3, weight=1.5
                ),
                CompletenessRule(
                    "payment_method", required=True, min_length=3, weight=1.5
                ),
                CompletenessRule("timestamp", required=True, weight=2.0),
                CompletenessRule("product_id", required=False, weight=1.0),
                CompletenessRule(
                    "location", required=False, allow_empty_strings=True, weight=0.5
                ),
                CompletenessRule("customer_tier", required=False, weight=0.5),
                CompletenessRule("category", required=False, weight=1.0),
            ],
            "user_behavior": [
                CompletenessRule("user_id", required=True, weight=2.0),
                CompletenessRule("session_id", required=True, min_length=5, weight=2.0),
                CompletenessRule("event_type", required=True, min_length=3, weight=2.0),
                CompletenessRule("timestamp", required=True, weight=2.0),
                CompletenessRule("page_url", required=False, min_length=7, weight=1.0),
                CompletenessRule(
                    "user_agent", required=False, min_length=10, weight=1.0
                ),
                CompletenessRule("product_id", required=False, weight=1.0),
                CompletenessRule(
                    "referrer_url", required=False, allow_empty_strings=True, weight=0.5
                ),
                CompletenessRule("device_type", required=False, weight=0.5),
            ],
            "customer_profile": [
                CompletenessRule("user_id", required=True, weight=2.0),
                CompletenessRule("email", required=True, min_length=5, weight=2.0),
                CompletenessRule("registration_date", required=True, weight=1.5),
                CompletenessRule("customer_tier", required=False, weight=1.0),
                CompletenessRule("age", required=False, weight=1.0),
                CompletenessRule(
                    "location", required=False, allow_empty_strings=True, weight=0.5
                ),
                CompletenessRule("phone", required=False, min_length=10, weight=1.0),
                CompletenessRule(
                    "preferences", required=False, allow_empty_strings=True, weight=0.5
                ),
            ],
        }

    def check_completeness(
        self, df: DataFrame, stream_type: str = "transaction"
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Check data completeness for streaming data.

        Args:
            df: Input DataFrame to check
            stream_type: Type of stream (transaction, user_behavior, customer_profile)

        Returns:
            Tuple of (dataframe_with_completeness_flags, completeness_results)
        """
        try:
            self.logger.info(f"Starting completeness check for {stream_type} stream")

            # Get completeness rules for stream type
            rules = self.completeness_rules.get(stream_type, [])
            if not rules:
                self.logger.warning(
                    f"No completeness rules found for stream type: {stream_type}"
                )
                return df.withColumn("completeness_passed", lit(True)), {
                    "overall_completeness": 1.0
                }

            # Apply completeness checks
            completeness_df = df
            field_completeness = {}

            for rule in rules:
                completeness_df, field_score = self._check_field_completeness(
                    completeness_df, rule
                )
                field_completeness[rule.column] = field_score

            # Calculate overall completeness score
            completeness_df = self._calculate_overall_completeness(
                completeness_df, rules
            )

            # Add completeness category
            completeness_df = self._add_completeness_category(completeness_df)

            # Generate completeness summary
            completeness_summary = self._generate_completeness_summary(
                completeness_df, field_completeness, stream_type
            )

            self.logger.info(
                f"Completeness check completed: {completeness_summary['overall_completeness']:.2%}"
            )
            return completeness_df, completeness_summary

        except Exception as e:
            self.logger.error(f"Error in completeness check: {e}")
            raise

    def _check_field_completeness(
        self, df: DataFrame, rule: CompletenessRule
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """Check completeness for a single field."""

        column_name = rule.column
        completeness_col = f"completeness_{column_name}"

        try:
            # Check if column exists
            if column_name not in df.columns:
                self.logger.warning(f"Column {column_name} not found in DataFrame")
                df = df.withColumn(completeness_col, lit(0.0))
                return df, {
                    "column": column_name,
                    "completeness_score": 0.0,
                    "required": rule.required,
                    "issues": ["Column not found"],
                    "total_records": df.count(),
                    "valid_records": 0,
                }

            # Base completeness check (null and NaN)
            base_condition = ~(col(column_name).isNull() | isnan(col(column_name)))

            # Add empty string check if not allowed
            if not rule.allow_empty_strings:
                base_condition = base_condition & (
                    (col(column_name).cast("string").isNull())
                    | (trim(col(column_name).cast("string")) != "")
                )

            # Add length constraints
            if rule.min_length is not None:
                base_condition = base_condition & (
                    length(col(column_name).cast("string")) >= rule.min_length
                )

            if rule.max_length is not None:
                base_condition = base_condition & (
                    length(col(column_name).cast("string")) <= rule.max_length
                )

            # Apply custom validation if provided
            if rule.custom_validation:
                try:
                    # Simple custom validation - extend as needed
                    if "email" in rule.custom_validation.lower():
                        base_condition = base_condition & col(column_name).rlike(
                            r"^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                        )
                except Exception as e:
                    self.logger.warning(
                        f"Custom validation failed for {column_name}: {e}"
                    )

            # Add completeness flag
            df = df.withColumn(completeness_col, base_condition.cast("int"))

            # Calculate completeness statistics
            total_records = df.count()
            valid_records = df.filter(col(completeness_col) == 1).count()
            completeness_score = valid_records / max(total_records, 1)

            # Identify issues
            issues = []
            if not rule.allow_empty_strings:
                empty_count = df.filter(
                    col(column_name).isNotNull()
                    & (trim(col(column_name).cast("string")) == "")
                ).count()
                if empty_count > 0:
                    issues.append(f"{empty_count} empty strings")

            null_count = df.filter(col(column_name).isNull()).count()
            if null_count > 0:
                issues.append(f"{null_count} null values")

            if rule.min_length is not None:
                short_count = df.filter(
                    col(column_name).isNotNull()
                    & (length(col(column_name).cast("string")) < rule.min_length)
                ).count()
                if short_count > 0:
                    issues.append(
                        f"{short_count} values too short (min: {rule.min_length})"
                    )

            if rule.max_length is not None:
                long_count = df.filter(
                    col(column_name).isNotNull()
                    & (length(col(column_name).cast("string")) > rule.max_length)
                ).count()
                if long_count > 0:
                    issues.append(
                        f"{long_count} values too long (max: {rule.max_length})"
                    )

            field_result = {
                "column": column_name,
                "completeness_score": completeness_score,
                "required": rule.required,
                "weight": rule.weight,
                "total_records": total_records,
                "valid_records": valid_records,
                "invalid_records": total_records - valid_records,
                "issues": issues,
            }

            return df, field_result

        except Exception as e:
            self.logger.error(f"Error checking completeness for {column_name}: {e}")
            df = df.withColumn(completeness_col, lit(0.0))
            return df, {
                "column": column_name,
                "completeness_score": 0.0,
                "required": rule.required,
                "issues": [f"Error: {str(e)}"],
                "total_records": df.count(),
                "valid_records": 0,
            }

    def _calculate_overall_completeness(
        self, df: DataFrame, rules: List[CompletenessRule]
    ) -> DataFrame:
        """Calculate overall completeness score."""

        # Get completeness columns with weights
        completeness_columns = []
        total_weight = 0

        for rule in rules:
            completeness_col = f"completeness_{rule.column}"
            if completeness_col in df.columns:
                completeness_columns.append((completeness_col, rule.weight))
                total_weight += rule.weight

        if not completeness_columns or total_weight == 0:
            return df.withColumn("overall_completeness_score", lit(0.0))

        # Calculate weighted completeness score
        weighted_sum = lit(0.0)
        for col_name, weight in completeness_columns:
            weighted_sum = weighted_sum + (col(col_name) * lit(weight))

        overall_score = weighted_sum / lit(total_weight)

        return df.withColumn("overall_completeness_score", overall_score)

    def _add_completeness_category(self, df: DataFrame) -> DataFrame:
        """Add completeness category based on score."""

        return (
            df.withColumn(
                "completeness_category",
                when(
                    col("overall_completeness_score") >= self.excellent_threshold,
                    lit("excellent"),
                )
                .when(
                    col("overall_completeness_score") >= self.good_threshold,
                    lit("good"),
                )
                .when(
                    col("overall_completeness_score") >= self.fair_threshold,
                    lit("fair"),
                )
                .otherwise(lit("poor")),
            )
            .withColumn(
                "completeness_passed",
                col("overall_completeness_score") >= self.fair_threshold,
            )
            .withColumn("completeness_check_timestamp", current_timestamp())
        )

    def _generate_completeness_summary(
        self, df: DataFrame, field_completeness: Dict[str, Any], stream_type: str
    ) -> Dict[str, Any]:
        """Generate completeness summary statistics."""

        try:
            total_records = df.count()

            # Calculate overall statistics
            overall_completeness = (
                df.select(
                    spark_round(col("overall_completeness_score").cast("double"), 4)
                )
                .agg({"round(CAST(overall_completeness_score AS DOUBLE), 4)": "avg"})
                .collect()[0][0]
                or 0.0
            )

            passed_records = df.filter(col("completeness_passed")).count()

            # Get completeness category distribution
            category_distribution = {}
            try:
                categories = df.groupBy("completeness_category").count().collect()
                for row in categories:
                    category_distribution[row["completeness_category"]] = {
                        "count": row["count"],
                        "percentage": round(
                            row["count"] / max(total_records, 1) * 100, 2
                        ),
                    }
            except Exception as e:
                self.logger.warning(f"Could not calculate category distribution: {e}")

            # Identify most problematic fields
            problematic_fields = []
            for field, stats in field_completeness.items():
                if (
                    stats["completeness_score"] < self.fair_threshold
                    and stats["required"]
                ):
                    problematic_fields.append(
                        {
                            "field": field,
                            "completeness_score": stats["completeness_score"],
                            "issues": stats.get("issues", []),
                        }
                    )

            # Sort by completeness score (worst first)
            problematic_fields.sort(key=lambda x: x["completeness_score"])

            return {
                "stream_type": stream_type,
                "total_records": total_records,
                "passed_records": passed_records,
                "failed_records": total_records - passed_records,
                "overall_completeness": overall_completeness,
                "pass_rate": passed_records / max(total_records, 1),
                "category_distribution": category_distribution,
                "field_completeness": field_completeness,
                "problematic_fields": problematic_fields[
                    :10
                ],  # Top 10 most problematic
                "excellent_records": category_distribution.get("excellent", {}).get(
                    "count", 0
                ),
                "good_records": category_distribution.get("good", {}).get("count", 0),
                "fair_records": category_distribution.get("fair", {}).get("count", 0),
                "poor_records": category_distribution.get("poor", {}).get("count", 0),
            }

        except Exception as e:
            self.logger.error(f"Error generating completeness summary: {e}")
            return {
                "stream_type": stream_type,
                "total_records": df.count(),
                "overall_completeness": 0.0,
                "error": str(e),
            }

    def add_custom_completeness_rule(
        self, stream_type: str, rule: CompletenessRule
    ) -> None:
        """
        Add a custom completeness rule.

        Args:
            stream_type: Type of stream to add rule for
            rule: CompletenessRule to add
        """
        if stream_type not in self.completeness_rules:
            self.completeness_rules[stream_type] = []

        self.completeness_rules[stream_type].append(rule)
        self.logger.info(
            f"Added custom completeness rule for '{rule.column}' in {stream_type} stream"
        )

    def get_completeness_dashboard_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate metrics for completeness monitoring dashboard.

        Args:
            df: DataFrame with completeness results

        Returns:
            Dictionary with dashboard metrics
        """
        try:
            total_records = df.count()

            # Overall metrics
            avg_completeness = (
                df.select(col("overall_completeness_score").cast("double"))
                .agg({"overall_completeness_score": "avg"})
                .collect()[0][0]
                or 0.0
            )

            # Category distribution
            category_counts = df.groupBy("completeness_category").count().collect()
            category_distribution = {
                row["completeness_category"]: {
                    "count": row["count"],
                    "percentage": round(row["count"] / max(total_records, 1) * 100, 2),
                }
                for row in category_counts
            }

            # Field-level metrics
            completeness_columns = [
                c
                for c in df.columns
                if c.startswith("completeness_") and c != "completeness_category"
            ]
            field_metrics = {}

            for col_name in completeness_columns:
                field_name = col_name.replace("completeness_", "")
                try:
                    avg_score = (
                        df.select(col(col_name).cast("double"))
                        .agg({col_name: "avg"})
                        .collect()[0][0]
                        or 0.0
                    )
                    field_metrics[field_name] = round(avg_score, 4)
                except Exception:
                    field_metrics[field_name] = 0.0

            return {
                "total_records": total_records,
                "average_completeness_score": round(avg_completeness, 4),
                "category_distribution": category_distribution,
                "field_completeness_scores": field_metrics,
                "pass_rate": round(
                    df.filter(col("completeness_passed")).count()
                    / max(total_records, 1)
                    * 100,
                    2,
                ),
            }

        except Exception as e:
            self.logger.error(f"Error generating completeness dashboard metrics: {e}")
            return {}

    def create_completeness_monitoring_stream(self, df: DataFrame) -> DataFrame:
        """
        Create a monitoring stream for completeness tracking.

        Args:
            df: DataFrame with completeness results

        Returns:
            DataFrame with completeness monitoring data
        """
        completeness_columns = [c for c in df.columns if c.startswith("completeness_")]

        return df.withColumn(
            "completeness_summary",
            struct(
                col("overall_completeness_score"),
                col("completeness_category"),
                col("completeness_passed"),
                *[
                    col(c)
                    for c in completeness_columns
                    if c
                    not in ["completeness_category", "completeness_check_timestamp"]
                ],
            ),
        ).select(
            "completeness_check_timestamp",
            "completeness_summary",
            "overall_completeness_score",
            "completeness_category",
            "completeness_passed",
        )

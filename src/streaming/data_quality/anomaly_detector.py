"""
Anomaly detection for streaming data.

This module provides real-time anomaly detection capabilities using
statistical methods and machine learning approaches for data streams.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_timestamp,
    lag,
    lead,
    lit,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import mean
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import percentile_approx
from pyspark.sql.functions import pow as spark_pow
from pyspark.sql.functions import row_number, sqrt, stddev
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when, window
from pyspark.sql.window import Window

from src.utils.logger import get_logger


class AnomalyType(Enum):
    """Types of anomalies that can be detected."""

    STATISTICAL_OUTLIER = "statistical_outlier"
    PATTERN_DEVIATION = "pattern_deviation"
    VOLUME_ANOMALY = "volume_anomaly"
    TEMPORAL_ANOMALY = "temporal_anomaly"
    BUSINESS_RULE_VIOLATION = "business_rule_violation"


@dataclass
class AnomalyRule:
    """Anomaly detection rule definition."""

    name: str
    description: str
    anomaly_type: AnomalyType
    threshold: float
    column: str
    enabled: bool = True
    severity: str = "medium"  # "low", "medium", "high", "critical"


class StreamingAnomalyDetector:
    """
    Real-time anomaly detection for streaming data.

    Provides statistical outlier detection, pattern analysis,
    and business rule-based anomaly detection for data streams.
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize streaming anomaly detector.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary for anomaly detection settings
        """
        self.spark = spark
        self.logger = get_logger(__name__)
        self.config = config or {}

        # Detection thresholds
        self.z_score_threshold = self.config.get("z_score_threshold", 3.0)
        self.iqr_multiplier = self.config.get("iqr_multiplier", 1.5)
        self.volume_change_threshold = self.config.get("volume_change_threshold", 0.5)

        # Initialize anomaly rules for different stream types
        self.anomaly_rules = self._initialize_anomaly_rules()

    def _initialize_anomaly_rules(self) -> Dict[str, List[AnomalyRule]]:
        """Initialize anomaly detection rules for different stream types."""

        return {
            "transaction": [
                AnomalyRule(
                    name="amount_outlier",
                    description="Detect unusually high transaction amounts",
                    anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                    threshold=3.0,
                    column="total_amount",
                    severity="high",
                ),
                AnomalyRule(
                    name="frequency_anomaly",
                    description="Detect unusual transaction frequency per user",
                    anomaly_type=AnomalyType.PATTERN_DEVIATION,
                    threshold=2.5,
                    column="user_id",
                    severity="medium",
                ),
                AnomalyRule(
                    name="late_night_transaction",
                    description="Detect transactions during unusual hours",
                    anomaly_type=AnomalyType.TEMPORAL_ANOMALY,
                    threshold=0.05,  # 5% threshold for late night activity
                    column="hour",
                    severity="low",
                ),
                AnomalyRule(
                    name="zero_amount",
                    description="Detect zero or negative amounts",
                    anomaly_type=AnomalyType.BUSINESS_RULE_VIOLATION,
                    threshold=0.0,
                    column="total_amount",
                    severity="critical",
                ),
            ],
            "user_behavior": [
                AnomalyRule(
                    name="session_duration_outlier",
                    description="Detect unusually long session durations",
                    anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                    threshold=3.0,
                    column="session_duration",
                    severity="medium",
                ),
                AnomalyRule(
                    name="page_view_burst",
                    description="Detect unusual page view bursts",
                    anomaly_type=AnomalyType.VOLUME_ANOMALY,
                    threshold=2.0,
                    column="page_views_per_minute",
                    severity="medium",
                ),
                AnomalyRule(
                    name="bot_behavior",
                    description="Detect potential bot-like behavior patterns",
                    anomaly_type=AnomalyType.PATTERN_DEVIATION,
                    threshold=0.1,
                    column="user_agent",
                    severity="high",
                ),
            ],
            "customer_profile": [
                AnomalyRule(
                    name="age_outlier",
                    description="Detect unusual age values",
                    anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                    threshold=3.0,
                    column="age",
                    severity="low",
                ),
                AnomalyRule(
                    name="ltv_spike",
                    description="Detect sudden lifetime value spikes",
                    anomaly_type=AnomalyType.PATTERN_DEVIATION,
                    threshold=5.0,
                    column="customer_lifetime_value",
                    severity="high",
                ),
            ],
        }

    def detect_anomalies(
        self, df: DataFrame, stream_type: str = "transaction"
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Detect anomalies in streaming data.

        Args:
            df: Input DataFrame to analyze
            stream_type: Type of stream (transaction, user_behavior, customer_profile)

        Returns:
            Tuple of (dataframe_with_anomaly_flags, anomaly_results)
        """
        try:
            self.logger.info(f"Starting anomaly detection for {stream_type} stream")

            # Get anomaly rules for stream type
            rules = self.anomaly_rules.get(stream_type, [])
            if not rules:
                self.logger.warning(
                    f"No anomaly rules found for stream type: {stream_type}"
                )
                return df.withColumn("is_anomaly", lit(False)), {"anomaly_count": 0}

            # Apply different types of anomaly detection
            anomaly_df = df
            anomaly_results = []

            # 1. Statistical outlier detection
            anomaly_df = self._detect_statistical_outliers(anomaly_df, rules)

            # 2. Pattern deviation detection
            anomaly_df = self._detect_pattern_deviations(anomaly_df, rules)

            # 3. Volume anomaly detection
            anomaly_df = self._detect_volume_anomalies(anomaly_df, rules)

            # 4. Temporal anomaly detection
            anomaly_df = self._detect_temporal_anomalies(anomaly_df, rules)

            # 5. Business rule violation detection
            anomaly_df = self._detect_business_rule_violations(anomaly_df, rules)

            # Add overall anomaly flag
            anomaly_df = self._add_overall_anomaly_flag(anomaly_df)

            # Calculate summary metrics
            total_count = anomaly_df.count()
            anomaly_count = anomaly_df.filter(col("is_anomaly")).count()

            anomaly_summary = {
                "stream_type": stream_type,
                "total_count": total_count,
                "anomaly_count": anomaly_count,
                "anomaly_rate": anomaly_count / max(total_count, 1),
                "rules_applied": len([r for r in rules if r.enabled]),
                "anomalies": self._extract_anomaly_details(anomaly_df),
            }

            self.logger.info(
                f"Anomaly detection completed: {anomaly_count}/{total_count} anomalies found"
            )
            return anomaly_df, anomaly_summary

        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            raise

    def _detect_statistical_outliers(
        self, df: DataFrame, rules: List[AnomalyRule]
    ) -> DataFrame:
        """Detect statistical outliers using Z-score and IQR methods."""

        outlier_rules = [
            r
            for r in rules
            if r.anomaly_type == AnomalyType.STATISTICAL_OUTLIER and r.enabled
        ]

        for rule in outlier_rules:
            if rule.column not in df.columns:
                continue

            try:
                # Calculate statistical metrics
                stats = df.select(
                    mean(col(rule.column)).alias("mean_val"),
                    stddev(col(rule.column)).alias("stddev_val"),
                    percentile_approx(col(rule.column), 0.25).alias("q1"),
                    percentile_approx(col(rule.column), 0.75).alias("q3"),
                ).collect()[0]

                if stats["stddev_val"] is None or stats["stddev_val"] == 0:
                    self.logger.warning(
                        f"Cannot calculate Z-score for {rule.column}: stddev is 0"
                    )
                    df = df.withColumn(f"anomaly_{rule.name}", lit(False))
                    continue

                # Z-score method
                z_score_threshold = rule.threshold
                df = df.withColumn(
                    f"z_score_{rule.column}",
                    spark_abs(
                        (col(rule.column) - lit(stats["mean_val"]))
                        / lit(stats["stddev_val"])
                    ),
                ).withColumn(
                    f"anomaly_{rule.name}_zscore",
                    col(f"z_score_{rule.column}") > z_score_threshold,
                )

                # IQR method
                iqr = stats["q3"] - stats["q1"]
                lower_bound = stats["q1"] - (self.iqr_multiplier * iqr)
                upper_bound = stats["q3"] + (self.iqr_multiplier * iqr)

                df = df.withColumn(
                    f"anomaly_{rule.name}_iqr",
                    (col(rule.column) < lit(lower_bound))
                    | (col(rule.column) > lit(upper_bound)),
                )

                # Combine Z-score and IQR
                df = df.withColumn(
                    f"anomaly_{rule.name}",
                    col(f"anomaly_{rule.name}_zscore")
                    | col(f"anomaly_{rule.name}_iqr"),
                )

            except Exception as e:
                self.logger.error(f"Error detecting outliers for rule {rule.name}: {e}")
                df = df.withColumn(f"anomaly_{rule.name}", lit(False))

        return df

    def _detect_pattern_deviations(
        self, df: DataFrame, rules: List[AnomalyRule]
    ) -> DataFrame:
        """Detect pattern deviations and unusual behaviors."""

        pattern_rules = [
            r
            for r in rules
            if r.anomaly_type == AnomalyType.PATTERN_DEVIATION and r.enabled
        ]

        for rule in pattern_rules:
            try:
                if rule.name == "frequency_anomaly" and "user_id" in df.columns:
                    # Detect unusual transaction frequency per user
                    window_spec = (
                        Window.partitionBy("user_id")
                        .orderBy("timestamp")
                        .rangeBetween(-3600, 0)
                    )  # 1 hour window

                    df = df.withColumn(
                        "user_transaction_count_1h", count("*").over(window_spec)
                    )

                    # Calculate average frequency
                    avg_freq = (
                        df.select(mean("user_transaction_count_1h")).collect()[0][0]
                        or 1
                    )

                    df = df.withColumn(
                        f"anomaly_{rule.name}",
                        col("user_transaction_count_1h")
                        > (lit(avg_freq) * rule.threshold),
                    )

                elif rule.name == "bot_behavior" and "user_agent" in df.columns:
                    # Simple bot detection based on user agent patterns
                    df = df.withColumn(
                        f"anomaly_{rule.name}",
                        col("user_agent").rlike("(?i)(bot|crawler|spider|scraper)"),
                    )

                elif (
                    rule.name == "ltv_spike" and "customer_lifetime_value" in df.columns
                ):
                    # Detect sudden LTV changes
                    window_spec = Window.partitionBy("user_id").orderBy("timestamp")

                    df = (
                        df.withColumn(
                            "prev_ltv", lag("customer_lifetime_value").over(window_spec)
                        )
                        .withColumn(
                            "ltv_change_ratio",
                            when(
                                col("prev_ltv") > 0,
                                col("customer_lifetime_value") / col("prev_ltv"),
                            ).otherwise(lit(1)),
                        )
                        .withColumn(
                            f"anomaly_{rule.name}",
                            col("ltv_change_ratio") > rule.threshold,
                        )
                    )

                else:
                    df = df.withColumn(f"anomaly_{rule.name}", lit(False))

            except Exception as e:
                self.logger.error(
                    f"Error detecting pattern deviation for rule {rule.name}: {e}"
                )
                df = df.withColumn(f"anomaly_{rule.name}", lit(False))

        return df

    def _detect_volume_anomalies(
        self, df: DataFrame, rules: List[AnomalyRule]
    ) -> DataFrame:
        """Detect volume-based anomalies like traffic spikes."""

        volume_rules = [
            r
            for r in rules
            if r.anomaly_type == AnomalyType.VOLUME_ANOMALY and r.enabled
        ]

        for rule in volume_rules:
            try:
                if rule.name == "page_view_burst" and "timestamp" in df.columns:
                    # Detect page view bursts per minute
                    df_with_minute = df.withColumn(
                        "minute_window", window(col("timestamp"), "1 minute")
                    )

                    page_views_per_minute = df_with_minute.groupBy(
                        "minute_window", "user_id"
                    ).agg(count("*").alias("page_views_per_minute"))

                    # Calculate average page views per minute
                    avg_page_views = (
                        page_views_per_minute.select(
                            mean("page_views_per_minute")
                        ).collect()[0][0]
                        or 1
                    )

                    # Join back to identify bursts
                    df = df.join(
                        page_views_per_minute.select(
                            col("minute_window.start").alias("window_start"),
                            "user_id",
                            "page_views_per_minute",
                        ),
                        ["user_id"],
                    ).withColumn(
                        f"anomaly_{rule.name}",
                        col("page_views_per_minute")
                        > (lit(avg_page_views) * rule.threshold),
                    )

                else:
                    df = df.withColumn(f"anomaly_{rule.name}", lit(False))

            except Exception as e:
                self.logger.error(
                    f"Error detecting volume anomaly for rule {rule.name}: {e}"
                )
                df = df.withColumn(f"anomaly_{rule.name}", lit(False))

        return df

    def _detect_temporal_anomalies(
        self, df: DataFrame, rules: List[AnomalyRule]
    ) -> DataFrame:
        """Detect temporal anomalies like unusual timing patterns."""

        temporal_rules = [
            r
            for r in rules
            if r.anomaly_type == AnomalyType.TEMPORAL_ANOMALY and r.enabled
        ]

        for rule in temporal_rules:
            try:
                if rule.name == "late_night_transaction" and "timestamp" in df.columns:
                    # Add hour column if not present
                    if "hour" not in df.columns:
                        df = df.withColumn(
                            "hour", col("timestamp").cast("timestamp").hour()
                        )

                    # Detect late night transactions (11 PM to 5 AM)
                    df = df.withColumn(
                        f"anomaly_{rule.name}", (col("hour") >= 23) | (col("hour") <= 5)
                    )

                else:
                    df = df.withColumn(f"anomaly_{rule.name}", lit(False))

            except Exception as e:
                self.logger.error(
                    f"Error detecting temporal anomaly for rule {rule.name}: {e}"
                )
                df = df.withColumn(f"anomaly_{rule.name}", lit(False))

        return df

    def _detect_business_rule_violations(
        self, df: DataFrame, rules: List[AnomalyRule]
    ) -> DataFrame:
        """Detect business rule violations."""

        business_rules = [
            r
            for r in rules
            if r.anomaly_type == AnomalyType.BUSINESS_RULE_VIOLATION and r.enabled
        ]

        for rule in business_rules:
            try:
                if rule.name == "zero_amount" and "total_amount" in df.columns:
                    df = df.withColumn(
                        f"anomaly_{rule.name}",
                        (col("total_amount") <= rule.threshold)
                        | col("total_amount").isNull(),
                    )

                else:
                    df = df.withColumn(f"anomaly_{rule.name}", lit(False))

            except Exception as e:
                self.logger.error(
                    f"Error detecting business rule violation for rule {rule.name}: {e}"
                )
                df = df.withColumn(f"anomaly_{rule.name}", lit(False))

        return df

    def _add_overall_anomaly_flag(self, df: DataFrame) -> DataFrame:
        """Add overall anomaly flag based on all detection results."""

        # Get all anomaly columns
        anomaly_columns = [
            c
            for c in df.columns
            if c.startswith("anomaly_") and not c.endswith(("_zscore", "_iqr"))
        ]

        if not anomaly_columns:
            return df.withColumn("is_anomaly", lit(False))

        # OR all anomaly flags
        anomaly_expr = col(anomaly_columns[0])
        for col_name in anomaly_columns[1:]:
            anomaly_expr = anomaly_expr | col(col_name)

        return df.withColumn("is_anomaly", anomaly_expr).withColumn(
            "anomaly_detection_timestamp", current_timestamp()
        )

    def _extract_anomaly_details(self, df: DataFrame) -> List[Dict[str, Any]]:
        """Extract details about detected anomalies."""

        try:
            # Get sample of anomalies for reporting
            anomaly_sample = df.filter(col("is_anomaly")).limit(100).collect()

            anomalies = []
            for row in anomaly_sample:
                anomaly_types = []

                # Check which specific anomaly types were triggered
                for col_name in row.asDict():
                    if col_name.startswith("anomaly_") and row[col_name] is True:
                        anomaly_types.append(col_name.replace("anomaly_", ""))

                if anomaly_types:
                    anomalies.append(
                        {
                            "timestamp": row.get("timestamp"),
                            "anomaly_types": anomaly_types,
                            "sample_data": {
                                k: v
                                for k, v in row.asDict().items()
                                if not k.startswith("anomaly_") and k != "is_anomaly"
                            },
                        }
                    )

            return anomalies

        except Exception as e:
            self.logger.error(f"Error extracting anomaly details: {e}")
            return []

    def add_custom_anomaly_rule(self, stream_type: str, rule: AnomalyRule) -> None:
        """
        Add a custom anomaly detection rule.

        Args:
            stream_type: Type of stream to add rule for
            rule: AnomalyRule to add
        """
        if stream_type not in self.anomaly_rules:
            self.anomaly_rules[stream_type] = []

        self.anomaly_rules[stream_type].append(rule)
        self.logger.info(
            f"Added custom anomaly rule '{rule.name}' for {stream_type} stream"
        )

    def get_anomaly_summary(self, anomaly_df: DataFrame) -> Dict[str, Any]:
        """
        Generate anomaly detection summary statistics.

        Args:
            anomaly_df: DataFrame with anomaly detection results

        Returns:
            Dictionary with anomaly summary
        """
        try:
            total_records = anomaly_df.count()
            anomaly_records = anomaly_df.filter(col("is_anomaly")).count()

            # Get anomaly type breakdown
            anomaly_columns = [
                c
                for c in anomaly_df.columns
                if c.startswith("anomaly_") and not c.endswith(("_zscore", "_iqr"))
            ]

            anomaly_breakdown = {}
            for anomaly_col in anomaly_columns:
                anomaly_name = anomaly_col.replace("anomaly_", "")
                count = anomaly_df.filter(col(anomaly_col)).count()
                anomaly_breakdown[anomaly_name] = {
                    "count": count,
                    "rate": count / max(total_records, 1),
                }

            return {
                "total_records": total_records,
                "anomaly_records": anomaly_records,
                "normal_records": total_records - anomaly_records,
                "anomaly_rate": anomaly_records / max(total_records, 1),
                "anomaly_breakdown": anomaly_breakdown,
            }

        except Exception as e:
            self.logger.error(f"Error generating anomaly summary: {e}")
            return {}

    def create_anomaly_alert_stream(self, anomaly_df: DataFrame) -> DataFrame:
        """
        Create a stream for anomaly alerts and monitoring.

        Args:
            anomaly_df: DataFrame with anomaly detection results

        Returns:
            DataFrame with anomaly alert data
        """
        return (
            anomaly_df.filter(col("is_anomaly"))
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn(
                "alert_level", lit("medium")
            )  # Could be made dynamic based on severity
            .select(
                "alert_timestamp",
                "timestamp",
                "is_anomaly",
                "alert_level",
                *[c for c in anomaly_df.columns if c.startswith("anomaly_")],
            )
        )

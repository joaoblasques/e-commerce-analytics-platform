"""
Fraud alert prioritization system for intelligent alert management.
"""

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


class AlertPriority(Enum):
    """Alert priority levels."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class AlertStatus(Enum):
    """Alert status types."""

    NEW = "new"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"
    FALSE_POSITIVE = "false_positive"
    ESCALATED = "escalated"


@dataclass
class AlertPriorityFactors:
    """Factors used in alert prioritization."""

    financial_impact: float
    customer_risk: float
    merchant_risk: float
    pattern_complexity: float
    historical_accuracy: float
    time_sensitivity: float
    business_criticality: float


@dataclass
class PrioritizedAlert:
    """Prioritized fraud alert with scoring details."""

    alert_id: str
    transaction_id: str
    customer_id: str
    merchant_id: str
    priority: AlertPriority
    priority_score: float
    financial_impact: float
    triggered_rules: List[str]
    alert_factors: AlertPriorityFactors
    recommended_actions: List[str]
    estimated_investigation_time: int  # minutes
    created_at: str
    status: AlertStatus = AlertStatus.NEW


class FraudAlertPrioritizer:
    """
    Intelligent fraud alert prioritization system.

    Features:
    - Multi-factor scoring algorithm
    - Customer and merchant risk integration
    - Historical performance tracking
    - Dynamic priority adjustment
    - Investigation workflow optimization
    - Resource allocation guidance
    """

    def __init__(self, spark: SparkSession):
        """Initialize the alert prioritizer."""
        self.spark = spark
        self.priority_weights = {
            "financial_impact": 0.25,
            "customer_risk": 0.20,
            "merchant_risk": 0.15,
            "pattern_complexity": 0.15,
            "historical_accuracy": 0.10,
            "time_sensitivity": 0.10,
            "business_criticality": 0.05,
        }
        self.priority_thresholds = {
            AlertPriority.CRITICAL: 0.8,
            AlertPriority.HIGH: 0.6,
            AlertPriority.MEDIUM: 0.4,
            AlertPriority.LOW: 0.2,
        }

    def prioritize_alerts(
        self,
        alerts_df: DataFrame,
        customer_risk_df: Optional[DataFrame] = None,
        merchant_risk_df: Optional[DataFrame] = None,
        historical_performance_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """
        Prioritize fraud alerts based on multiple factors.

        Args:
            alerts_df: DataFrame with fraud alerts
            customer_risk_df: Optional customer risk scores DataFrame
            merchant_risk_df: Optional merchant risk scores DataFrame
            historical_performance_df: Optional historical alert performance data

        Returns:
            DataFrame with prioritized alerts
        """
        # Start with the alerts
        result_df = alerts_df

        # Calculate financial impact factor
        result_df = self._calculate_financial_impact_factor(result_df)

        # Add customer risk factor
        if customer_risk_df:
            result_df = self._add_customer_risk_factor(result_df, customer_risk_df)
        else:
            result_df = result_df.withColumn("customer_risk_factor", F.lit(0.5))

        # Add merchant risk factor
        if merchant_risk_df:
            result_df = self._add_merchant_risk_factor(result_df, merchant_risk_df)
        else:
            result_df = result_df.withColumn("merchant_risk_factor", F.lit(0.5))

        # Calculate pattern complexity factor
        result_df = self._calculate_pattern_complexity_factor(result_df)

        # Add historical accuracy factor
        if historical_performance_df:
            result_df = self._add_historical_accuracy_factor(
                result_df, historical_performance_df
            )
        else:
            result_df = result_df.withColumn("historical_accuracy_factor", F.lit(0.5))

        # Calculate time sensitivity factor
        result_df = self._calculate_time_sensitivity_factor(result_df)

        # Calculate business criticality factor
        result_df = self._calculate_business_criticality_factor(result_df)

        # Calculate final priority score
        result_df = self._calculate_priority_score(result_df)

        # Assign priority levels
        result_df = self._assign_priority_levels(result_df)

        # Add recommended actions
        result_df = self._add_recommended_actions(result_df)

        # Estimate investigation time
        result_df = self._estimate_investigation_time(result_df)

        return result_df

    def _calculate_financial_impact_factor(self, df: DataFrame) -> DataFrame:
        """Calculate financial impact factor (0-1 scale)."""
        # Get transaction amount percentiles for normalization
        percentiles = df.select(
            F.expr("percentile_approx(price, 0.5)").alias("median_amount"),
            F.expr("percentile_approx(price, 0.95)").alias("p95_amount"),
            F.expr("percentile_approx(price, 0.99)").alias("p99_amount"),
        ).collect()[0]

        median_amount = percentiles["median_amount"]
        p95_amount = percentiles["p95_amount"]
        p99_amount = percentiles["p99_amount"]

        return df.withColumn(
            "financial_impact_factor",
            F.when(F.col("price") >= p99_amount, 1.0)
            .when(F.col("price") >= p95_amount, 0.8)
            .when(F.col("price") >= median_amount * 5, 0.6)
            .when(F.col("price") >= median_amount * 2, 0.4)
            .when(F.col("price") >= median_amount, 0.2)
            .otherwise(0.1),
        )

    def _add_customer_risk_factor(
        self, df: DataFrame, customer_risk_df: DataFrame
    ) -> DataFrame:
        """Add customer risk factor from customer risk scores."""
        return df.join(
            customer_risk_df.select(
                "customer_id",
                F.col("customer_risk_score").alias("customer_risk_factor"),
            ),
            "customer_id",
            "left",
        ).fillna({"customer_risk_factor": 0.5})

    def _add_merchant_risk_factor(
        self, df: DataFrame, merchant_risk_df: DataFrame
    ) -> DataFrame:
        """Add merchant risk factor from merchant risk scores."""
        return df.join(
            merchant_risk_df.select(
                "merchant_id",
                F.col("merchant_risk_score").alias("merchant_risk_factor"),
            ),
            "merchant_id",
            "left",
        ).fillna({"merchant_risk_factor": 0.5})

    def _calculate_pattern_complexity_factor(self, df: DataFrame) -> DataFrame:
        """Calculate pattern complexity based on number and types of triggered rules."""
        return df.withColumn(
            "pattern_complexity_factor",
            F.when(F.size("triggered_rules") >= 5, 1.0)
            .when(F.size("triggered_rules") >= 3, 0.8)
            .when(F.size("triggered_rules") >= 2, 0.6)
            .when(F.size("triggered_rules") >= 1, 0.4)
            .otherwise(0.2),
        ).withColumn(
            "pattern_complexity_factor",
            # Bonus for critical severity rules
            F.when(
                F.array_contains("fraud_alerts.severity", "CRITICAL"),
                F.col("pattern_complexity_factor") * 1.5,
            ).otherwise(F.col("pattern_complexity_factor")),
        )

    def _add_historical_accuracy_factor(
        self, df: DataFrame, historical_df: DataFrame
    ) -> DataFrame:
        """Add historical accuracy factor based on past alert performance."""
        # Calculate rule accuracy from historical data
        rule_accuracy = historical_df.groupBy("rule_id").agg(
            F.avg(
                F.when(F.col("resolution") == "TRUE_POSITIVE", 1.0).otherwise(0.0)
            ).alias("accuracy")
        )

        # For alerts with multiple rules, take the average accuracy
        return (
            df.join(
                rule_accuracy.withColumnRenamed("rule_id", "triggered_rule"),
                F.array_contains("triggered_rules", "triggered_rule"),
                "left",
            )
            .groupBy("transaction_id")
            .agg(
                F.first("*").alias("alert_data"),
                F.avg("accuracy").alias("historical_accuracy_factor"),
            )
            .select("alert_data.*", "historical_accuracy_factor")
            .fillna({"historical_accuracy_factor": 0.5})
        )

    def _calculate_time_sensitivity_factor(self, df: DataFrame) -> DataFrame:
        """Calculate time sensitivity factor based on transaction recency and patterns."""
        current_time = F.current_timestamp()

        return df.withColumn(
            "minutes_since_transaction",
            (F.unix_timestamp(current_time) - F.unix_timestamp("timestamp")) / 60,
        ).withColumn(
            "time_sensitivity_factor",
            F.when(F.col("minutes_since_transaction") <= 5, 1.0)  # Very recent
            .when(F.col("minutes_since_transaction") <= 15, 0.9)  # Recent
            .when(F.col("minutes_since_transaction") <= 60, 0.7)  # Moderately recent
            .when(F.col("minutes_since_transaction") <= 240, 0.5)  # Within 4 hours
            .when(F.col("minutes_since_transaction") <= 1440, 0.3)  # Within 24 hours
            .otherwise(0.1),  # Older than 24 hours
        )

    def _calculate_business_criticality_factor(self, df: DataFrame) -> DataFrame:
        """Calculate business criticality based on transaction characteristics."""
        return df.withColumn(
            "business_criticality_factor",
            F.when(F.col("payment_method").isin(["credit_card", "bank_transfer"]), 0.8)
            .when(F.col("payment_method") == "digital_wallet", 0.6)
            .when(F.col("payment_method") == "paypal", 0.5)
            .otherwise(0.3),
        ).withColumn(
            "business_criticality_factor",
            # Higher criticality for high-value customers or merchants
            F.when(
                F.col("customer_risk_factor") > 0.7,
                F.col("business_criticality_factor") * 1.2,
            )
            .when(
                F.col("merchant_risk_factor") > 0.7,
                F.col("business_criticality_factor") * 1.1,
            )
            .otherwise(F.col("business_criticality_factor")),
        )

    def _calculate_priority_score(self, df: DataFrame) -> DataFrame:
        """Calculate weighted priority score."""
        weights = self.priority_weights

        priority_score_expr = (
            F.col("financial_impact_factor") * weights["financial_impact"]
            + F.col("customer_risk_factor") * weights["customer_risk"]
            + F.col("merchant_risk_factor") * weights["merchant_risk"]
            + F.col("pattern_complexity_factor") * weights["pattern_complexity"]
            + F.col("historical_accuracy_factor") * weights["historical_accuracy"]
            + F.col("time_sensitivity_factor") * weights["time_sensitivity"]
            + F.col("business_criticality_factor") * weights["business_criticality"]
        )

        return df.withColumn("priority_score", priority_score_expr)

    def _assign_priority_levels(self, df: DataFrame) -> DataFrame:
        """Assign priority levels based on priority score."""
        thresholds = self.priority_thresholds

        return df.withColumn(
            "alert_priority",
            F.when(
                F.col("priority_score") >= thresholds[AlertPriority.CRITICAL],
                "CRITICAL",
            )
            .when(F.col("priority_score") >= thresholds[AlertPriority.HIGH], "HIGH")
            .when(F.col("priority_score") >= thresholds[AlertPriority.MEDIUM], "MEDIUM")
            .otherwise("LOW"),
        )

    def _add_recommended_actions(self, df: DataFrame) -> DataFrame:
        """Add recommended investigation actions based on alert characteristics."""
        return df.withColumn(
            "recommended_actions",
            F.when(
                F.col("alert_priority") == "CRITICAL",
                F.array(
                    F.lit("IMMEDIATE_REVIEW"),
                    F.lit("BLOCK_CUSTOMER"),
                    F.lit("ESCALATE_TO_SENIOR"),
                ),
            )
            .when(
                F.col("alert_priority") == "HIGH",
                F.array(
                    F.lit("REVIEW_WITHIN_1_HOUR"),
                    F.lit("VERIFY_CUSTOMER_CONTACT"),
                    F.lit("CHECK_RECENT_ACTIVITY"),
                ),
            )
            .when(
                F.col("alert_priority") == "MEDIUM",
                F.array(F.lit("REVIEW_WITHIN_4_HOURS"), F.lit("STANDARD_VERIFICATION")),
            )
            .otherwise(
                F.array(F.lit("REVIEW_WITHIN_24_HOURS"), F.lit("BATCH_PROCESS"))
            ),
        )

    def _estimate_investigation_time(self, df: DataFrame) -> DataFrame:
        """Estimate investigation time in minutes based on alert complexity."""
        return df.withColumn(
            "estimated_investigation_time_minutes",
            F.when(F.col("alert_priority") == "CRITICAL", 45)
            .when(F.col("alert_priority") == "HIGH", 30)
            .when(F.col("alert_priority") == "MEDIUM", 15)
            .otherwise(5),
        ).withColumn(
            "estimated_investigation_time_minutes",
            # Adjust based on pattern complexity
            F.col("estimated_investigation_time_minutes")
            + (F.size("triggered_rules") * 5),
        )

    def get_priority_queue(
        self,
        prioritized_alerts_df: DataFrame,
        max_alerts: int = 100,
        priority_filter: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Get prioritized alert queue for investigation.

        Args:
            prioritized_alerts_df: DataFrame with prioritized alerts
            max_alerts: Maximum number of alerts to return
            priority_filter: Optional list of priorities to include

        Returns:
            DataFrame with top priority alerts
        """
        result_df = prioritized_alerts_df

        # Filter by priority if specified
        if priority_filter:
            result_df = result_df.filter(F.col("alert_priority").isin(priority_filter))

        # Order by priority score and transaction recency
        result_df = result_df.orderBy(
            F.col("priority_score").desc(),
            F.col("time_sensitivity_factor").desc(),
            F.col("timestamp").desc(),
        )

        # Limit results
        result_df = result_df.limit(max_alerts)

        # Add queue position
        window_spec = Window.orderBy(F.col("priority_score").desc())
        result_df = result_df.withColumn(
            "queue_position", F.row_number().over(window_spec)
        )

        return result_df

    def get_alert_summary_metrics(self, prioritized_alerts_df: DataFrame) -> Dict:
        """
        Get summary metrics for prioritized alerts.

        Args:
            prioritized_alerts_df: DataFrame with prioritized alerts

        Returns:
            Dictionary with alert summary metrics
        """
        # Count by priority
        priority_counts = (
            prioritized_alerts_df.groupBy("alert_priority")
            .agg(
                F.count("*").alias("count"),
                F.sum("price").alias("total_amount"),
                F.avg("priority_score").alias("avg_priority_score"),
            )
            .collect()
        )

        # Overall statistics
        overall_stats = prioritized_alerts_df.agg(
            F.count("*").alias("total_alerts"),
            F.sum("price").alias("total_financial_exposure"),
            F.avg("priority_score").alias("avg_priority_score"),
            F.sum("estimated_investigation_time_minutes").alias(
                "total_investigation_time"
            ),
        ).collect()[0]

        # Top rules triggered
        top_rules = (
            prioritized_alerts_df.select(F.explode("triggered_rules").alias("rule"))
            .groupBy("rule")
            .agg(F.count("*").alias("count"))
            .orderBy(F.col("count").desc())
            .limit(10)
            .collect()
        )

        return {
            "priority_breakdown": {
                row["alert_priority"]: {
                    "count": row["count"],
                    "total_amount": float(row["total_amount"]),
                    "avg_priority_score": float(row["avg_priority_score"]),
                }
                for row in priority_counts
            },
            "overall_stats": {
                "total_alerts": overall_stats["total_alerts"],
                "total_financial_exposure": float(
                    overall_stats["total_financial_exposure"]
                ),
                "avg_priority_score": float(overall_stats["avg_priority_score"]),
                "total_investigation_time_hours": overall_stats[
                    "total_investigation_time"
                ]
                / 60,
            },
            "top_triggered_rules": [
                {"rule": row["rule"], "count": row["count"]} for row in top_rules
            ],
        }

    def update_priority_weights(self, new_weights: Dict[str, float]) -> None:
        """
        Update priority calculation weights.

        Args:
            new_weights: Dictionary with new weights
        """
        for key, value in new_weights.items():
            if key in self.priority_weights:
                self.priority_weights[key] = value

    def update_priority_thresholds(
        self, new_thresholds: Dict[AlertPriority, float]
    ) -> None:
        """
        Update priority level thresholds.

        Args:
            new_thresholds: Dictionary with new thresholds
        """
        for priority, threshold in new_thresholds.items():
            if priority in self.priority_thresholds:
                self.priority_thresholds[priority] = threshold

    def save_prioritized_alerts(
        self, prioritized_alerts_df: DataFrame, output_path: str
    ) -> None:
        """
        Save prioritized alerts to storage.

        Args:
            prioritized_alerts_df: DataFrame with prioritized alerts
            output_path: Output path for saving alerts
        """
        prioritized_alerts_df.select(
            "transaction_id",
            "customer_id",
            "merchant_id",
            "alert_priority",
            "priority_score",
            "price",
            "triggered_rules",
            "recommended_actions",
            "estimated_investigation_time_minutes",
            "timestamp",
        ).write.mode("overwrite").parquet(output_path)

"""
Data Quality Engine for orchestrating streaming data quality checks.

This module provides the main orchestration engine that coordinates
all data quality components for real-time streaming data processing.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, count, current_timestamp, lit, struct
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_json, when

from src.utils.logger import get_logger

from .anomaly_detector import StreamingAnomalyDetector
from .completeness_checker import CompletenessChecker
from .profiler import StreamingDataProfiler
from .validator import StreamingDataValidator


class QualityLevel(Enum):
    """Data quality levels."""

    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


@dataclass
class QualityReport:
    """Data quality assessment report."""

    timestamp: datetime
    total_records: int
    valid_records: int
    invalid_records: int
    completeness_score: float
    anomaly_count: int
    quality_level: QualityLevel
    validation_errors: List[Dict[str, Any]]
    anomalies: List[Dict[str, Any]]
    profile_summary: Dict[str, Any]
    recommendations: List[str]


class DataQualityEngine:
    """
    Comprehensive data quality engine for streaming data.

    Orchestrates validation, anomaly detection, completeness checking,
    and data profiling for real-time streaming data.
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize data quality engine.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary for quality settings
        """
        self.spark = spark
        self.logger = get_logger(__name__)
        self.config = config or {}

        # Initialize quality components
        self.validator = StreamingDataValidator(
            spark, self.config.get("validation", {})
        )
        self.anomaly_detector = StreamingAnomalyDetector(
            spark, self.config.get("anomaly", {})
        )
        self.completeness_checker = CompletenessChecker(
            spark, self.config.get("completeness", {})
        )
        self.profiler = StreamingDataProfiler(spark, self.config.get("profiling", {}))

        # Quality thresholds
        self.quality_thresholds = {
            "excellent": {
                "validation_rate": 0.99,
                "completeness": 0.98,
                "anomaly_rate": 0.01,
            },
            "good": {
                "validation_rate": 0.95,
                "completeness": 0.95,
                "anomaly_rate": 0.05,
            },
            "fair": {
                "validation_rate": 0.90,
                "completeness": 0.90,
                "anomaly_rate": 0.10,
            },
            "poor": {
                "validation_rate": 0.80,
                "completeness": 0.80,
                "anomaly_rate": 0.20,
            },
        }

    def assess_data_quality(
        self, df: DataFrame, stream_type: str = "unknown", enable_profiling: bool = True
    ) -> Tuple[DataFrame, QualityReport]:
        """
        Perform comprehensive data quality assessment on streaming data.

        Args:
            df: Input DataFrame to assess
            stream_type: Type of stream (transaction, user_behavior, etc.)
            enable_profiling: Whether to perform data profiling

        Returns:
            Tuple of (enriched_dataframe_with_quality_flags, quality_report)
        """
        try:
            self.logger.info(
                f"Starting data quality assessment for {stream_type} stream"
            )

            # Step 1: Data validation
            validated_df, validation_results = self.validator.validate_stream(
                df, stream_type
            )

            # Step 2: Anomaly detection
            anomaly_df, anomaly_results = self.anomaly_detector.detect_anomalies(
                validated_df, stream_type
            )

            # Step 3: Completeness checking
            (
                completeness_df,
                completeness_results,
            ) = self.completeness_checker.check_completeness(anomaly_df)

            # Step 4: Data profiling (optional)
            profile_results = {}
            if enable_profiling:
                profile_results = self.profiler.profile_stream(
                    completeness_df, stream_type
                )

            # Step 5: Aggregate quality flags
            quality_df = self._add_quality_summary(completeness_df)

            # Step 6: Generate quality report
            quality_report = self._generate_quality_report(
                quality_df,
                validation_results,
                anomaly_results,
                completeness_results,
                profile_results,
                stream_type,
            )

            self.logger.info(
                f"Data quality assessment completed: {quality_report.quality_level.value}"
            )
            return quality_df, quality_report

        except Exception as e:
            self.logger.error(f"Error in data quality assessment: {e}")
            raise

    def _add_quality_summary(self, df: DataFrame) -> DataFrame:
        """Add overall quality summary flags to DataFrame."""
        return (
            df.withColumn(
                "data_quality_score",
                when(
                    col("validation_passed")
                    & col("completeness_passed")
                    & ~col("is_anomaly"),
                    lit(100),
                )
                .when(
                    col("validation_passed")
                    & col("completeness_passed")
                    & col("is_anomaly"),
                    lit(75),
                )
                .when(col("validation_passed") & ~col("completeness_passed"), lit(50))
                .when(~col("validation_passed"), lit(25))
                .otherwise(lit(0)),
            )
            .withColumn(
                "quality_level",
                when(col("data_quality_score") >= 90, lit("excellent"))
                .when(col("data_quality_score") >= 75, lit("good"))
                .when(col("data_quality_score") >= 50, lit("fair"))
                .when(col("data_quality_score") >= 25, lit("poor"))
                .otherwise(lit("critical")),
            )
            .withColumn("quality_assessment_timestamp", current_timestamp())
        )

    def _generate_quality_report(
        self,
        df: DataFrame,
        validation_results: Dict[str, Any],
        anomaly_results: Dict[str, Any],
        completeness_results: Dict[str, Any],
        profile_results: Dict[str, Any],
        stream_type: str,
    ) -> QualityReport:
        """Generate comprehensive quality report."""

        # Calculate metrics
        total_records = df.count()

        # Validation metrics
        valid_records = validation_results.get("valid_count", 0)
        invalid_records = validation_results.get("invalid_count", 0)
        validation_rate = valid_records / max(total_records, 1)

        # Completeness metrics
        completeness_score = completeness_results.get("overall_completeness", 0.0)

        # Anomaly metrics
        anomaly_count = anomaly_results.get("anomaly_count", 0)
        anomaly_rate = anomaly_count / max(total_records, 1)

        # Determine overall quality level
        quality_level = self._determine_quality_level(
            validation_rate, completeness_score, anomaly_rate
        )

        # Generate recommendations
        recommendations = self._generate_recommendations(
            validation_rate, completeness_score, anomaly_rate, stream_type
        )

        return QualityReport(
            timestamp=datetime.now(),
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            completeness_score=completeness_score,
            anomaly_count=anomaly_count,
            quality_level=quality_level,
            validation_errors=validation_results.get("errors", []),
            anomalies=anomaly_results.get("anomalies", []),
            profile_summary=profile_results,
            recommendations=recommendations,
        )

    def _determine_quality_level(
        self, validation_rate: float, completeness_score: float, anomaly_rate: float
    ) -> QualityLevel:
        """Determine overall quality level based on metrics."""

        for level, thresholds in self.quality_thresholds.items():
            if (
                validation_rate >= thresholds["validation_rate"]
                and completeness_score >= thresholds["completeness"]
                and anomaly_rate <= thresholds["anomaly_rate"]
            ):
                return QualityLevel(level)

        return QualityLevel.CRITICAL

    def _generate_recommendations(
        self,
        validation_rate: float,
        completeness_score: float,
        anomaly_rate: float,
        stream_type: str,
    ) -> List[str]:
        """Generate actionable recommendations based on quality metrics."""

        recommendations = []

        if validation_rate < 0.95:
            recommendations.append(
                f"Validation rate ({validation_rate:.2%}) is below target. "
                "Review data source quality and validation rules."
            )

        if completeness_score < 0.95:
            recommendations.append(
                f"Completeness score ({completeness_score:.2%}) is below target. "
                "Investigate data pipeline for missing values."
            )

        if anomaly_rate > 0.05:
            recommendations.append(
                f"Anomaly rate ({anomaly_rate:.2%}) is above threshold. "
                "Review anomaly patterns and adjust detection rules."
            )

        if stream_type == "transaction" and validation_rate < 0.99:
            recommendations.append(
                "Transaction streams require higher validation rates. "
                "Consider stricter validation rules for financial data."
            )

        if not recommendations:
            recommendations.append("Data quality is within acceptable thresholds.")

        return recommendations

    def create_quality_monitoring_stream(self, df: DataFrame) -> DataFrame:
        """
        Create a monitoring stream for continuous quality assessment.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with quality monitoring metrics
        """
        return (
            df.withColumn("quality_check_timestamp", current_timestamp())
            .withColumn(
                "quality_summary",
                struct(
                    col("data_quality_score"),
                    col("quality_level"),
                    col("validation_passed"),
                    col("completeness_passed"),
                    col("is_anomaly"),
                ),
            )
            .select(
                "quality_check_timestamp",
                "quality_summary",
                col("data_quality_score"),
                col("quality_level"),
            )
        )

    def get_quality_dashboard_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate metrics for quality monitoring dashboard.

        Args:
            df: DataFrame with quality flags

        Returns:
            Dictionary with dashboard metrics
        """
        try:
            # Aggregate quality metrics
            quality_metrics = (
                df.groupBy("quality_level")
                .agg(
                    count("*").alias("record_count"),
                    (count("*") * 100.0 / df.count()).alias("percentage"),
                )
                .collect()
            )

            # Calculate overall metrics
            total_records = df.count()
            avg_quality_score = (
                df.agg({"data_quality_score": "avg"}).collect()[0][0] or 0
            )

            validation_passed_count = df.filter(col("validation_passed")).count()
            anomaly_count = df.filter(col("is_anomaly")).count()

            return {
                "total_records": total_records,
                "average_quality_score": round(avg_quality_score, 2),
                "validation_pass_rate": round(
                    validation_passed_count / max(total_records, 1) * 100, 2
                ),
                "anomaly_rate": round(anomaly_count / max(total_records, 1) * 100, 2),
                "quality_distribution": {
                    row["quality_level"]: {
                        "count": row["record_count"],
                        "percentage": round(row["percentage"], 2),
                    }
                    for row in quality_metrics
                },
            }

        except Exception as e:
            self.logger.error(f"Error generating dashboard metrics: {e}")
            return {}

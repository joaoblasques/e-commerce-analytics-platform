"""
Model performance monitoring and feedback system for ML fraud detection models.

This module provides comprehensive monitoring of ML model performance, data drift detection,
and feedback loop implementation for continuous model improvement.
"""

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class MonitoringMetric(Enum):
    """Types of monitoring metrics."""

    ACCURACY = "accuracy"
    PRECISION = "precision"
    RECALL = "recall"
    F1_SCORE = "f1_score"
    AUC_ROC = "auc_roc"
    FALSE_POSITIVE_RATE = "false_positive_rate"
    FALSE_NEGATIVE_RATE = "false_negative_rate"
    PREDICTION_LATENCY = "prediction_latency"
    THROUGHPUT = "throughput"
    DATA_DRIFT = "data_drift"
    CONCEPT_DRIFT = "concept_drift"


@dataclass
class ModelPerformanceMetrics:
    """Model performance metrics at a point in time."""

    timestamp: datetime
    model_name: str
    model_version: str
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    auc_roc: float
    false_positive_rate: float
    false_negative_rate: float
    confusion_matrix: List[List[int]]
    sample_size: int
    prediction_latency_ms: float
    throughput_per_second: float


@dataclass
class DataDriftMetrics:
    """Data drift detection metrics."""

    timestamp: datetime
    feature_name: str
    drift_score: float
    p_value: float
    is_drift_detected: bool
    drift_magnitude: str  # "low", "medium", "high"
    reference_distribution_stats: Dict[str, float]
    current_distribution_stats: Dict[str, float]


@dataclass
class ModelAlert:
    """Model monitoring alert."""

    alert_id: str
    timestamp: datetime
    model_name: str
    model_version: str
    metric_type: MonitoringMetric
    severity: AlertSeverity
    message: str
    current_value: float
    threshold: float
    recommendation: str


class ModelPerformanceMonitor:
    """
    Comprehensive model performance monitoring system.

    Features:
    - Real-time performance tracking
    - Data drift detection
    - Concept drift detection
    - Automated alerting and recommendations
    - Performance trend analysis
    - Model comparison and A/B testing analysis
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """Initialize the model performance monitor."""
        self.spark = spark
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Performance history storage
        self.performance_history: Dict[str, deque] = {}
        self.drift_history: Dict[str, deque] = {}
        self.alerts: List[ModelAlert] = []

        # Reference data for drift detection
        self.reference_data: Dict[str, DataFrame] = {}
        self.feature_stats: Dict[str, Dict] = {}

        # Monitoring state
        self.monitoring_active = False
        self.monitoring_thread = None

        # Alert callbacks
        self.alert_callbacks: List[callable] = []

    def _get_default_config(self) -> Dict:
        """Get default monitoring configuration."""
        return {
            "performance_thresholds": {
                "accuracy_min": 0.85,
                "precision_min": 0.80,
                "recall_min": 0.75,
                "f1_score_min": 0.77,
                "auc_roc_min": 0.80,
                "false_positive_rate_max": 0.10,
                "false_negative_rate_max": 0.15,
                "prediction_latency_max_ms": 1000,
                "throughput_min_per_second": 100,
            },
            "drift_detection": {
                "drift_score_threshold": 0.1,
                "p_value_threshold": 0.05,
                "reference_window_days": 30,
                "monitoring_window_days": 7,
                "min_samples_for_drift": 1000,
            },
            "monitoring": {
                "evaluation_interval_minutes": 60,
                "history_retention_days": 90,
                "batch_size": 10000,
                "enable_real_time_monitoring": True,
            },
            "alerting": {
                "alert_cooldown_minutes": 30,
                "max_alerts_per_hour": 10,
                "enable_email_alerts": False,
                "enable_slack_alerts": False,
            },
        }

    def set_reference_data(self, model_name: str, reference_df: DataFrame) -> None:
        """
        Set reference data for drift detection.

        Args:
            model_name: Name of the model
            reference_df: Reference DataFrame for baseline comparison
        """
        self.logger.info(f"Setting reference data for model {model_name}")

        # Store reference data
        self.reference_data[model_name] = reference_df

        # Calculate feature statistics for drift detection
        feature_stats = {}
        numeric_columns = [
            field.name
            for field in reference_df.schema.fields
            if field.dataType in [DoubleType(), IntegerType()]
        ]

        for col in numeric_columns:
            if col in reference_df.columns:
                stats = reference_df.select(
                    F.mean(col).alias("mean"),
                    F.stddev(col).alias("stddev"),
                    F.min(col).alias("min"),
                    F.max(col).alias("max"),
                    F.expr(f"percentile_approx({col}, 0.25)").alias("q25"),
                    F.expr(f"percentile_approx({col}, 0.5)").alias("median"),
                    F.expr(f"percentile_approx({col}, 0.75)").alias("q75"),
                ).collect()[0]

                feature_stats[col] = {
                    "mean": float(stats["mean"]) if stats["mean"] else 0.0,
                    "stddev": float(stats["stddev"]) if stats["stddev"] else 0.0,
                    "min": float(stats["min"]) if stats["min"] else 0.0,
                    "max": float(stats["max"]) if stats["max"] else 0.0,
                    "q25": float(stats["q25"]) if stats["q25"] else 0.0,
                    "median": float(stats["median"]) if stats["median"] else 0.0,
                    "q75": float(stats["q75"]) if stats["q75"] else 0.0,
                }

        self.feature_stats[model_name] = feature_stats
        self.logger.info(f"Calculated statistics for {len(feature_stats)} features")

    def evaluate_model_performance(
        self,
        model_name: str,
        model_version: str,
        predictions_df: DataFrame,
        actuals_df: DataFrame,
    ) -> ModelPerformanceMetrics:
        """
        Evaluate model performance against actual outcomes.

        Args:
            model_name: Name of the model
            model_version: Version of the model
            predictions_df: DataFrame with model predictions
            actuals_df: DataFrame with actual fraud outcomes

        Returns:
            Model performance metrics
        """
        self.logger.info(
            f"Evaluating performance for {model_name} version {model_version}"
        )

        # Join predictions with actuals
        evaluation_df = predictions_df.join(
            actuals_df.select("transaction_id", "actual_fraud"),
            "transaction_id",
            "inner",
        )

        # Calculate confusion matrix components
        tp = evaluation_df.filter(
            (F.col("ml_fraud_prediction") == 1) & (F.col("actual_fraud") == 1)
        ).count()
        fp = evaluation_df.filter(
            (F.col("ml_fraud_prediction") == 1) & (F.col("actual_fraud") == 0)
        ).count()
        tn = evaluation_df.filter(
            (F.col("ml_fraud_prediction") == 0) & (F.col("actual_fraud") == 0)
        ).count()
        fn = evaluation_df.filter(
            (F.col("ml_fraud_prediction") == 0) & (F.col("actual_fraud") == 1)
        ).count()

        total_samples = tp + fp + tn + fn

        if total_samples == 0:
            raise ValueError("No samples found for evaluation")

        # Calculate metrics
        accuracy = (tp + tn) / total_samples if total_samples > 0 else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1_score = (
            2 * (precision * recall) / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        false_positive_rate = fp / (fp + tn) if (fp + tn) > 0 else 0.0
        false_negative_rate = fn / (fn + tp) if (fn + tp) > 0 else 0.0

        # Calculate AUC-ROC (simplified approximation)
        auc_roc = self._calculate_auc_roc(evaluation_df)

        # Performance timing (would be measured during actual prediction)
        avg_prediction_latency = (
            evaluation_df.agg(
                F.avg("prediction_time_ms").alias("avg_latency")
            ).collect()[0]["avg_latency"]
            if "prediction_time_ms" in evaluation_df.columns
            else 100.0
        )

        # Create metrics object
        metrics = ModelPerformanceMetrics(
            timestamp=datetime.now(),
            model_name=model_name,
            model_version=model_version,
            accuracy=accuracy,
            precision=precision,
            recall=recall,
            f1_score=f1_score,
            auc_roc=auc_roc,
            false_positive_rate=false_positive_rate,
            false_negative_rate=false_negative_rate,
            confusion_matrix=[[tn, fp], [fn, tp]],
            sample_size=total_samples,
            prediction_latency_ms=avg_prediction_latency,
            throughput_per_second=0.0,  # Would be calculated separately
        )

        # Store in history
        model_key = f"{model_name}_{model_version}"
        if model_key not in self.performance_history:
            self.performance_history[model_key] = deque(maxlen=1000)

        self.performance_history[model_key].append(metrics)

        # Check for performance degradation
        self._check_performance_alerts(metrics)

        self.logger.info(
            f"Performance evaluation completed. Accuracy: {accuracy:.3f}, F1: {f1_score:.3f}"
        )
        return metrics

    def _calculate_auc_roc(self, evaluation_df: DataFrame) -> float:
        """Calculate AUC-ROC score (simplified implementation)."""
        try:
            # Get fraud probabilities and actual labels
            prob_actual = evaluation_df.select(
                "fraud_probability", "actual_fraud"
            ).collect()

            if len(prob_actual) < 2:
                return 0.5

            # Convert to lists for calculation
            probabilities = [row["fraud_probability"] for row in prob_actual]
            actuals = [row["actual_fraud"] for row in prob_actual]

            # Simple AUC calculation (this is a simplified version)
            # In production, would use proper AUC calculation
            from sklearn.metrics import roc_auc_score

            return float(roc_auc_score(actuals, probabilities))

        except Exception as e:
            self.logger.warning(f"Could not calculate AUC-ROC: {e}")
            return 0.5

    def detect_data_drift(
        self, model_name: str, current_df: DataFrame
    ) -> List[DataDriftMetrics]:
        """
        Detect data drift by comparing current data with reference data.

        Args:
            model_name: Name of the model
            current_df: Current data to compare against reference

        Returns:
            List of drift metrics for each feature
        """
        if model_name not in self.reference_data:
            self.logger.warning(f"No reference data found for model {model_name}")
            return []

        self.logger.info(f"Detecting data drift for model {model_name}")

        reference_df = self.reference_data[model_name]
        drift_results = []

        # Check numeric features
        numeric_columns = [
            field.name
            for field in current_df.schema.fields
            if field.dataType in [DoubleType(), IntegerType()]
        ]

        for column in numeric_columns:
            if column in reference_df.columns and column in current_df.columns:
                drift_metric = self._calculate_feature_drift(
                    column, reference_df, current_df, model_name
                )
                if drift_metric:
                    drift_results.append(drift_metric)

        # Store drift history
        if model_name not in self.drift_history:
            self.drift_history[model_name] = deque(maxlen=1000)

        for drift_metric in drift_results:
            self.drift_history[model_name].append(drift_metric)

            # Check for drift alerts
            if drift_metric.is_drift_detected:
                self._create_drift_alert(drift_metric)

        self.logger.info(
            f"Data drift detection completed. Found {len(drift_results)} features analyzed"
        )
        return drift_results

    def _calculate_feature_drift(
        self,
        feature_name: str,
        reference_df: DataFrame,
        current_df: DataFrame,
        model_name: str,
    ) -> Optional[DataDriftMetrics]:
        """Calculate drift for a specific feature using statistical tests."""
        try:
            # Get reference statistics
            ref_stats = self.feature_stats.get(model_name, {}).get(feature_name, {})
            if not ref_stats:
                return None

            # Calculate current statistics
            current_stats = current_df.select(
                F.mean(feature_name).alias("mean"),
                F.stddev(feature_name).alias("stddev"),
                F.min(feature_name).alias("min"),
                F.max(feature_name).alias("max"),
                F.expr(f"percentile_approx({feature_name}, 0.5)").alias("median"),
            ).collect()[0]

            current_dict = {
                "mean": float(current_stats["mean"]) if current_stats["mean"] else 0.0,
                "stddev": float(current_stats["stddev"])
                if current_stats["stddev"]
                else 0.0,
                "min": float(current_stats["min"]) if current_stats["min"] else 0.0,
                "max": float(current_stats["max"]) if current_stats["max"] else 0.0,
                "median": float(current_stats["median"])
                if current_stats["median"]
                else 0.0,
            }

            # Calculate drift score (simplified KS-test approximation)
            drift_score = self._calculate_ks_drift_score(ref_stats, current_dict)

            # Determine drift magnitude
            drift_magnitude = "low"
            if drift_score > 0.3:
                drift_magnitude = "high"
            elif drift_score > 0.1:
                drift_magnitude = "medium"

            # Check if drift is significant
            is_drift_detected = (
                drift_score > self.config["drift_detection"]["drift_score_threshold"]
            )

            return DataDriftMetrics(
                timestamp=datetime.now(),
                feature_name=feature_name,
                drift_score=drift_score,
                p_value=1.0 - drift_score,  # Simplified p-value
                is_drift_detected=is_drift_detected,
                drift_magnitude=drift_magnitude,
                reference_distribution_stats=ref_stats,
                current_distribution_stats=current_dict,
            )

        except Exception as e:
            self.logger.error(
                f"Error calculating drift for feature {feature_name}: {e}"
            )
            return None

    def _calculate_ks_drift_score(self, ref_stats: Dict, current_stats: Dict) -> float:
        """Calculate a simplified drift score based on statistical differences."""
        # Compare means (normalized by standard deviation)
        ref_mean = ref_stats.get("mean", 0.0)
        current_mean = current_stats.get("mean", 0.0)
        ref_std = max(ref_stats.get("stddev", 1.0), 0.001)  # Avoid division by zero

        mean_diff = abs(ref_mean - current_mean) / ref_std

        # Compare standard deviations
        current_std = current_stats.get("stddev", 1.0)
        std_ratio = min(ref_std, current_std) / max(ref_std, current_std, 0.001)
        std_diff = 1.0 - std_ratio

        # Compare medians
        ref_median = ref_stats.get("median", 0.0)
        current_median = current_stats.get("median", 0.0)
        median_diff = abs(ref_median - current_median) / max(abs(ref_median), 0.001)

        # Combine into overall drift score
        drift_score = mean_diff * 0.5 + std_diff * 0.3 + median_diff * 0.2

        return min(drift_score, 1.0)  # Cap at 1.0

    def _check_performance_alerts(self, metrics: ModelPerformanceMetrics) -> None:
        """Check if performance metrics trigger alerts."""
        thresholds = self.config["performance_thresholds"]

        alerts_to_create = []

        # Check accuracy
        if metrics.accuracy < thresholds["accuracy_min"]:
            alerts_to_create.append(
                (
                    MonitoringMetric.ACCURACY,
                    metrics.accuracy,
                    thresholds["accuracy_min"],
                    f"Model accuracy {metrics.accuracy:.3f} below threshold {thresholds['accuracy_min']:.3f}",
                    AlertSeverity.WARNING,
                )
            )

        # Check precision
        if metrics.precision < thresholds["precision_min"]:
            alerts_to_create.append(
                (
                    MonitoringMetric.PRECISION,
                    metrics.precision,
                    thresholds["precision_min"],
                    f"Model precision {metrics.precision:.3f} below threshold {thresholds['precision_min']:.3f}",
                    AlertSeverity.WARNING,
                )
            )

        # Check recall
        if metrics.recall < thresholds["recall_min"]:
            alerts_to_create.append(
                (
                    MonitoringMetric.RECALL,
                    metrics.recall,
                    thresholds["recall_min"],
                    f"Model recall {metrics.recall:.3f} below threshold {thresholds['recall_min']:.3f}",
                    AlertSeverity.WARNING,
                )
            )

        # Check false positive rate
        if metrics.false_positive_rate > thresholds["false_positive_rate_max"]:
            alerts_to_create.append(
                (
                    MonitoringMetric.FALSE_POSITIVE_RATE,
                    metrics.false_positive_rate,
                    thresholds["false_positive_rate_max"],
                    f"False positive rate {metrics.false_positive_rate:.3f} above threshold",
                    AlertSeverity.CRITICAL,
                )
            )

        # Create alerts
        for (
            metric_type,
            current_value,
            threshold,
            message,
            severity,
        ) in alerts_to_create:
            self._create_alert(
                metrics.model_name,
                metrics.model_version,
                metric_type,
                severity,
                message,
                current_value,
                threshold,
            )

    def _create_alert(
        self,
        model_name: str,
        model_version: str,
        metric_type: MonitoringMetric,
        severity: AlertSeverity,
        message: str,
        current_value: float,
        threshold: float,
    ) -> None:
        """Create a monitoring alert."""
        alert = ModelAlert(
            alert_id=f"alert_{int(time.time() * 1000)}",
            timestamp=datetime.now(),
            model_name=model_name,
            model_version=model_version,
            metric_type=metric_type,
            severity=severity,
            message=message,
            current_value=current_value,
            threshold=threshold,
            recommendation=self._get_alert_recommendation(metric_type, severity),
        )

        self.alerts.append(alert)

        # Trigger alert callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                self.logger.error(f"Alert callback failed: {e}")

        self.logger.warning(f"Alert created: {alert.message}")

    def _create_drift_alert(self, drift_metric: DataDriftMetrics) -> None:
        """Create an alert for data drift detection."""
        severity = AlertSeverity.WARNING
        if drift_metric.drift_magnitude == "high":
            severity = AlertSeverity.CRITICAL

        message = (
            f"Data drift detected for feature {drift_metric.feature_name}. "
            f"Drift score: {drift_metric.drift_score:.3f} ({drift_metric.drift_magnitude})"
        )

        alert = ModelAlert(
            alert_id=f"drift_{int(time.time() * 1000)}",
            timestamp=drift_metric.timestamp,
            model_name="drift_monitor",
            model_version="1.0",
            metric_type=MonitoringMetric.DATA_DRIFT,
            severity=severity,
            message=message,
            current_value=drift_metric.drift_score,
            threshold=self.config["drift_detection"]["drift_score_threshold"],
            recommendation="Consider retraining the model with recent data",
        )

        self.alerts.append(alert)

        # Trigger alert callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                self.logger.error(f"Alert callback failed: {e}")

    def _get_alert_recommendation(
        self, metric_type: MonitoringMetric, severity: AlertSeverity
    ) -> str:
        """Get recommendation for alert type."""
        recommendations = {
            MonitoringMetric.ACCURACY: "Consider retraining model or reviewing feature engineering",
            MonitoringMetric.PRECISION: "Review model threshold or add more training data for positive class",
            MonitoringMetric.RECALL: "Lower prediction threshold or add more diverse training examples",
            MonitoringMetric.FALSE_POSITIVE_RATE: "Increase prediction threshold or review feature quality",
            MonitoringMetric.FALSE_NEGATIVE_RATE: "Lower prediction threshold or improve model sensitivity",
            MonitoringMetric.DATA_DRIFT: "Consider retraining model with recent data",
            MonitoringMetric.PREDICTION_LATENCY: "Optimize model inference or increase computing resources",
        }
        return recommendations.get(
            metric_type, "Review model performance and consider retraining"
        )

    def get_performance_trends(
        self, model_name: str, model_version: str, days: int = 30
    ) -> Dict[str, List[float]]:
        """
        Get performance trends over time.

        Args:
            model_name: Name of the model
            model_version: Version of the model
            days: Number of days to include in trend

        Returns:
            Dictionary with metric trends
        """
        model_key = f"{model_name}_{model_version}"

        if model_key not in self.performance_history:
            return {}

        # Filter by date range
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_metrics = [
            m for m in self.performance_history[model_key] if m.timestamp >= cutoff_date
        ]

        if not recent_metrics:
            return {}

        # Extract trends
        trends = {
            "timestamps": [m.timestamp.isoformat() for m in recent_metrics],
            "accuracy": [m.accuracy for m in recent_metrics],
            "precision": [m.precision for m in recent_metrics],
            "recall": [m.recall for m in recent_metrics],
            "f1_score": [m.f1_score for m in recent_metrics],
            "auc_roc": [m.auc_roc for m in recent_metrics],
            "false_positive_rate": [m.false_positive_rate for m in recent_metrics],
            "prediction_latency_ms": [m.prediction_latency_ms for m in recent_metrics],
        }

        return trends

    def add_alert_callback(self, callback: callable) -> None:
        """Add a callback function to be called when alerts are created."""
        self.alert_callbacks.append(callback)

    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get comprehensive monitoring summary."""
        return {
            "monitoring_status": "active" if self.monitoring_active else "inactive",
            "models_monitored": list(self.performance_history.keys()),
            "total_alerts": len(self.alerts),
            "recent_alerts": len(
                [
                    a
                    for a in self.alerts
                    if a.timestamp >= datetime.now() - timedelta(hours=24)
                ]
            ),
            "drift_features_monitored": sum(
                len(history) for history in self.drift_history.values()
            ),
            "last_evaluation_time": max(
                [
                    max(history, key=lambda x: x.timestamp).timestamp
                    for history in self.performance_history.values()
                ],
                default=datetime.min,
            ).isoformat()
            if self.performance_history
            else None,
        }

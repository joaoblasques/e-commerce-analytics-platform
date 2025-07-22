"""
Model serving pipeline for real-time fraud detection using ML models.

This module provides infrastructure for serving trained ML fraud detection models
in production with real-time prediction capabilities, model versioning, and A/B testing.
"""

import json
import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .ml_models import MLFraudModelTrainer, ModelStatus, ModelType


class ServingStatus(Enum):
    """Model serving status."""

    INITIALIZING = "initializing"
    ACTIVE = "active"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    ERROR = "error"


@dataclass
class ModelVersion:
    """Model version information."""

    version: str
    model_name: str
    model_path: str
    deployed_at: datetime
    status: ModelStatus
    performance_metrics: Dict[str, float]
    traffic_percentage: float = 0.0
    prediction_count: int = 0
    error_count: int = 0


@dataclass
class PredictionRequest:
    """Real-time prediction request."""

    request_id: str
    transaction_data: Dict[str, Any]
    timestamp: datetime
    model_version: Optional[str] = None


@dataclass
class PredictionResponse:
    """Real-time prediction response."""

    request_id: str
    fraud_probability: float
    prediction: int
    model_version: str
    prediction_time_ms: float
    feature_values: Dict[str, float]
    error: Optional[str] = None


class ModelServingPipeline:
    """
    Real-time model serving pipeline for fraud detection.

    Features:
    - Real-time ML model predictions
    - A/B testing with traffic splitting
    - Model versioning and rollback
    - Performance monitoring and alerting
    - Fallback to rule-based system
    - Feature store integration
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """Initialize the model serving pipeline."""
        self.spark = spark
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Model management
        self.model_versions: Dict[str, ModelVersion] = {}
        self.active_models: Dict[str, Any] = {}
        self.model_trainer = MLFraudModelTrainer(spark)

        # Serving infrastructure
        self.serving_status = ServingStatus.INITIALIZING
        self.prediction_queue = queue.Queue(maxsize=10000)
        self.response_callbacks: Dict[str, Callable] = {}

        # Performance monitoring
        self.metrics = {
            "total_predictions": 0,
            "successful_predictions": 0,
            "failed_predictions": 0,
            "average_prediction_time_ms": 0.0,
            "throughput_per_second": 0.0,
        }

        # A/B testing
        self.traffic_split_config = {}

        # Background threads
        self._prediction_thread = None
        self._monitoring_thread = None
        self._running = False

    def _get_default_config(self) -> Dict:
        """Get default configuration for model serving."""
        return {
            "serving": {
                "max_batch_size": 1000,
                "batch_timeout_ms": 100,
                "max_queue_size": 10000,
                "prediction_timeout_ms": 5000,
                "fallback_enabled": True,
            },
            "monitoring": {
                "metrics_update_interval_seconds": 10,
                "performance_threshold_ms": 1000,
                "error_rate_threshold": 0.05,
                "throughput_threshold_per_second": 100,
            },
            "models": {
                "model_store_path": "data/models/fraud_detection",
                "max_versions_per_model": 5,
                "default_traffic_split": {"champion": 90, "challenger": 10},
            },
            "feature_store": {"cache_ttl_minutes": 60, "batch_size": 10000},
        }

    def deploy_model(
        self,
        model_name: str,
        model_path: str,
        version: str,
        traffic_percentage: float = 0.0,
    ) -> bool:
        """
        Deploy a new model version.

        Args:
            model_name: Name of the model
            model_path: Path to the trained model
            version: Version identifier
            traffic_percentage: Percentage of traffic to route to this model

        Returns:
            True if deployment successful, False otherwise
        """
        try:
            self.logger.info(f"Deploying model {model_name} version {version}")

            # Load the model
            self.model_trainer.load_models(model_path)

            # Create model version record
            model_version = ModelVersion(
                version=version,
                model_name=model_name,
                model_path=model_path,
                deployed_at=datetime.now(),
                status=ModelStatus.DEPLOYED,
                performance_metrics={},
                traffic_percentage=traffic_percentage,
            )

            # Store model version
            version_key = f"{model_name}_{version}"
            self.model_versions[version_key] = model_version

            # Update active models
            if model_name not in self.active_models:
                self.active_models[model_name] = []

            self.active_models[model_name].append(
                {
                    "version": version,
                    "model": self.model_trainer.models.get(model_name),
                    "traffic_percentage": traffic_percentage,
                }
            )

            # Update traffic split
            self._update_traffic_split(model_name)

            self.logger.info(
                f"Model {model_name} version {version} deployed successfully"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"Failed to deploy model {model_name} version {version}: {e}"
            )
            return False

    def _update_traffic_split(self, model_name: str) -> None:
        """Update traffic split configuration for A/B testing."""
        if model_name not in self.active_models:
            return

        versions = self.active_models[model_name]
        if len(versions) == 1:
            versions[0]["traffic_percentage"] = 100.0
        else:
            # Implement champion/challenger pattern
            # Sort by deployment time, latest is challenger
            versions.sort(
                key=lambda x: self.model_versions[
                    f"{model_name}_{x['version']}"
                ].deployed_at
            )

            # Champion gets default traffic, challenger gets remaining
            champion_traffic = self.config["models"]["default_traffic_split"][
                "champion"
            ]
            challenger_traffic = self.config["models"]["default_traffic_split"][
                "challenger"
            ]

            for i, version in enumerate(versions):
                if i == len(versions) - 1:  # Latest version (challenger)
                    version["traffic_percentage"] = challenger_traffic
                else:  # Previous versions
                    version["traffic_percentage"] = champion_traffic / (
                        len(versions) - 1
                    )

        # Update traffic split config
        self.traffic_split_config[model_name] = {
            v["version"]: v["traffic_percentage"] for v in versions
        }

    def predict_fraud(
        self, transaction_data: Dict[str, Any], model_name: str = "random_forest"
    ) -> PredictionResponse:
        """
        Make real-time fraud prediction.

        Args:
            transaction_data: Transaction data for prediction
            model_name: Name of the model to use

        Returns:
            Prediction response with fraud probability and metadata
        """
        start_time = time.time()
        request_id = (
            f"pred_{int(time.time() * 1000)}_{hash(str(transaction_data)) % 10000}"
        )

        try:
            # Select model version based on traffic split
            selected_version = self._select_model_version(model_name)
            if not selected_version:
                raise ValueError(f"No active model found for {model_name}")

            # Convert transaction data to DataFrame
            transaction_df = self._create_transaction_dataframe(transaction_data)

            # Make prediction
            predictions_df = self.model_trainer.predict_fraud_probability(
                transaction_df, model_name
            )

            # Extract prediction result
            prediction_result = predictions_df.select(
                "fraud_probability", "ml_fraud_prediction", "features"
            ).collect()[0]

            # Calculate prediction time
            prediction_time_ms = (time.time() - start_time) * 1000

            # Create response
            response = PredictionResponse(
                request_id=request_id,
                fraud_probability=float(prediction_result["fraud_probability"]),
                prediction=int(prediction_result["ml_fraud_prediction"]),
                model_version=selected_version,
                prediction_time_ms=prediction_time_ms,
                feature_values={},  # Could extract from features vector if needed
            )

            # Update metrics
            self._update_prediction_metrics(response, success=True)

            return response

        except Exception as e:
            self.logger.error(f"Prediction failed for request {request_id}: {e}")

            prediction_time_ms = (time.time() - start_time) * 1000

            # Create error response
            response = PredictionResponse(
                request_id=request_id,
                fraud_probability=0.5,  # Neutral prediction
                prediction=0,
                model_version="error",
                prediction_time_ms=prediction_time_ms,
                feature_values={},
                error=str(e),
            )

            # Update metrics
            self._update_prediction_metrics(response, success=False)

            return response

    def _select_model_version(self, model_name: str) -> Optional[str]:
        """
        Select model version based on traffic split configuration.

        Args:
            model_name: Name of the model

        Returns:
            Selected model version or None
        """
        if model_name not in self.traffic_split_config:
            return None

        import random

        traffic_split = self.traffic_split_config[model_name]
        random_value = random.random() * 100

        cumulative_percentage = 0
        for version, percentage in traffic_split.items():
            cumulative_percentage += percentage
            if random_value <= cumulative_percentage:
                return version

        # Fallback to first version
        return list(traffic_split.keys())[0]

    def _create_transaction_dataframe(
        self, transaction_data: Dict[str, Any]
    ) -> DataFrame:
        """Create Spark DataFrame from transaction data."""
        # Define schema
        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("merchant_id", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("payment_method", StringType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )

        # Convert to DataFrame
        data = [
            (
                transaction_data.get("customer_id", "unknown"),
                transaction_data.get("merchant_id", "unknown"),
                float(transaction_data.get("price", 0.0)),
                transaction_data.get("payment_method", "unknown"),
                datetime.fromisoformat(
                    transaction_data.get("timestamp", datetime.now().isoformat())
                ),
            )
        ]

        return self.spark.createDataFrame(data, schema)

    def start_serving(self) -> None:
        """Start the model serving pipeline."""
        self.logger.info("Starting model serving pipeline")
        self._running = True
        self.serving_status = ServingStatus.ACTIVE

        # Start background threads
        self._prediction_thread = threading.Thread(
            target=self._prediction_worker, daemon=True
        )
        self._monitoring_thread = threading.Thread(
            target=self._monitoring_worker, daemon=True
        )

        self._prediction_thread.start()
        self._monitoring_thread.start()

        self.logger.info("Model serving pipeline started successfully")

    def stop_serving(self) -> None:
        """Stop the model serving pipeline."""
        self.logger.info("Stopping model serving pipeline")
        self._running = False
        self.serving_status = ServingStatus.OFFLINE

        # Wait for threads to finish
        if self._prediction_thread:
            self._prediction_thread.join(timeout=5)
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)

        self.logger.info("Model serving pipeline stopped")

    def _prediction_worker(self) -> None:
        """Background worker for processing prediction requests."""
        while self._running:
            try:
                # Process queued predictions (if implementing async predictions)
                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"Prediction worker error: {e}")

    def _monitoring_worker(self) -> None:
        """Background worker for monitoring model performance."""
        while self._running:
            try:
                # Update performance metrics
                self._update_serving_metrics()

                # Check for performance degradation
                self._check_model_health()

                time.sleep(self.config["monitoring"]["metrics_update_interval_seconds"])

            except Exception as e:
                self.logger.error(f"Monitoring worker error: {e}")

    def _update_prediction_metrics(
        self, response: PredictionResponse, success: bool
    ) -> None:
        """Update prediction metrics."""
        self.metrics["total_predictions"] += 1

        if success:
            self.metrics["successful_predictions"] += 1
        else:
            self.metrics["failed_predictions"] += 1

        # Update average prediction time (exponential moving average)
        alpha = 0.1
        self.metrics["average_prediction_time_ms"] = (
            alpha * response.prediction_time_ms
            + (1 - alpha) * self.metrics["average_prediction_time_ms"]
        )

    def _update_serving_metrics(self) -> None:
        """Update serving performance metrics."""
        # Calculate throughput (simplified)
        current_time = time.time()
        if hasattr(self, "_last_metrics_update"):
            time_diff = current_time - self._last_metrics_update
            if time_diff > 0:
                predictions_diff = self.metrics["total_predictions"] - getattr(
                    self, "_last_prediction_count", 0
                )
                self.metrics["throughput_per_second"] = predictions_diff / time_diff

        self._last_metrics_update = current_time
        self._last_prediction_count = self.metrics["total_predictions"]

    def _check_model_health(self) -> None:
        """Check model health and trigger alerts if needed."""
        # Check error rate
        total_predictions = self.metrics["total_predictions"]
        if total_predictions > 100:  # Only check after minimum predictions
            error_rate = self.metrics["failed_predictions"] / total_predictions
            threshold = self.config["monitoring"]["error_rate_threshold"]

            if error_rate > threshold:
                self.logger.warning(
                    f"High error rate detected: {error_rate:.3f} > {threshold:.3f}"
                )
                self.serving_status = ServingStatus.DEGRADED

        # Check prediction latency
        avg_time = self.metrics["average_prediction_time_ms"]
        threshold_ms = self.config["monitoring"]["performance_threshold_ms"]

        if avg_time > threshold_ms:
            self.logger.warning(
                f"High prediction latency: {avg_time:.1f}ms > {threshold_ms}ms"
            )
            if self.serving_status == ServingStatus.ACTIVE:
                self.serving_status = ServingStatus.DEGRADED

    def get_serving_status(self) -> Dict[str, Any]:
        """Get current serving status and metrics."""
        return {
            "status": self.serving_status.value,
            "metrics": self.metrics,
            "active_models": {
                model_name: [
                    {
                        "version": v["version"],
                        "traffic_percentage": v["traffic_percentage"],
                    }
                    for v in versions
                ]
                for model_name, versions in self.active_models.items()
            },
            "model_versions": {
                key: {
                    "version": mv.version,
                    "model_name": mv.model_name,
                    "status": mv.status.value,
                    "deployed_at": mv.deployed_at.isoformat(),
                    "traffic_percentage": mv.traffic_percentage,
                    "prediction_count": mv.prediction_count,
                    "error_count": mv.error_count,
                }
                for key, mv in self.model_versions.items()
            },
        }

    def rollback_model(self, model_name: str, target_version: str) -> bool:
        """
        Rollback to a previous model version.

        Args:
            model_name: Name of the model
            target_version: Version to rollback to

        Returns:
            True if rollback successful, False otherwise
        """
        try:
            version_key = f"{model_name}_{target_version}"
            if version_key not in self.model_versions:
                raise ValueError(
                    f"Version {target_version} not found for model {model_name}"
                )

            # Set target version to 100% traffic
            self.traffic_split_config[model_name] = {target_version: 100.0}

            # Update active models
            for version_info in self.active_models.get(model_name, []):
                if version_info["version"] == target_version:
                    version_info["traffic_percentage"] = 100.0
                else:
                    version_info["traffic_percentage"] = 0.0

            self.logger.info(
                f"Rolled back model {model_name} to version {target_version}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Rollback failed for model {model_name}: {e}")
            return False

    def update_traffic_split(
        self, model_name: str, traffic_config: Dict[str, float]
    ) -> bool:
        """
        Update traffic split configuration for A/B testing.

        Args:
            model_name: Name of the model
            traffic_config: Dictionary mapping version to traffic percentage

        Returns:
            True if update successful, False otherwise
        """
        try:
            # Validate traffic percentages sum to 100
            total_traffic = sum(traffic_config.values())
            if abs(total_traffic - 100.0) > 0.1:
                raise ValueError(
                    f"Traffic percentages must sum to 100, got {total_traffic}"
                )

            # Update configuration
            self.traffic_split_config[model_name] = traffic_config.copy()

            # Update active models
            for version_info in self.active_models.get(model_name, []):
                version = version_info["version"]
                if version in traffic_config:
                    version_info["traffic_percentage"] = traffic_config[version]

            self.logger.info(
                f"Updated traffic split for model {model_name}: {traffic_config}"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"Failed to update traffic split for model {model_name}: {e}"
            )
            return False

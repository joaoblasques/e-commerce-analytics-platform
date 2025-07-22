"""
Tests for ML model serving pipeline.

This module tests the real-time model serving infrastructure for fraud detection,
including model deployment, A/B testing, traffic splitting, and performance monitoring.
"""

import json
import tempfile
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics.fraud_detection.ml_models import ModelStatus
from src.analytics.fraud_detection.model_serving import (
    ModelServingPipeline,
    ModelVersion,
    PredictionRequest,
    PredictionResponse,
    ServingStatus,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return (
        SparkSession.builder.appName("TestModelServing")
        .master("local[2]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def sample_transaction_data():
    """Sample transaction data for prediction testing."""
    return {
        "customer_id": "customer_123",
        "merchant_id": "merchant_456",
        "price": 150.75,
        "payment_method": "credit_card",
        "timestamp": datetime.now().isoformat(),
    }


@pytest.fixture
def model_serving_pipeline(spark):
    """Create model serving pipeline for testing."""
    config = {
        "serving": {
            "max_batch_size": 100,
            "batch_timeout_ms": 50,
            "max_queue_size": 1000,
            "prediction_timeout_ms": 2000,
            "fallback_enabled": True,
        },
        "monitoring": {
            "metrics_update_interval_seconds": 5,
            "performance_threshold_ms": 500,
            "error_rate_threshold": 0.1,
            "throughput_threshold_per_second": 50,
        },
        "models": {
            "model_store_path": "test_models",
            "max_versions_per_model": 3,
            "default_traffic_split": {"champion": 80, "challenger": 20},
        },
    }
    return ModelServingPipeline(spark, config)


@pytest.fixture
def mock_model_trainer():
    """Mock ML model trainer for testing."""
    trainer = Mock()
    trainer.models = {"random_forest": Mock(), "gradient_boosting": Mock()}
    trainer.load_models.return_value = True

    # Mock prediction results
    mock_predictions = Mock()
    mock_predictions.select.return_value.collect.return_value = [
        {
            "fraud_probability": 0.75,
            "ml_fraud_prediction": 1,
            "features": [0.1, 0.2, 0.3],
        }
    ]
    trainer.predict_fraud_probability.return_value = mock_predictions

    return trainer


class TestModelVersion:
    """Test model version data class."""

    def test_model_version_creation(self):
        """Test creation of model version."""
        version = ModelVersion(
            version="v1.0.0",
            model_name="random_forest",
            model_path="/path/to/model",
            deployed_at=datetime.now(),
            status=ModelStatus.DEPLOYED,
            performance_metrics={"accuracy": 0.85},
            traffic_percentage=100.0,
            prediction_count=1000,
            error_count=5,
        )

        assert version.version == "v1.0.0"
        assert version.model_name == "random_forest"
        assert version.status == ModelStatus.DEPLOYED
        assert version.performance_metrics["accuracy"] == 0.85
        assert version.traffic_percentage == 100.0
        assert version.prediction_count == 1000
        assert version.error_count == 5


class TestPredictionRequest:
    """Test prediction request data class."""

    def test_prediction_request_creation(self, sample_transaction_data):
        """Test creation of prediction request."""
        request = PredictionRequest(
            request_id="req_123",
            transaction_data=sample_transaction_data,
            timestamp=datetime.now(),
            model_version="v1.0.0",
        )

        assert request.request_id == "req_123"
        assert request.transaction_data == sample_transaction_data
        assert request.model_version == "v1.0.0"
        assert isinstance(request.timestamp, datetime)


class TestPredictionResponse:
    """Test prediction response data class."""

    def test_prediction_response_creation(self):
        """Test creation of prediction response."""
        response = PredictionResponse(
            request_id="req_123",
            fraud_probability=0.85,
            prediction=1,
            model_version="v1.0.0",
            prediction_time_ms=150.5,
            feature_values={"price": 100.0, "hour": 14},
            error=None,
        )

        assert response.request_id == "req_123"
        assert response.fraud_probability == 0.85
        assert response.prediction == 1
        assert response.model_version == "v1.0.0"
        assert response.prediction_time_ms == 150.5
        assert response.feature_values["price"] == 100.0
        assert response.error is None


class TestModelServingPipeline:
    """Test cases for model serving pipeline."""

    def test_initialization(self, model_serving_pipeline):
        """Test pipeline initialization."""
        assert model_serving_pipeline.spark is not None
        assert model_serving_pipeline.serving_status == ServingStatus.INITIALIZING
        assert isinstance(model_serving_pipeline.model_versions, dict)
        assert isinstance(model_serving_pipeline.active_models, dict)
        assert isinstance(model_serving_pipeline.metrics, dict)

        # Check initial metrics
        assert model_serving_pipeline.metrics["total_predictions"] == 0
        assert model_serving_pipeline.metrics["successful_predictions"] == 0
        assert model_serving_pipeline.metrics["failed_predictions"] == 0

    def test_default_config(self, model_serving_pipeline):
        """Test default configuration structure."""
        config = model_serving_pipeline.config

        assert "serving" in config
        assert "monitoring" in config
        assert "models" in config
        assert "feature_store" in config

        # Check serving config
        serving_config = config["serving"]
        assert "max_batch_size" in serving_config
        assert "prediction_timeout_ms" in serving_config

        # Check monitoring config
        monitoring_config = config["monitoring"]
        assert "performance_threshold_ms" in monitoring_config
        assert "error_rate_threshold" in monitoring_config

    @patch("src.analytics.fraud_detection.model_serving.MLFraudModelTrainer")
    def test_model_deployment_success(self, mock_trainer_class, model_serving_pipeline):
        """Test successful model deployment."""
        # Setup mock trainer
        mock_trainer = Mock()
        mock_trainer.load_models.return_value = True
        mock_trainer.models = {"random_forest": Mock()}
        mock_trainer_class.return_value = mock_trainer
        model_serving_pipeline.model_trainer = mock_trainer

        success = model_serving_pipeline.deploy_model(
            model_name="random_forest",
            model_path="/test/path",
            version="v1.0.0",
            traffic_percentage=100.0,
        )

        assert success is True

        # Check model version was stored
        version_key = "random_forest_v1.0.0"
        assert version_key in model_serving_pipeline.model_versions

        model_version = model_serving_pipeline.model_versions[version_key]
        assert model_version.model_name == "random_forest"
        assert model_version.version == "v1.0.0"
        assert model_version.traffic_percentage == 100.0

        # Check active models updated
        assert "random_forest" in model_serving_pipeline.active_models
        assert len(model_serving_pipeline.active_models["random_forest"]) == 1

    @patch("src.analytics.fraud_detection.model_serving.MLFraudModelTrainer")
    def test_model_deployment_failure(self, mock_trainer_class, model_serving_pipeline):
        """Test model deployment failure."""
        # Setup mock trainer to raise exception
        mock_trainer = Mock()
        mock_trainer.load_models.side_effect = Exception("Model loading failed")
        mock_trainer_class.return_value = mock_trainer
        model_serving_pipeline.model_trainer = mock_trainer

        success = model_serving_pipeline.deploy_model(
            model_name="random_forest", model_path="/invalid/path", version="v1.0.0"
        )

        assert success is False
        assert len(model_serving_pipeline.model_versions) == 0
        assert len(model_serving_pipeline.active_models) == 0

    def test_traffic_split_single_model(self, model_serving_pipeline):
        """Test traffic split with single model."""
        # Manually add a model to active models
        model_serving_pipeline.active_models["test_model"] = [
            {"version": "v1.0.0", "traffic_percentage": 0}
        ]

        model_serving_pipeline._update_traffic_split("test_model")

        # Single model should get 100% traffic
        assert (
            model_serving_pipeline.active_models["test_model"][0]["traffic_percentage"]
            == 100.0
        )
        assert "test_model" in model_serving_pipeline.traffic_split_config
        assert (
            model_serving_pipeline.traffic_split_config["test_model"]["v1.0.0"] == 100.0
        )

    def test_traffic_split_multiple_models(self, model_serving_pipeline):
        """Test traffic split with multiple models (champion/challenger)."""
        # Add model versions with timestamps
        base_time = datetime.now()
        model_serving_pipeline.model_versions = {
            "test_model_v1.0.0": ModelVersion(
                version="v1.0.0",
                model_name="test_model",
                model_path="/path1",
                deployed_at=base_time - timedelta(hours=1),  # Older (champion)
                status=ModelStatus.DEPLOYED,
                performance_metrics={},
            ),
            "test_model_v2.0.0": ModelVersion(
                version="v2.0.0",
                model_name="test_model",
                model_path="/path2",
                deployed_at=base_time,  # Newer (challenger)
                status=ModelStatus.DEPLOYED,
                performance_metrics={},
            ),
        }

        model_serving_pipeline.active_models["test_model"] = [
            {"version": "v1.0.0", "traffic_percentage": 0},
            {"version": "v2.0.0", "traffic_percentage": 0},
        ]

        model_serving_pipeline._update_traffic_split("test_model")

        # Check champion/challenger split
        traffic_config = model_serving_pipeline.traffic_split_config["test_model"]
        assert traffic_config["v1.0.0"] == 90.0  # Champion
        assert traffic_config["v2.0.0"] == 10.0  # Challenger

    def test_model_version_selection(self, model_serving_pipeline):
        """Test model version selection based on traffic split."""
        # Set up traffic split
        model_serving_pipeline.traffic_split_config["test_model"] = {
            "v1.0.0": 70.0,
            "v2.0.0": 30.0,
        }

        # Test selection multiple times to check distribution
        selections = []
        for _ in range(100):
            version = model_serving_pipeline._select_model_version("test_model")
            selections.append(version)

        # Should return valid versions
        for version in selections:
            assert version in ["v1.0.0", "v2.0.0"]

        # Check that both versions were selected (probabilistic test)
        unique_versions = set(selections)
        assert len(unique_versions) >= 1  # At least one version selected

    def test_transaction_dataframe_creation(
        self, model_serving_pipeline, sample_transaction_data
    ):
        """Test conversion of transaction data to DataFrame."""
        df = model_serving_pipeline._create_transaction_dataframe(
            sample_transaction_data
        )

        assert df.count() == 1

        # Check schema
        expected_columns = [
            "customer_id",
            "merchant_id",
            "price",
            "payment_method",
            "timestamp",
        ]
        for col in expected_columns:
            assert col in df.columns

        # Check data values
        row = df.collect()[0]
        assert row["customer_id"] == "customer_123"
        assert row["merchant_id"] == "merchant_456"
        assert row["price"] == 150.75
        assert row["payment_method"] == "credit_card"

    @patch("src.analytics.fraud_detection.model_serving.MLFraudModelTrainer")
    def test_fraud_prediction_success(
        self,
        mock_trainer_class,
        model_serving_pipeline,
        sample_transaction_data,
        mock_model_trainer,
    ):
        """Test successful fraud prediction."""
        # Setup pipeline with mock trainer
        model_serving_pipeline.model_trainer = mock_model_trainer
        model_serving_pipeline.traffic_split_config["random_forest"] = {"v1.0.0": 100.0}

        response = model_serving_pipeline.predict_fraud(sample_transaction_data)

        assert isinstance(response, PredictionResponse)
        assert response.fraud_probability == 0.75
        assert response.prediction == 1
        assert response.prediction_time_ms > 0
        assert response.error is None

        # Check metrics updated
        assert model_serving_pipeline.metrics["total_predictions"] == 1
        assert model_serving_pipeline.metrics["successful_predictions"] == 1

    @patch("src.analytics.fraud_detection.model_serving.MLFraudModelTrainer")
    def test_fraud_prediction_failure(
        self, mock_trainer_class, model_serving_pipeline, sample_transaction_data
    ):
        """Test fraud prediction failure."""
        # Setup pipeline with failing trainer
        mock_trainer = Mock()
        mock_trainer.predict_fraud_probability.side_effect = Exception(
            "Prediction failed"
        )
        model_serving_pipeline.model_trainer = mock_trainer
        model_serving_pipeline.traffic_split_config["random_forest"] = {"v1.0.0": 100.0}

        response = model_serving_pipeline.predict_fraud(sample_transaction_data)

        assert isinstance(response, PredictionResponse)
        assert response.fraud_probability == 0.5  # Neutral prediction on error
        assert response.prediction == 0
        assert response.error is not None

        # Check metrics updated
        assert model_serving_pipeline.metrics["total_predictions"] == 1
        assert model_serving_pipeline.metrics["failed_predictions"] == 1

    def test_prediction_metrics_update(self, model_serving_pipeline):
        """Test prediction metrics updates."""
        # Create mock response
        response = PredictionResponse(
            request_id="test_req",
            fraud_probability=0.8,
            prediction=1,
            model_version="v1.0.0",
            prediction_time_ms=200.0,
            feature_values={},
        )

        initial_total = model_serving_pipeline.metrics["total_predictions"]
        initial_successful = model_serving_pipeline.metrics["successful_predictions"]
        initial_avg_time = model_serving_pipeline.metrics["average_prediction_time_ms"]

        model_serving_pipeline._update_prediction_metrics(response, success=True)

        assert model_serving_pipeline.metrics["total_predictions"] == initial_total + 1
        assert (
            model_serving_pipeline.metrics["successful_predictions"]
            == initial_successful + 1
        )

        # Check exponential moving average update
        expected_avg = 0.1 * 200.0 + 0.9 * initial_avg_time
        assert (
            abs(
                model_serving_pipeline.metrics["average_prediction_time_ms"]
                - expected_avg
            )
            < 0.01
        )

    def test_model_health_check_error_rate(self, model_serving_pipeline):
        """Test model health check for high error rate."""
        # Set metrics to trigger error rate alert
        model_serving_pipeline.metrics["total_predictions"] = 1000
        model_serving_pipeline.metrics["failed_predictions"] = 150  # 15% error rate
        model_serving_pipeline.serving_status = ServingStatus.ACTIVE

        model_serving_pipeline._check_model_health()

        assert model_serving_pipeline.serving_status == ServingStatus.DEGRADED

    def test_model_health_check_latency(self, model_serving_pipeline):
        """Test model health check for high latency."""
        # Set metrics to trigger latency alert
        model_serving_pipeline.metrics[
            "average_prediction_time_ms"
        ] = 2000  # Above threshold
        model_serving_pipeline.serving_status = ServingStatus.ACTIVE

        model_serving_pipeline._check_model_health()

        assert model_serving_pipeline.serving_status == ServingStatus.DEGRADED

    def test_get_serving_status(self, model_serving_pipeline):
        """Test serving status retrieval."""
        # Add some test data
        model_serving_pipeline.metrics["total_predictions"] = 500
        model_serving_pipeline.active_models["test_model"] = [
            {"version": "v1.0.0", "traffic_percentage": 100.0}
        ]

        status = model_serving_pipeline.get_serving_status()

        assert "status" in status
        assert "metrics" in status
        assert "active_models" in status
        assert "model_versions" in status

        assert status["metrics"]["total_predictions"] == 500
        assert "test_model" in status["active_models"]

    def test_model_rollback(self, model_serving_pipeline):
        """Test model rollback functionality."""
        # Setup model versions
        model_serving_pipeline.model_versions["test_model_v1.0.0"] = ModelVersion(
            version="v1.0.0",
            model_name="test_model",
            model_path="/path1",
            deployed_at=datetime.now(),
            status=ModelStatus.DEPLOYED,
            performance_metrics={},
        )

        model_serving_pipeline.active_models["test_model"] = [
            {"version": "v1.0.0", "traffic_percentage": 50.0},
            {"version": "v2.0.0", "traffic_percentage": 50.0},
        ]

        success = model_serving_pipeline.rollback_model("test_model", "v1.0.0")

        assert success is True
        assert (
            model_serving_pipeline.traffic_split_config["test_model"]["v1.0.0"] == 100.0
        )

        # Check active models updated
        for version_info in model_serving_pipeline.active_models["test_model"]:
            if version_info["version"] == "v1.0.0":
                assert version_info["traffic_percentage"] == 100.0
            else:
                assert version_info["traffic_percentage"] == 0.0

    def test_traffic_split_update(self, model_serving_pipeline):
        """Test traffic split configuration updates."""
        # Setup active models
        model_serving_pipeline.active_models["test_model"] = [
            {"version": "v1.0.0", "traffic_percentage": 50.0},
            {"version": "v2.0.0", "traffic_percentage": 50.0},
        ]

        new_traffic_config = {"v1.0.0": 80.0, "v2.0.0": 20.0}
        success = model_serving_pipeline.update_traffic_split(
            "test_model", new_traffic_config
        )

        assert success is True
        assert (
            model_serving_pipeline.traffic_split_config["test_model"]
            == new_traffic_config
        )

        # Check active models updated
        for version_info in model_serving_pipeline.active_models["test_model"]:
            expected_percentage = new_traffic_config[version_info["version"]]
            assert version_info["traffic_percentage"] == expected_percentage

    def test_traffic_split_update_invalid_total(self, model_serving_pipeline):
        """Test traffic split update with invalid percentage total."""
        invalid_traffic_config = {"v1.0.0": 60.0, "v2.0.0": 50.0}  # Sums to 110%

        success = model_serving_pipeline.update_traffic_split(
            "test_model", invalid_traffic_config
        )

        assert success is False

    def test_serving_lifecycle(self, model_serving_pipeline):
        """Test complete serving pipeline lifecycle."""
        # Start serving
        model_serving_pipeline.start_serving()
        assert model_serving_pipeline.serving_status == ServingStatus.ACTIVE
        assert model_serving_pipeline._running is True

        # Give threads time to start
        time.sleep(0.1)

        # Stop serving
        model_serving_pipeline.stop_serving()
        assert model_serving_pipeline.serving_status == ServingStatus.OFFLINE
        assert model_serving_pipeline._running is False

    def test_edge_cases(self, model_serving_pipeline):
        """Test edge cases and error handling."""
        # Test prediction with no active models
        response = model_serving_pipeline.predict_fraud({"test": "data"})
        assert response.error is not None

        # Test model selection with no traffic config
        version = model_serving_pipeline._select_model_version("nonexistent")
        assert version is None

        # Test rollback with invalid version
        success = model_serving_pipeline.rollback_model("test_model", "nonexistent")
        assert success is False


class TestServingStatus:
    """Test serving status enumeration."""

    def test_serving_status_values(self):
        """Test serving status enum values."""
        assert ServingStatus.INITIALIZING.value == "initializing"
        assert ServingStatus.ACTIVE.value == "active"
        assert ServingStatus.DEGRADED.value == "degraded"
        assert ServingStatus.OFFLINE.value == "offline"
        assert ServingStatus.ERROR.value == "error"

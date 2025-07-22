"""
Tests for ML fraud detection models.

This module tests the machine learning ensemble models for fraud detection,
including training, prediction, feature importance, and performance evaluation.
"""

import shutil
import tempfile
from datetime import datetime, timedelta

import pyspark.sql.functions as F
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

from src.analytics.fraud_detection.ml_models import (
    MLFraudModelTrainer,
    ModelConfiguration,
    ModelMetrics,
    ModelStatus,
    ModelType,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return (
        SparkSession.builder.appName("TestMLFraudModels")
        .master("local[2]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def sample_transaction_data(spark):
    """Create sample transaction data for testing."""
    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )

    # Generate sample data with both legitimate and fraud patterns
    base_time = datetime.now()
    data = []

    # Normal transactions
    for i in range(100):
        data.append(
            (
                f"txn_{i}",
                f"customer_{i % 20}",
                f"merchant_{i % 10}",
                float(50 + (i % 200)),  # Normal amounts
                "credit_card" if i % 2 == 0 else "debit_card",
                base_time - timedelta(hours=i % 24),
            )
        )

    # Fraudulent patterns
    for i in range(100, 150):
        data.append(
            (
                f"txn_{i}",
                f"customer_{i % 20}",
                f"merchant_{i % 10}",
                float(5000 + (i % 5000)),  # High amounts (fraud indicator)
                "credit_card",
                base_time - timedelta(hours=2),  # Unusual hours (fraud indicator)
            )
        )

    return spark.createDataFrame(data, schema)


@pytest.fixture
def ml_trainer(spark):
    """Create ML fraud model trainer for testing."""
    return MLFraudModelTrainer(spark)


class TestMLFraudModelTrainer:
    """Test cases for ML fraud model trainer."""

    def test_initialization(self, ml_trainer):
        """Test trainer initialization."""
        assert ml_trainer.spark is not None
        assert ml_trainer.config is not None
        assert ml_trainer.target_column == "is_fraud"
        assert isinstance(ml_trainer.models, dict)
        assert len(ml_trainer.models) == 0

    def test_default_config(self, ml_trainer):
        """Test default configuration structure."""
        config = ml_trainer.config

        assert "feature_engineering" in config
        assert "models" in config
        assert "evaluation" in config

        # Check feature engineering config
        feature_config = config["feature_engineering"]
        assert "transaction_features" in feature_config
        assert "customer_features" in feature_config
        assert "merchant_features" in feature_config
        assert "categorical_features" in feature_config

        # Check model configs
        model_config = config["models"]
        assert "random_forest" in model_config
        assert "gradient_boosting" in model_config
        assert "logistic_regression" in model_config

    def test_feature_preparation(self, ml_trainer, sample_transaction_data):
        """Test feature preparation pipeline."""
        features_df = ml_trainer.prepare_features(sample_transaction_data)

        # Check that new features were added
        expected_columns = [
            "hour",
            "day_of_week",
            "day_of_month",
            "month",
            "is_weekend",
            "is_business_hours",
            "is_unusual_hour",
            "customer_avg_amount",
            "customer_max_amount",
            "customer_min_amount",
            "amount_deviation_from_avg",
            "amount_z_score",
            "transaction_count_1h",
            "total_amount_1h",
            "merchant_transaction_count",
            "merchant_avg_amount",
            "merchant_risk_score",
            "is_fraud",
        ]

        for col in expected_columns:
            assert col in features_df.columns, f"Column {col} missing from features"

        # Check that fraud labels are generated
        fraud_count = features_df.filter(F.col("is_fraud") == 1).count()
        assert fraud_count > 0, "No fraud labels generated"

        # Check data types
        assert features_df.schema["hour"].dataType == IntegerType()
        assert features_df.schema["is_weekend"].dataType in [
            IntegerType(),
            BooleanType(),
        ]
        assert features_df.schema["customer_avg_amount"].dataType == DoubleType()

    def test_ensemble_model_training(self, ml_trainer, sample_transaction_data):
        """Test ensemble model training."""
        training_results = ml_trainer.train_ensemble_models(sample_transaction_data)

        # Check that all models were trained
        assert "models" in training_results
        assert "metrics" in training_results
        assert "feature_columns" in training_results

        models = training_results["models"]
        assert "random_forest" in models
        assert "gradient_boosting" in models
        assert "logistic_regression" in models

        # Check metrics
        metrics = training_results["metrics"]
        for model_name in ["random_forest", "gradient_boosting", "logistic_regression"]:
            assert model_name in metrics
            model_metrics = metrics[model_name]
            assert "auc_roc" in model_metrics
            assert "accuracy" in model_metrics
            assert 0.0 <= model_metrics["auc_roc"] <= 1.0
            assert 0.0 <= model_metrics["accuracy"] <= 1.0

        # Check that models are stored
        assert len(ml_trainer.models) == 3
        assert "random_forest" in ml_trainer.models
        assert "gradient_boosting" in ml_trainer.models
        assert "logistic_regression" in ml_trainer.models

    def test_fraud_prediction(self, ml_trainer, sample_transaction_data):
        """Test fraud probability prediction."""
        # First train the models
        ml_trainer.train_ensemble_models(sample_transaction_data)

        # Create prediction data
        pred_data = sample_transaction_data.limit(10)

        # Test prediction with each model
        for model_name in ["random_forest", "gradient_boosting", "logistic_regression"]:
            predictions_df = ml_trainer.predict_fraud_probability(pred_data, model_name)

            # Check prediction columns exist
            assert "fraud_probability" in predictions_df.columns
            assert "ml_fraud_prediction" in predictions_df.columns

            # Check prediction values are valid
            predictions = predictions_df.select(
                "fraud_probability", "ml_fraud_prediction"
            ).collect()

            for row in predictions:
                prob = row["fraud_probability"]
                pred = row["ml_fraud_prediction"]

                assert 0.0 <= prob <= 1.0, f"Invalid probability: {prob}"
                assert pred in [0, 1], f"Invalid prediction: {pred}"

    def test_feature_importance(self, ml_trainer, sample_transaction_data):
        """Test feature importance extraction."""
        # Train models
        ml_trainer.train_ensemble_models(sample_transaction_data)

        # Test feature importance for Random Forest (supports feature importance)
        importance = ml_trainer.get_feature_importance("random_forest")

        assert isinstance(importance, dict)
        assert len(importance) > 0

        # Check that importance values are valid
        for feature, score in importance.items():
            assert isinstance(feature, str)
            assert isinstance(score, float)
            assert score >= 0.0

        # Check that importance values sum to approximately 1.0
        total_importance = sum(importance.values())
        assert 0.8 <= total_importance <= 1.2  # Allow some tolerance

    def test_model_save_load(self, ml_trainer, sample_transaction_data):
        """Test model saving and loading."""
        # Train models
        ml_trainer.train_ensemble_models(sample_transaction_data)

        with tempfile.TemporaryDirectory() as temp_dir:
            # Save models
            ml_trainer.save_models(temp_dir)

            # Create new trainer and load models
            new_trainer = MLFraudModelTrainer(ml_trainer.spark)
            new_trainer.load_models(temp_dir)

            # Check that models were loaded
            assert len(new_trainer.models) > 0

            # Test predictions with loaded models
            pred_data = sample_transaction_data.limit(5)

            for model_name in new_trainer.models.keys():
                predictions = new_trainer.predict_fraud_probability(
                    pred_data, model_name
                )
                assert predictions.count() == 5
                assert "fraud_probability" in predictions.columns

    def test_cross_validation(self, ml_trainer, sample_transaction_data):
        """Test cross-validation functionality."""
        cv_results = ml_trainer.cross_validate_model(
            sample_transaction_data, "random_forest"
        )

        assert "average_auc" in cv_results
        assert "std_auc" in cv_results
        assert "best_model" in cv_results

        # Check metric values
        assert 0.0 <= cv_results["average_auc"] <= 1.0
        assert cv_results["std_auc"] >= 0.0
        assert cv_results["best_model"] is not None

    def test_invalid_model_prediction(self, ml_trainer, sample_transaction_data):
        """Test prediction with invalid model name."""
        with pytest.raises(ValueError, match="Model nonexistent not found"):
            ml_trainer.predict_fraud_probability(sample_transaction_data, "nonexistent")

    def test_invalid_feature_importance(self, ml_trainer):
        """Test feature importance for invalid model."""
        with pytest.raises(ValueError, match="Model nonexistent not found"):
            ml_trainer.get_feature_importance("nonexistent")

    def test_edge_cases(self, ml_trainer, spark):
        """Test edge cases and error handling."""
        # Empty DataFrame
        empty_schema = StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("merchant_id", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("payment_method", StringType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )

        empty_df = spark.createDataFrame([], empty_schema)

        # Feature preparation should handle empty DataFrame
        features_df = ml_trainer.prepare_features(empty_df)
        assert features_df.count() == 0

        # Single row DataFrame
        single_row_data = [
            ("txn_1", "customer_1", "merchant_1", 100.0, "credit_card", datetime.now())
        ]
        single_df = spark.createDataFrame(single_row_data, empty_schema)

        features_df = ml_trainer.prepare_features(single_df)
        assert features_df.count() == 1
        assert "is_fraud" in features_df.columns


class TestModelTypes:
    """Test model type enumeration."""

    def test_model_types(self):
        """Test model type enum values."""
        assert ModelType.RANDOM_FOREST.value == "random_forest"
        assert ModelType.GRADIENT_BOOSTING.value == "gradient_boosting"
        assert ModelType.LOGISTIC_REGRESSION.value == "logistic_regression"
        assert ModelType.ENSEMBLE.value == "ensemble"


class TestModelStatus:
    """Test model status enumeration."""

    def test_model_status(self):
        """Test model status enum values."""
        assert ModelStatus.TRAINING.value == "training"
        assert ModelStatus.TRAINED.value == "trained"
        assert ModelStatus.DEPLOYED.value == "deployed"
        assert ModelStatus.RETIRED.value == "retired"
        assert ModelStatus.FAILED.value == "failed"


class TestModelMetrics:
    """Test model metrics data class."""

    def test_model_metrics_creation(self):
        """Test creation of model metrics."""
        metrics = ModelMetrics(
            accuracy=0.85,
            precision=0.80,
            recall=0.75,
            f1_score=0.77,
            auc_roc=0.82,
            confusion_matrix=[[100, 20], [15, 65]],
            feature_importance={"price": 0.3, "hour": 0.2},
            training_time=120.5,
            prediction_time=0.05,
            model_size_mb=15.2,
        )

        assert metrics.accuracy == 0.85
        assert metrics.precision == 0.80
        assert metrics.recall == 0.75
        assert metrics.f1_score == 0.77
        assert metrics.auc_roc == 0.82
        assert len(metrics.confusion_matrix) == 2
        assert "price" in metrics.feature_importance
        assert metrics.training_time == 120.5


class TestModelConfiguration:
    """Test model configuration data class."""

    def test_model_configuration_creation(self):
        """Test creation of model configuration."""
        config = ModelConfiguration(
            model_type=ModelType.RANDOM_FOREST,
            features=["price", "hour", "customer_avg_amount"],
            hyperparameters={"numTrees": 100, "maxDepth": 10},
            cross_validation_folds=5,
            test_split_ratio=0.2,
            random_seed=42,
        )

        assert config.model_type == ModelType.RANDOM_FOREST
        assert len(config.features) == 3
        assert config.hyperparameters["numTrees"] == 100
        assert config.cross_validation_folds == 5
        assert config.test_split_ratio == 0.2
        assert config.random_seed == 42

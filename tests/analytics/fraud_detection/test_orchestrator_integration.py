"""
Integration tests for fraud detection orchestrator with ML models.

This module tests the complete integration between the orchestrator and ML components,
ensuring all systems work together properly.
"""

import shutil
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

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

from src.analytics.fraud_detection.feedback_loop import FeedbackLabel
from src.analytics.fraud_detection.fraud_detection_orchestrator import (
    FraudDetectionOrchestrator,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return (
        SparkSession.builder.appName("TestOrchestratorIntegration")
        .master("local[2]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def sample_transaction_stream(spark):
    """Create sample transaction stream data."""
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

    base_time = datetime.now()
    data = []

    for i in range(100):
        # Create fraud and legitimate patterns
        is_fraud = i % 10 == 0  # 10% fraud rate

        if is_fraud:
            price = 5000.0 + (i * 100)  # High amounts
            customer_id = f"customer_{i % 5}"  # High velocity
        else:
            price = 50.0 + (i * 5)  # Normal amounts
            customer_id = f"customer_{i}"

        data.append(
            (
                f"txn_{i}",
                customer_id,
                f"merchant_{i % 20}",
                price,
                "credit_card",
                base_time - timedelta(hours=i % 24),
            )
        )

    return spark.createDataFrame(data, schema)


@pytest.fixture
def orchestrator_config():
    """Configuration for orchestrator testing."""
    return {
        "stream_source_path": "test_data/transactions",
        "checkpoint_location": "test_data/checkpoints",
        "output_path": "test_data/fraud_alerts",
        "enable_ml_models": True,
        "ml_ensemble_enabled": True,
        "ml_model_threshold": 0.6,
        "rule_weight": 0.4,
        "ml_weight": 0.6,
        "enable_pattern_analysis": True,
        "enable_merchant_scoring": True,
        "enable_alert_prioritization": True,
        "ml_model_path": "test_data/models",
    }


@pytest.fixture
def orchestrator(spark, orchestrator_config):
    """Create orchestrator for testing."""
    return FraudDetectionOrchestrator(spark, orchestrator_config)


class TestOrchestratorMLIntegration:
    """Test ML integration with fraud detection orchestrator."""

    def test_orchestrator_initialization_with_ml(self, orchestrator):
        """Test orchestrator initialization with ML components."""
        assert orchestrator.spark is not None
        assert orchestrator.config["enable_ml_models"] is True

        # Check traditional components
        assert orchestrator.rules_engine is not None
        assert orchestrator.pattern_analyzer is not None
        assert orchestrator.merchant_risk_scorer is not None
        assert orchestrator.alert_prioritizer is not None

        # Check ML components
        assert orchestrator.ml_trainer is not None
        assert orchestrator.model_serving is not None
        assert orchestrator.model_monitor is not None
        assert orchestrator.feedback_loop is not None

    def test_ml_fraud_detection_application(
        self, orchestrator, sample_transaction_stream
    ):
        """Test application of ML fraud detection to transaction stream."""
        # Apply ML fraud detection
        result_df = orchestrator._apply_ml_fraud_detection(sample_transaction_stream)

        # Check ML columns are added
        expected_ml_columns = [
            "ml_fraud_probability",
            "ml_fraud_prediction",
            "ml_model_version",
            "ml_prediction_time_ms",
        ]

        for col in expected_ml_columns:
            assert col in result_df.columns

        # Check data types and reasonable values
        sample_rows = result_df.limit(5).collect()
        for row in sample_rows:
            assert 0.0 <= row.ml_fraud_probability <= 1.0
            assert row.ml_fraud_prediction in [0, 1]
            assert row.ml_model_version is not None
            assert row.ml_prediction_time_ms >= 0.0

    def test_fraud_score_combination(self, orchestrator, sample_transaction_stream):
        """Test combination of rule-based and ML-based fraud scores."""
        # Add mock rule-based fraud scores
        stream_with_rules = sample_transaction_stream.withColumn(
            "fraud_score", F.when(F.col("price") > 1000, 0.8).otherwise(0.2)
        )

        # Apply ML detection
        stream_with_ml = orchestrator._apply_ml_fraud_detection(stream_with_rules)

        # Combine scores
        combined_df = orchestrator._combine_fraud_scores(stream_with_ml)

        # Check combined score columns
        assert "combined_fraud_score" in combined_df.columns
        assert "combined_fraud_prediction" in combined_df.columns
        assert "detection_method" in combined_df.columns

        # Check combined scores are reasonable
        sample_rows = combined_df.limit(10).collect()
        for row in sample_rows:
            assert 0.0 <= row.combined_fraud_score <= 1.0
            assert row.combined_fraud_prediction in [0, 1]
            assert row.detection_method in [
                "rules_only",
                "ml_only",
                "rules_and_ml",
                "none",
            ]

            # If both rule and ML scores exist, combined should be weighted average
            if row.fraud_score is not None and row.ml_fraud_probability is not None:
                expected_combined = (row.fraud_score * 0.4) + (
                    row.ml_fraud_probability * 0.6
                )
                assert abs(row.combined_fraud_score - expected_combined) < 0.01

    def test_complete_fraud_detection_pipeline(
        self, orchestrator, sample_transaction_stream
    ):
        """Test the complete fraud detection pipeline with ML integration."""
        # Mock the pattern analyzer and rules engine
        with patch.object(
            orchestrator.pattern_analyzer, "create_comprehensive_patterns"
        ) as mock_patterns, patch.object(
            orchestrator.rules_engine, "apply_rules"
        ) as mock_rules:
            # Configure mocks
            mock_patterns.return_value = sample_transaction_stream
            mock_rules.return_value = sample_transaction_stream.withColumn(
                "fraud_score", F.when(F.col("price") > 1000, 0.9).otherwise(0.1)
            ).withColumn("triggered_rules", F.array())

            # Apply complete pipeline
            result_df = orchestrator._apply_fraud_detection_pipeline(
                sample_transaction_stream
            )

            # Check that all expected columns are present
            expected_columns = [
                "combined_fraud_score",
                "combined_fraud_prediction",
                "detection_method",
                "ml_fraud_probability",
                "ml_fraud_prediction",
                "processing_timestamp",
                "detection_version",
                "alert_id",
            ]

            for col in expected_columns:
                assert col in result_df.columns

            # Check that filters work correctly
            alerts_count = result_df.count()
            assert alerts_count > 0  # Should have some alerts

    @patch(
        "src.analytics.fraud_detection.fraud_detection_orchestrator.FraudDetectionOrchestrator._prepare_training_data_with_feedback"
    )
    def test_ml_model_training_through_orchestrator(
        self, mock_prepare_data, orchestrator, sample_transaction_stream
    ):
        """Test ML model training through orchestrator."""
        # Mock training data preparation
        mock_prepare_data.return_value = sample_transaction_stream

        # Mock file operations
        with patch.object(
            orchestrator.ml_trainer, "train_ensemble_models"
        ) as mock_train, patch.object(
            orchestrator.ml_trainer, "save_models"
        ) as mock_save, patch.object(
            orchestrator.model_serving, "deploy_model"
        ) as mock_deploy:
            # Configure mocks
            mock_train.return_value = {
                "models": {"random_forest": Mock(), "gradient_boosting": Mock()},
                "metrics": {
                    "random_forest": {"accuracy": 0.85},
                    "gradient_boosting": {"accuracy": 0.83},
                },
                "feature_columns": ["price", "hour", "customer_avg_amount"],
                "training_data_size": 1000,
                "test_data_size": 200,
            }
            mock_save.return_value = True
            mock_deploy.return_value = True

            # Train models
            results = orchestrator.train_ml_models()

            # Check results
            assert "error" not in results
            assert "models" in results
            assert len(results["models"]) == 2
            assert results["training_data_size"] == 1000

            # Check that models were saved and deployed
            mock_save.assert_called_once()
            assert mock_deploy.call_count == 2  # Two models deployed

    def test_investigator_feedback_collection(self, orchestrator):
        """Test investigator feedback collection through orchestrator."""
        success = orchestrator.collect_investigator_feedback(
            transaction_id="test_txn_001",
            prediction_id="test_pred_001",
            is_fraud=True,
            investigator_id="test_investigator",
            confidence=0.9,
            comments="Test feedback",
        )

        assert success is True

        # Check that feedback was queued
        assert not orchestrator.feedback_loop.feedback_queue.empty()

        # Get the feedback entry
        feedback_entry = orchestrator.feedback_loop.feedback_queue.get()
        assert feedback_entry.transaction_id == "test_txn_001"
        assert feedback_entry.feedback_label == FeedbackLabel.TRUE_POSITIVE
        assert feedback_entry.investigator_id == "test_investigator"
        assert feedback_entry.confidence_score == 0.9

    def test_customer_dispute_collection(self, orchestrator):
        """Test customer dispute collection through orchestrator."""
        success = orchestrator.collect_customer_dispute(
            transaction_id="test_txn_002",
            prediction_id="test_pred_002",
            dispute_type="chargeback",
            resolution="legitimate",
        )

        assert success is True

        # Check that dispute was queued
        assert not orchestrator.feedback_loop.feedback_queue.empty()

        # Get the feedback entry
        feedback_entry = orchestrator.feedback_loop.feedback_queue.get()
        assert feedback_entry.transaction_id == "test_txn_002"
        assert feedback_entry.feedback_label == FeedbackLabel.FALSE_POSITIVE

    def test_batch_updates_with_ml(self, orchestrator, sample_transaction_stream):
        """Test batch updates including ML model updates."""
        # Mock Delta table reads
        with patch.object(orchestrator.spark, "read") as mock_read:
            mock_reader = Mock()
            mock_reader.format.return_value.load.return_value = (
                sample_transaction_stream
            )
            mock_read.return_value = mock_reader

            # Mock ML update methods
            with patch.object(
                orchestrator, "_update_merchant_risk_scores"
            ) as mock_merchant, patch.object(
                orchestrator, "_update_customer_risk_profiles"
            ) as mock_customer, patch.object(
                orchestrator, "_update_rule_performance_metrics"
            ) as mock_rules, patch.object(
                orchestrator, "_update_ml_models"
            ) as mock_ml, patch.object(
                orchestrator, "_process_ml_feedback"
            ) as mock_feedback:
                # Run batch updates
                orchestrator.process_batch_updates()

                # Check that all update methods were called
                mock_merchant.assert_called_once()
                mock_customer.assert_called_once()
                mock_rules.assert_called_once()
                mock_ml.assert_called_once()
                mock_feedback.assert_called_once()

    def test_ml_model_performance_retrieval(self, orchestrator):
        """Test ML model performance metrics retrieval."""
        # Mock monitoring components
        with patch.object(
            orchestrator.model_monitor, "get_performance_trends"
        ) as mock_trends, patch.object(
            orchestrator.model_monitor, "get_monitoring_summary"
        ) as mock_summary, patch.object(
            orchestrator.model_serving, "get_serving_status"
        ) as mock_serving:
            # Configure mocks
            mock_trends.return_value = {
                "timestamps": ["2024-01-01", "2024-01-02"],
                "accuracy": [0.85, 0.87],
                "precision": [0.80, 0.82],
            }
            mock_summary.return_value = {
                "monitoring_status": "active",
                "models_monitored": ["fraud_ensemble"],
                "total_alerts": 5,
            }
            mock_serving.return_value = {
                "status": "active",
                "metrics": {"total_predictions": 1000, "successful_predictions": 995},
            }

            # Get performance metrics
            performance = orchestrator.get_ml_model_performance()

            assert "error" not in performance
            assert performance["model_name"] == "fraud_ensemble"
            assert "performance_trends" in performance
            assert "monitoring_summary" in performance
            assert "serving_status" in performance

    def test_system_status_with_ml(self, orchestrator):
        """Test system status reporting with ML components."""
        # Mock ML component status methods
        with patch.object(
            orchestrator.model_serving, "get_serving_status"
        ) as mock_serving, patch.object(
            orchestrator.model_monitor, "get_monitoring_summary"
        ) as mock_monitoring, patch.object(
            orchestrator.feedback_loop, "get_retraining_status"
        ) as mock_retraining, patch.object(
            orchestrator.feedback_loop, "get_feedback_summary"
        ) as mock_feedback:
            # Configure mocks
            mock_serving.return_value = {
                "status": "active",
                "metrics": {"total_predictions": 500},
            }
            mock_monitoring.return_value = {
                "monitoring_status": "active",
                "total_alerts": 3,
            }
            mock_retraining.return_value = {"total_jobs": 2, "active_jobs": 1}
            mock_feedback.return_value = {
                "total_feedback_entries": 10,
                "feedback_based_accuracy": 0.85,
            }

            # Get system status
            status = orchestrator.get_system_status()

            # Check traditional components
            assert "active_streams" in status
            assert "total_rules" in status
            assert "enabled_rules" in status

            # Check ML components
            assert "ml_models_loaded" in status
            assert "model_serving_status" in status
            assert "monitoring_status" in status
            assert "retraining_status" in status
            assert "feedback_summary" in status

            assert status["model_serving_status"]["status"] == "active"
            assert status["monitoring_status"]["monitoring_status"] == "active"
            assert status["retraining_status"]["total_jobs"] == 2
            assert status["feedback_summary"]["total_feedback_entries"] == 10

    def test_ml_components_disabled(self, spark):
        """Test orchestrator behavior when ML components are disabled."""
        config = {"enable_ml_models": False}
        orchestrator = FraudDetectionOrchestrator(spark, config)

        # ML components should still be initialized but not used
        assert orchestrator.ml_trainer is not None
        assert orchestrator.model_serving is not None

        # System status should reflect disabled ML
        status = orchestrator.get_system_status()
        assert "ml_models_loaded" not in status

        # ML model performance should return error
        performance = orchestrator.get_ml_model_performance()
        assert "error" in performance
        assert "disabled" in performance["error"]

    def test_error_handling_in_ml_integration(
        self, orchestrator, sample_transaction_stream
    ):
        """Test error handling in ML integration scenarios."""
        # Test ML detection with errors
        with patch.object(
            orchestrator.model_serving,
            "predict_fraud",
            side_effect=Exception("Model error"),
        ):
            result_df = orchestrator._apply_ml_fraud_detection(
                sample_transaction_stream
            )

            # Should return DataFrame with null ML columns
            sample_rows = result_df.limit(1).collect()
            assert len(sample_rows) > 0
            # ML columns should be null or have default values

        # Test score combination with missing data
        incomplete_df = sample_transaction_stream.withColumn(
            "fraud_score", F.lit(None)
        ).withColumn("ml_fraud_probability", F.lit(None))

        combined_df = orchestrator._combine_fraud_scores(incomplete_df)

        # Should handle null values gracefully
        sample_rows = combined_df.limit(1).collect()
        assert len(sample_rows) > 0

    def test_configuration_integration(self, spark):
        """Test configuration integration between components."""
        custom_config = {
            "enable_ml_models": True,
            "ml_ensemble_enabled": True,
            "ml_model_threshold": 0.7,
            "rule_weight": 0.3,
            "ml_weight": 0.7,
            "ml_config": {"custom_ml_setting": True},
            "serving_config": {"custom_serving_setting": True},
            "monitoring_config": {"custom_monitoring_setting": True},
            "feedback_config": {"custom_feedback_setting": True},
        }

        orchestrator = FraudDetectionOrchestrator(spark, custom_config)

        # Check that configuration is properly passed
        assert orchestrator.config["ml_model_threshold"] == 0.7
        assert orchestrator.config["rule_weight"] == 0.3
        assert orchestrator.config["ml_weight"] == 0.7

        # Update configuration
        new_config = {"ml_model_threshold": 0.8}
        orchestrator.update_configuration(new_config)

        assert orchestrator.config["ml_model_threshold"] == 0.8

    def test_end_to_end_workflow(self, orchestrator, sample_transaction_stream):
        """Test end-to-end workflow with ML integration."""
        # This is a comprehensive test that simulates a complete workflow

        # Step 1: Train models
        with patch.object(
            orchestrator.ml_trainer, "train_ensemble_models"
        ) as mock_train, patch.object(
            orchestrator.model_serving, "deploy_model"
        ) as mock_deploy:
            mock_train.return_value = {
                "models": {"random_forest": Mock()},
                "metrics": {"random_forest": {"accuracy": 0.85}},
                "feature_columns": ["price"],
                "training_data_size": 1000,
                "test_data_size": 200,
            }
            mock_deploy.return_value = True

            results = orchestrator.train_ml_models()
            assert "error" not in results

        # Step 2: Process transactions through pipeline
        with patch.object(
            orchestrator.pattern_analyzer, "create_comprehensive_patterns"
        ) as mock_patterns, patch.object(
            orchestrator.rules_engine, "apply_rules"
        ) as mock_rules:
            mock_patterns.return_value = sample_transaction_stream
            mock_rules.return_value = sample_transaction_stream.withColumn(
                "fraud_score", F.lit(0.5)
            ).withColumn("triggered_rules", F.array())

            pipeline_result = orchestrator._apply_fraud_detection_pipeline(
                sample_transaction_stream
            )
            assert pipeline_result.count() > 0

        # Step 3: Collect feedback
        feedback_success = orchestrator.collect_investigator_feedback(
            "txn_001", "pred_001", True, "inv_001", 0.9
        )
        assert feedback_success is True

        # Step 4: Get system status
        status = orchestrator.get_system_status()
        assert "ml_models_loaded" in status

        # Step 5: Get performance metrics
        with patch.object(
            orchestrator.model_monitor, "get_performance_trends"
        ) as mock_trends:
            mock_trends.return_value = {"accuracy": [0.85, 0.87]}
            performance = orchestrator.get_ml_model_performance()
            assert "error" not in performance

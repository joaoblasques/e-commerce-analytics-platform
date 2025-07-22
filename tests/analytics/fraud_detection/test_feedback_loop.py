"""
Tests for ML model feedback loop system.

This module tests the continuous learning and feedback system for fraud detection models,
including investigator feedback collection, automated retraining, and performance optimization.
"""

import queue
import threading
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

from src.analytics.fraud_detection.feedback_loop import (
    FeedbackEntry,
    FeedbackLabel,
    FeedbackLoop,
    FeedbackType,
    RetrainingJob,
    RetrainingTrigger,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return (
        SparkSession.builder.appName("TestFeedbackLoop")
        .master("local[2]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def feedback_loop(spark):
    """Create feedback loop system for testing."""
    config = {
        "feedback_collection": {
            "investigator_feedback_weight": 1.0,
            "customer_dispute_weight": 0.8,
            "automated_evaluation_weight": 0.6,
            "min_confidence_threshold": 0.7,
            "max_feedback_age_days": 90,
        },
        "retraining_triggers": {
            "min_feedback_count": 10,  # Lower for testing
            "performance_degradation_threshold": 0.05,
            "data_drift_threshold": 0.1,
            "scheduled_interval_days": 1,  # Lower for testing
            "max_false_positive_rate": 0.15,
            "min_precision_threshold": 0.75,
        },
        "model_selection": {
            "validation_split_ratio": 0.2,
            "min_improvement_threshold": 0.02,
            "a_b_test_duration_days": 1,  # Lower for testing
            "traffic_split_percentage": 10,
            "champion_challenger_enabled": True,
        },
        "data_management": {
            "training_data_retention_months": 12,
            "feedback_data_retention_months": 24,
            "incremental_learning_enabled": True,
            "sample_balancing_enabled": True,
        },
    }
    return FeedbackLoop(spark, config)


@pytest.fixture
def mock_components():
    """Create mock components for feedback loop testing."""
    mock_trainer = Mock()
    mock_trainer.train_ensemble_models.return_value = {
        "models": {"random_forest": Mock()},
        "metrics": {"accuracy": 0.85, "f1_score": 0.82},
    }
    mock_trainer.save_models.return_value = True

    mock_monitor = Mock()
    mock_monitor.evaluate_model_performance.return_value = {
        "accuracy": 0.87,
        "precision": 0.84,
        "recall": 0.80,
        "f1_score": 0.82,
    }

    mock_serving = Mock()
    mock_serving.deploy_model.return_value = True

    return {"trainer": mock_trainer, "monitor": mock_monitor, "serving": mock_serving}


class TestFeedbackEntry:
    """Test feedback entry data class."""

    def test_feedback_entry_creation(self):
        """Test creation of feedback entry."""
        entry = FeedbackEntry(
            feedback_id="fb_123",
            transaction_id="txn_456",
            prediction_id="pred_789",
            feedback_type=FeedbackType.INVESTIGATOR_FEEDBACK,
            feedback_label=FeedbackLabel.TRUE_POSITIVE,
            investigator_id="inv_001",
            confidence_score=0.9,
            comments="Clear fraud pattern detected",
            timestamp=datetime.now(),
            model_name="random_forest",
            model_version="v1.0.0",
            prediction_probability=0.85,
            actual_outcome=True,
            investigation_time_hours=2.5,
        )

        assert entry.feedback_id == "fb_123"
        assert entry.transaction_id == "txn_456"
        assert entry.feedback_type == FeedbackType.INVESTIGATOR_FEEDBACK
        assert entry.feedback_label == FeedbackLabel.TRUE_POSITIVE
        assert entry.confidence_score == 0.9
        assert entry.actual_outcome is True
        assert entry.investigation_time_hours == 2.5


class TestRetrainingJob:
    """Test retraining job data class."""

    def test_retraining_job_creation(self):
        """Test creation of retraining job."""
        job = RetrainingJob(
            job_id="job_123",
            model_name="fraud_detection_ensemble",
            trigger=RetrainingTrigger.FEEDBACK_THRESHOLD,
            triggered_at=datetime.now(),
            status="running",
            training_data_size=10000,
            validation_metrics={"accuracy": 0.88, "f1_score": 0.85},
            completion_time=datetime.now() + timedelta(hours=2),
            error_message=None,
        )

        assert job.job_id == "job_123"
        assert job.model_name == "fraud_detection_ensemble"
        assert job.trigger == RetrainingTrigger.FEEDBACK_THRESHOLD
        assert job.status == "running"
        assert job.training_data_size == 10000
        assert job.validation_metrics["accuracy"] == 0.88
        assert job.error_message is None


class TestFeedbackLoop:
    """Test cases for feedback loop system."""

    def test_initialization(self, feedback_loop):
        """Test feedback loop initialization."""
        assert feedback_loop.spark is not None
        assert feedback_loop.config is not None
        assert isinstance(feedback_loop.feedback_entries, list)
        assert isinstance(feedback_loop.feedback_queue, queue.Queue)
        assert isinstance(feedback_loop.retraining_jobs, dict)
        assert feedback_loop._running is False

        # Check component initialization
        assert feedback_loop.model_trainer is not None
        assert feedback_loop.performance_monitor is not None
        assert feedback_loop.model_serving is not None

    def test_default_config(self, feedback_loop):
        """Test default configuration structure."""
        config = feedback_loop.config

        assert "feedback_collection" in config
        assert "retraining_triggers" in config
        assert "model_selection" in config
        assert "data_management" in config

        # Check feedback collection config
        feedback_config = config["feedback_collection"]
        assert "investigator_feedback_weight" in feedback_config
        assert "min_confidence_threshold" in feedback_config

        # Check retraining triggers
        trigger_config = config["retraining_triggers"]
        assert "min_feedback_count" in trigger_config
        assert "performance_degradation_threshold" in trigger_config

    def test_collect_investigator_feedback_success(self, feedback_loop):
        """Test successful investigator feedback collection."""
        success = feedback_loop.collect_investigator_feedback(
            transaction_id="txn_123",
            prediction_id="pred_456",
            feedback_label=FeedbackLabel.TRUE_POSITIVE,
            investigator_id="inv_001",
            confidence_score=0.9,
            comments="Clear fraud indicators",
        )

        assert success is True

        # Check feedback was queued
        assert not feedback_loop.feedback_queue.empty()

        # Process the feedback
        feedback_entry = feedback_loop.feedback_queue.get()
        assert feedback_entry.transaction_id == "txn_123"
        assert feedback_entry.feedback_label == FeedbackLabel.TRUE_POSITIVE
        assert feedback_entry.investigator_id == "inv_001"
        assert feedback_entry.confidence_score == 0.9
        assert feedback_entry.feedback_type == FeedbackType.INVESTIGATOR_FEEDBACK

    def test_collect_investigator_feedback_failure(self, feedback_loop):
        """Test investigator feedback collection with invalid data."""
        with patch.object(
            feedback_loop.feedback_queue, "put", side_effect=Exception("Queue error")
        ):
            success = feedback_loop.collect_investigator_feedback(
                transaction_id="txn_123",
                prediction_id="pred_456",
                feedback_label=FeedbackLabel.TRUE_POSITIVE,
                investigator_id="inv_001",
                confidence_score=0.9,
            )

            assert success is False

    def test_collect_customer_dispute_fraud_confirmed(self, feedback_loop):
        """Test customer dispute collection with fraud confirmed."""
        success = feedback_loop.collect_customer_dispute(
            transaction_id="txn_123",
            prediction_id="pred_456",
            dispute_type="chargeback",
            resolution="fraud_confirmed",
        )

        assert success is True

        # Check feedback was queued
        feedback_entry = feedback_loop.feedback_queue.get()
        assert feedback_entry.transaction_id == "txn_123"
        assert feedback_entry.feedback_label == FeedbackLabel.TRUE_POSITIVE
        assert feedback_entry.feedback_type == FeedbackType.CUSTOMER_DISPUTE
        assert feedback_entry.confidence_score == 0.9  # High confidence for disputes

    def test_collect_customer_dispute_false_positive(self, feedback_loop):
        """Test customer dispute collection with false positive."""
        success = feedback_loop.collect_customer_dispute(
            transaction_id="txn_123",
            prediction_id="pred_456",
            dispute_type="dispute",
            resolution="legitimate",
        )

        assert success is True

        feedback_entry = feedback_loop.feedback_queue.get()
        assert feedback_entry.feedback_label == FeedbackLabel.FALSE_POSITIVE
        assert feedback_entry.actual_outcome is False

    def test_collect_customer_dispute_uncertain(self, feedback_loop):
        """Test customer dispute collection with uncertain resolution."""
        success = feedback_loop.collect_customer_dispute(
            transaction_id="txn_123",
            prediction_id="pred_456",
            dispute_type="inquiry",
            resolution="under_review",
        )

        assert success is True

        feedback_entry = feedback_loop.feedback_queue.get()
        assert feedback_entry.feedback_label == FeedbackLabel.UNCERTAIN

    def test_feedback_processing_lifecycle(self, feedback_loop):
        """Test complete feedback processing lifecycle."""
        # Start feedback processing
        feedback_loop.start_feedback_processing()
        assert feedback_loop._running is True
        assert feedback_loop.feedback_processor_thread is not None
        assert feedback_loop.retraining_scheduler_thread is not None

        # Add some feedback
        feedback_loop.collect_investigator_feedback(
            "txn_123", "pred_456", FeedbackLabel.TRUE_POSITIVE, "inv_001", 0.9
        )

        # Give threads time to process
        time.sleep(0.2)

        # Stop processing
        feedback_loop.stop_feedback_processing()
        assert feedback_loop._running is False

    def test_feedback_entry_processing(self, feedback_loop):
        """Test individual feedback entry processing."""
        feedback_entry = FeedbackEntry(
            feedback_id="test_fb",
            transaction_id="txn_123",
            prediction_id="pred_456",
            feedback_type=FeedbackType.INVESTIGATOR_FEEDBACK,
            feedback_label=FeedbackLabel.TRUE_POSITIVE,
            investigator_id="inv_001",
            confidence_score=0.9,
            comments=None,
            timestamp=datetime.now(),
            model_name="unknown",
            model_version="unknown",
            prediction_probability=0.0,
            actual_outcome=True,
            investigation_time_hours=None,
        )

        initial_count = len(feedback_loop.feedback_entries)
        feedback_loop._process_feedback_entry(feedback_entry)

        # Check feedback was stored
        assert len(feedback_loop.feedback_entries) == initial_count + 1

        # Check feedback was enriched
        processed_entry = feedback_loop.feedback_entries[-1]
        assert processed_entry.model_name != "unknown"
        assert processed_entry.model_version != "unknown"
        assert processed_entry.prediction_probability > 0.0

    def test_retraining_trigger_feedback_threshold(self, feedback_loop):
        """Test retraining trigger based on feedback count."""
        # Add enough feedback to trigger retraining
        for i in range(15):  # Above min_feedback_count of 10
            feedback_entry = FeedbackEntry(
                feedback_id=f"fb_{i}",
                transaction_id=f"txn_{i}",
                prediction_id=f"pred_{i}",
                feedback_type=FeedbackType.INVESTIGATOR_FEEDBACK,
                feedback_label=FeedbackLabel.TRUE_POSITIVE,
                investigator_id="inv_001",
                confidence_score=0.9,
                comments=None,
                timestamp=datetime.now() - timedelta(days=i % 5),  # Within 30 days
                model_name="random_forest",
                model_version="v1.0.0",
                prediction_probability=0.8,
                actual_outcome=True,
                investigation_time_hours=None,
            )
            feedback_loop.feedback_entries.append(feedback_entry)

        initial_job_count = len(feedback_loop.retraining_jobs)
        feedback_loop._check_retraining_triggers()

        # Should trigger retraining
        assert len(feedback_loop.retraining_jobs) > initial_job_count

        # Check job details
        job = list(feedback_loop.retraining_jobs.values())[-1]
        assert job.trigger == RetrainingTrigger.FEEDBACK_THRESHOLD
        assert job.status == "queued"

    def test_retraining_trigger_performance_degradation(self, feedback_loop):
        """Test retraining trigger based on performance degradation."""
        # Add feedback with high false positive rate
        for i in range(15):
            # 60% false positives (above 15% threshold)
            label = (
                FeedbackLabel.FALSE_POSITIVE if i < 9 else FeedbackLabel.TRUE_POSITIVE
            )

            feedback_entry = FeedbackEntry(
                feedback_id=f"fb_{i}",
                transaction_id=f"txn_{i}",
                prediction_id=f"pred_{i}",
                feedback_type=FeedbackType.INVESTIGATOR_FEEDBACK,
                feedback_label=label,
                investigator_id="inv_001",
                confidence_score=0.8,
                comments=None,
                timestamp=datetime.now() - timedelta(days=i % 5),
                model_name="random_forest",
                model_version="v1.0.0",
                prediction_probability=0.7,
                actual_outcome=label == FeedbackLabel.TRUE_POSITIVE,
                investigation_time_hours=None,
            )
            feedback_loop.feedback_entries.append(feedback_entry)

        initial_job_count = len(feedback_loop.retraining_jobs)
        feedback_loop._check_retraining_triggers()

        # Should trigger retraining due to high false positive rate
        assert len(feedback_loop.retraining_jobs) > initial_job_count

    def test_retraining_trigger_scheduled(self, feedback_loop):
        """Test scheduled retraining trigger."""
        # Should trigger initial retraining (no previous training)
        initial_job_count = len(feedback_loop.retraining_jobs)
        feedback_loop._check_retraining_triggers()

        assert len(feedback_loop.retraining_jobs) > initial_job_count

        # Check it's a scheduled trigger
        job = list(feedback_loop.retraining_jobs.values())[-1]
        assert job.trigger == RetrainingTrigger.SCHEDULED

    def test_has_recent_retraining(self, feedback_loop):
        """Test recent retraining detection."""
        # Add recent retraining job
        job = RetrainingJob(
            job_id="recent_job",
            model_name="test_model",
            trigger=RetrainingTrigger.FEEDBACK_THRESHOLD,
            triggered_at=datetime.now() - timedelta(hours=2),  # Recent
            status="completed",
            training_data_size=1000,
            validation_metrics={},
        )
        feedback_loop.retraining_jobs["recent_job"] = job

        # Should detect recent retraining
        has_recent = feedback_loop._has_recent_retraining("feedback_threshold")
        assert has_recent is True

        # Should not detect for different trigger type
        has_recent_other = feedback_loop._has_recent_retraining("scheduled")
        assert has_recent_other is False

    def test_should_schedule_retraining_first_time(self, feedback_loop):
        """Test scheduled retraining for first time."""
        # No previous scheduled retraining
        should_schedule = feedback_loop._should_schedule_retraining()
        assert should_schedule is True

    def test_should_schedule_retraining_interval(self, feedback_loop):
        """Test scheduled retraining based on interval."""
        # Add old scheduled retraining job
        old_job = RetrainingJob(
            job_id="old_job",
            model_name="test_model",
            trigger=RetrainingTrigger.SCHEDULED,
            triggered_at=datetime.now() - timedelta(days=2),  # Beyond 1-day interval
            status="completed",
            training_data_size=1000,
            validation_metrics={},
        )
        feedback_loop.retraining_jobs["old_job"] = old_job

        should_schedule = feedback_loop._should_schedule_retraining()
        assert should_schedule is True

        # Add recent scheduled retraining job
        recent_job = RetrainingJob(
            job_id="recent_job",
            model_name="test_model",
            trigger=RetrainingTrigger.SCHEDULED,
            triggered_at=datetime.now() - timedelta(hours=2),  # Within 1-day interval
            status="completed",
            training_data_size=1000,
            validation_metrics={},
        )
        feedback_loop.retraining_jobs["recent_job"] = recent_job

        should_schedule = feedback_loop._should_schedule_retraining()
        assert should_schedule is False

    def test_trigger_retraining(self, feedback_loop):
        """Test retraining trigger process."""
        initial_job_count = len(feedback_loop.retraining_jobs)

        job_id = feedback_loop._trigger_retraining(RetrainingTrigger.MANUAL)

        assert job_id is not None
        assert len(feedback_loop.retraining_jobs) == initial_job_count + 1

        # Check job details
        job = feedback_loop.retraining_jobs[job_id]
        assert job.job_id == job_id
        assert job.trigger == RetrainingTrigger.MANUAL
        assert job.status == "queued"

        # Give thread time to start (it will fail without proper setup, but that's expected)
        time.sleep(0.1)

    def test_prepare_training_data_with_feedback(self, feedback_loop, spark):
        """Test training data preparation with feedback."""
        # Add some feedback entries
        for i in range(5):
            feedback_entry = FeedbackEntry(
                feedback_id=f"fb_{i}",
                transaction_id=f"txn_{i}",
                prediction_id=f"pred_{i}",
                feedback_type=FeedbackType.INVESTIGATOR_FEEDBACK,
                feedback_label=FeedbackLabel.TRUE_POSITIVE
                if i < 2
                else FeedbackLabel.FALSE_POSITIVE,
                investigator_id="inv_001",
                confidence_score=0.8,
                comments=None,
                timestamp=datetime.now(),
                model_name="random_forest",
                model_version="v1.0.0",
                prediction_probability=0.7,
                actual_outcome=i < 2,
                investigation_time_hours=None,
            )
            feedback_loop.feedback_entries.append(feedback_entry)

        # Mock SQL execution
        with patch.object(spark, "sql") as mock_sql:
            # Mock empty DataFrame for base query
            mock_df = spark.createDataFrame(
                [],
                StructType(
                    [
                        StructField("transaction_id", StringType(), True),
                        StructField("customer_id", StringType(), True),
                        StructField("merchant_id", StringType(), True),
                        StructField("price", DoubleType(), True),
                        StructField("payment_method", StringType(), True),
                        StructField("timestamp", TimestampType(), True),
                        StructField("is_fraud", IntegerType(), True),
                    ]
                ),
            )
            mock_sql.return_value = mock_df

            training_df = feedback_loop._prepare_training_data_with_feedback()

            assert training_df is not None
            assert "is_fraud" in training_df.columns

    def test_evaluate_retrained_models(self, feedback_loop):
        """Test retrained model evaluation."""
        training_results = {"models": {"random_forest": Mock()}}

        metrics = feedback_loop._evaluate_retrained_models(training_results)

        assert isinstance(metrics, dict)
        assert "accuracy" in metrics
        assert "precision" in metrics
        assert "recall" in metrics
        assert "f1_score" in metrics
        assert "auc_roc" in metrics

        # Check metric ranges
        for metric_name, value in metrics.items():
            assert 0.0 <= value <= 1.0

    def test_should_deploy_retrained_model(self, feedback_loop):
        """Test retrained model deployment decision."""
        # Test with improvement above threshold
        good_metrics = {"f1_score": 0.82}  # Above current 0.78 + 0.02 threshold
        should_deploy = feedback_loop._should_deploy_retrained_model(good_metrics)
        assert should_deploy is True

        # Test with improvement below threshold
        poor_metrics = {"f1_score": 0.79}  # Below threshold
        should_deploy = feedback_loop._should_deploy_retrained_model(poor_metrics)
        assert should_deploy is False

        # Test with missing metric
        empty_metrics = {}
        should_deploy = feedback_loop._should_deploy_retrained_model(empty_metrics)
        assert should_deploy is False

    @patch(
        "src.analytics.fraud_detection.feedback_loop.FeedbackLoop._deploy_retrained_model"
    )
    def test_deploy_retrained_model(self, mock_deploy, feedback_loop):
        """Test retrained model deployment."""
        training_results = {"models": {"random_forest": Mock()}}
        job_id = "test_job_123"

        # Mock model_trainer.save_models and model_serving.deploy_model
        with patch.object(
            feedback_loop.model_trainer, "save_models"
        ) as mock_save, patch.object(
            feedback_loop.model_serving, "deploy_model", return_value=True
        ) as mock_deploy_serving:
            feedback_loop._deploy_retrained_model(training_results, job_id)

            mock_save.assert_called_once()
            mock_deploy_serving.assert_called_once()

    def test_cleanup_old_jobs(self, feedback_loop):
        """Test cleanup of old retraining jobs."""
        # Add old completed job
        old_job = RetrainingJob(
            job_id="old_job",
            model_name="test_model",
            trigger=RetrainingTrigger.SCHEDULED,
            triggered_at=datetime.now() - timedelta(days=40),
            status="completed",
            training_data_size=1000,
            validation_metrics={},
            completion_time=datetime.now() - timedelta(days=35),  # Old completion
        )
        feedback_loop.retraining_jobs["old_job"] = old_job

        # Add recent job
        recent_job = RetrainingJob(
            job_id="recent_job",
            model_name="test_model",
            trigger=RetrainingTrigger.MANUAL,
            triggered_at=datetime.now() - timedelta(days=5),
            status="completed",
            training_data_size=1000,
            validation_metrics={},
            completion_time=datetime.now() - timedelta(days=2),  # Recent completion
        )
        feedback_loop.retraining_jobs["recent_job"] = recent_job

        initial_count = len(feedback_loop.retraining_jobs)
        feedback_loop._cleanup_old_jobs()

        # Should remove old job, keep recent job
        assert len(feedback_loop.retraining_jobs) == initial_count - 1
        assert "old_job" not in feedback_loop.retraining_jobs
        assert "recent_job" in feedback_loop.retraining_jobs

    def test_feedback_summary(self, feedback_loop):
        """Test feedback summary generation."""
        # Add various types of feedback
        feedback_types = [
            (FeedbackType.INVESTIGATOR_FEEDBACK, FeedbackLabel.TRUE_POSITIVE),
            (FeedbackType.INVESTIGATOR_FEEDBACK, FeedbackLabel.FALSE_POSITIVE),
            (FeedbackType.CUSTOMER_DISPUTE, FeedbackLabel.TRUE_POSITIVE),
            (FeedbackType.CUSTOMER_DISPUTE, FeedbackLabel.FALSE_POSITIVE),
            (FeedbackType.AUTOMATED_EVALUATION, FeedbackLabel.TRUE_NEGATIVE),
        ]

        for i, (fb_type, fb_label) in enumerate(feedback_types):
            feedback_entry = FeedbackEntry(
                feedback_id=f"fb_{i}",
                transaction_id=f"txn_{i}",
                prediction_id=f"pred_{i}",
                feedback_type=fb_type,
                feedback_label=fb_label,
                investigator_id="inv_001"
                if fb_type == FeedbackType.INVESTIGATOR_FEEDBACK
                else None,
                confidence_score=0.8,
                comments=None,
                timestamp=datetime.now() - timedelta(days=i),
                model_name="random_forest",
                model_version="v1.0.0",
                prediction_probability=0.7,
                actual_outcome=fb_label
                in [FeedbackLabel.TRUE_POSITIVE, FeedbackLabel.TRUE_NEGATIVE],
                investigation_time_hours=2.0,
            )
            feedback_loop.feedback_entries.append(feedback_entry)

        summary = feedback_loop.get_feedback_summary(days=30)

        assert "time_period_days" in summary
        assert "total_feedback_entries" in summary
        assert "feedback_by_type" in summary
        assert "feedback_by_label" in summary
        assert "feedback_based_accuracy" in summary
        assert "average_confidence" in summary
        assert "unique_investigators" in summary

        assert summary["total_feedback_entries"] == 5
        assert summary["unique_investigators"] == 1
        assert 0.0 <= summary["feedback_based_accuracy"] <= 1.0
        assert 0.0 <= summary["average_confidence"] <= 1.0

    def test_feedback_summary_empty(self, feedback_loop):
        """Test feedback summary with no data."""
        summary = feedback_loop.get_feedback_summary(days=30)

        assert "message" in summary
        assert "No feedback found" in summary["message"]

    def test_retraining_status(self, feedback_loop):
        """Test retraining status reporting."""
        # Add various job statuses
        jobs = [
            ("job_1", "queued"),
            ("job_2", "running"),
            ("job_3", "completed_deployed"),
            ("job_4", "completed_not_deployed"),
            ("job_5", "failed"),
        ]

        for job_id, status in jobs:
            job = RetrainingJob(
                job_id=job_id,
                model_name="test_model",
                trigger=RetrainingTrigger.MANUAL,
                triggered_at=datetime.now() - timedelta(hours=1),
                status=status,
                training_data_size=1000,
                validation_metrics={},
            )
            feedback_loop.retraining_jobs[job_id] = job

        status_report = feedback_loop.get_retraining_status()

        assert "total_jobs" in status_report
        assert "active_jobs" in status_report
        assert "completed_jobs" in status_report
        assert "failed_jobs" in status_report
        assert "recent_jobs" in status_report

        assert status_report["total_jobs"] == 5
        assert status_report["active_jobs"] == 2  # queued + running
        assert status_report["completed_jobs"] == 2  # both completed statuses
        assert status_report["failed_jobs"] == 1
        assert (
            len(status_report["recent_jobs"]) <= 5
        )  # Limited to 10, should show all 5

    def test_retraining_callback(self, feedback_loop):
        """Test retraining completion callbacks."""
        callback_called = []

        def test_callback(job):
            callback_called.append(job)

        feedback_loop.add_retraining_callback(test_callback)

        # Simulate job completion (would normally be called by _execute_retraining)
        test_job = RetrainingJob(
            job_id="test_job",
            model_name="test_model",
            trigger=RetrainingTrigger.MANUAL,
            triggered_at=datetime.now(),
            status="completed",
            training_data_size=1000,
            validation_metrics={"accuracy": 0.85},
        )

        # Manually call callbacks (simulating completion)
        for callback in feedback_loop.retraining_callbacks:
            callback(test_job)

        assert len(callback_called) == 1
        assert callback_called[0] == test_job

    def test_edge_cases_and_error_handling(self, feedback_loop):
        """Test edge cases and error handling."""
        # Test with empty feedback queue
        feedback_loop._check_retraining_triggers()  # Should not crash

        # Test summary with future date range
        summary = feedback_loop.get_feedback_summary(days=-1)
        assert summary["total_feedback_entries"] == 0

        # Test retraining status with empty jobs
        status = feedback_loop.get_retraining_status()
        assert status["total_jobs"] == 0
        assert status["active_jobs"] == 0


class TestEnumerations:
    """Test feedback loop enumeration classes."""

    def test_feedback_type(self):
        """Test feedback type enum values."""
        assert FeedbackType.INVESTIGATOR_FEEDBACK.value == "investigator_feedback"
        assert FeedbackType.CUSTOMER_DISPUTE.value == "customer_dispute"
        assert FeedbackType.REGULATORY_ACTION.value == "regulatory_action"
        assert FeedbackType.AUTOMATED_EVALUATION.value == "automated_evaluation"
        assert FeedbackType.PERFORMANCE_DEGRADATION.value == "performance_degradation"

    def test_feedback_label(self):
        """Test feedback label enum values."""
        assert FeedbackLabel.TRUE_POSITIVE.value == "true_positive"
        assert FeedbackLabel.FALSE_POSITIVE.value == "false_positive"
        assert FeedbackLabel.TRUE_NEGATIVE.value == "true_negative"
        assert FeedbackLabel.FALSE_NEGATIVE.value == "false_negative"
        assert FeedbackLabel.UNCERTAIN.value == "uncertain"

    def test_retraining_trigger(self):
        """Test retraining trigger enum values."""
        assert RetrainingTrigger.SCHEDULED.value == "scheduled"
        assert (
            RetrainingTrigger.PERFORMANCE_DEGRADATION.value == "performance_degradation"
        )
        assert RetrainingTrigger.DATA_DRIFT.value == "data_drift"
        assert RetrainingTrigger.FEEDBACK_THRESHOLD.value == "feedback_threshold"
        assert RetrainingTrigger.MANUAL.value == "manual"

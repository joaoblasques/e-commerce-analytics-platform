"""
Feedback loop system for continuous model improvement in fraud detection.

This module implements automated feedback collection, analysis, and model retraining
based on real-world performance and investigator feedback.
"""

import logging
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from .ml_models import MLFraudModelTrainer
from .model_monitoring import ModelPerformanceMonitor
from .model_serving import ModelServingPipeline


class FeedbackType(Enum):
    """Types of feedback for model improvement."""

    INVESTIGATOR_FEEDBACK = "investigator_feedback"
    CUSTOMER_DISPUTE = "customer_dispute"
    REGULATORY_ACTION = "regulatory_action"
    AUTOMATED_EVALUATION = "automated_evaluation"
    PERFORMANCE_DEGRADATION = "performance_degradation"


class FeedbackLabel(Enum):
    """Feedback labels for model predictions."""

    TRUE_POSITIVE = "true_positive"  # Correctly identified fraud
    FALSE_POSITIVE = "false_positive"  # Incorrectly flagged as fraud
    TRUE_NEGATIVE = "true_negative"  # Correctly identified as legitimate
    FALSE_NEGATIVE = "false_negative"  # Missed fraud
    UNCERTAIN = "uncertain"  # Requires further investigation


class RetrainingTrigger(Enum):
    """Triggers for model retraining."""

    SCHEDULED = "scheduled"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    DATA_DRIFT = "data_drift"
    FEEDBACK_THRESHOLD = "feedback_threshold"
    MANUAL = "manual"


@dataclass
class FeedbackEntry:
    """Individual feedback entry."""

    feedback_id: str
    transaction_id: str
    prediction_id: str
    feedback_type: FeedbackType
    feedback_label: FeedbackLabel
    investigator_id: Optional[str]
    confidence_score: float
    comments: Optional[str]
    timestamp: datetime
    model_name: str
    model_version: str
    prediction_probability: float
    actual_outcome: Optional[bool]
    investigation_time_hours: Optional[float]


@dataclass
class RetrainingJob:
    """Model retraining job information."""

    job_id: str
    model_name: str
    trigger: RetrainingTrigger
    triggered_at: datetime
    status: str
    training_data_size: int
    validation_metrics: Dict[str, float]
    completion_time: Optional[datetime] = None
    error_message: Optional[str] = None


class FeedbackLoop:
    """
    Continuous feedback loop for model improvement.

    Features:
    - Investigator feedback collection
    - Automated performance evaluation
    - Model retraining triggers
    - A/B testing for model comparison
    - Performance-based model selection
    - Continuous learning pipeline
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """Initialize the feedback loop system."""
        self.spark = spark
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.model_trainer = MLFraudModelTrainer(spark)
        self.performance_monitor = ModelPerformanceMonitor(spark)
        self.model_serving = ModelServingPipeline(spark)

        # Feedback storage
        self.feedback_entries: List[FeedbackEntry] = []
        self.feedback_queue = queue.Queue(maxsize=10000)

        # Retraining management
        self.retraining_jobs: Dict[str, RetrainingJob] = {}
        self.retraining_schedule = {}

        # Background processing
        self.feedback_processor_thread = None
        self.retraining_scheduler_thread = None
        self._running = False

        # Callbacks
        self.retraining_callbacks: List[callable] = []

    def _get_default_config(self) -> Dict:
        """Get default feedback loop configuration."""
        return {
            "feedback_collection": {
                "investigator_feedback_weight": 1.0,
                "customer_dispute_weight": 0.8,
                "automated_evaluation_weight": 0.6,
                "min_confidence_threshold": 0.7,
                "max_feedback_age_days": 90,
            },
            "retraining_triggers": {
                "min_feedback_count": 1000,
                "performance_degradation_threshold": 0.05,
                "data_drift_threshold": 0.1,
                "scheduled_interval_days": 30,
                "max_false_positive_rate": 0.15,
                "min_precision_threshold": 0.75,
            },
            "model_selection": {
                "validation_split_ratio": 0.2,
                "min_improvement_threshold": 0.02,
                "a_b_test_duration_days": 7,
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

    def collect_investigator_feedback(
        self,
        transaction_id: str,
        prediction_id: str,
        feedback_label: FeedbackLabel,
        investigator_id: str,
        confidence_score: float,
        comments: Optional[str] = None,
    ) -> bool:
        """
        Collect feedback from fraud investigators.

        Args:
            transaction_id: ID of the transaction
            prediction_id: ID of the prediction
            feedback_label: Investigator's assessment of the prediction
            investigator_id: ID of the investigator
            confidence_score: Confidence in the feedback (0-1)
            comments: Optional comments

        Returns:
            True if feedback collected successfully
        """
        try:
            feedback_entry = FeedbackEntry(
                feedback_id=f"inv_{int(time.time() * 1000)}_{hash(transaction_id) % 10000}",
                transaction_id=transaction_id,
                prediction_id=prediction_id,
                feedback_type=FeedbackType.INVESTIGATOR_FEEDBACK,
                feedback_label=feedback_label,
                investigator_id=investigator_id,
                confidence_score=confidence_score,
                comments=comments,
                timestamp=datetime.now(),
                model_name="unknown",  # Would be filled from prediction record
                model_version="unknown",
                prediction_probability=0.0,  # Would be filled from prediction record
                actual_outcome=feedback_label
                in [FeedbackLabel.TRUE_POSITIVE, FeedbackLabel.TRUE_NEGATIVE],
                investigation_time_hours=None,
            )

            # Add to processing queue
            self.feedback_queue.put(feedback_entry)

            self.logger.info(
                f"Collected investigator feedback: {feedback_label.value} for transaction {transaction_id}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to collect investigator feedback: {e}")
            return False

    def collect_customer_dispute(
        self,
        transaction_id: str,
        prediction_id: str,
        dispute_type: str,
        resolution: str,
    ) -> bool:
        """
        Collect customer dispute information.

        Args:
            transaction_id: ID of the disputed transaction
            prediction_id: ID of the prediction
            dispute_type: Type of dispute
            resolution: Final resolution of the dispute

        Returns:
            True if dispute collected successfully
        """
        try:
            # Map dispute resolution to feedback label
            if resolution.lower() in ["fraud_confirmed", "confirmed"]:
                feedback_label = FeedbackLabel.TRUE_POSITIVE
            elif resolution.lower() in [
                "false_positive",
                "legitimate",
                "resolved_customer",
            ]:
                feedback_label = FeedbackLabel.FALSE_POSITIVE
            else:
                feedback_label = FeedbackLabel.UNCERTAIN

            feedback_entry = FeedbackEntry(
                feedback_id=f"disp_{int(time.time() * 1000)}_{hash(transaction_id) % 10000}",
                transaction_id=transaction_id,
                prediction_id=prediction_id,
                feedback_type=FeedbackType.CUSTOMER_DISPUTE,
                feedback_label=feedback_label,
                investigator_id=None,
                confidence_score=0.9,  # High confidence in customer disputes
                comments=f"Dispute type: {dispute_type}, Resolution: {resolution}",
                timestamp=datetime.now(),
                model_name="unknown",
                model_version="unknown",
                prediction_probability=0.0,
                actual_outcome=feedback_label == FeedbackLabel.TRUE_POSITIVE,
                investigation_time_hours=None,
            )

            # Add to processing queue
            self.feedback_queue.put(feedback_entry)

            self.logger.info(
                f"Collected customer dispute: {resolution} for transaction {transaction_id}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to collect customer dispute: {e}")
            return False

    def start_feedback_processing(self) -> None:
        """Start background feedback processing."""
        self.logger.info("Starting feedback processing")
        self._running = True

        # Start feedback processor thread
        self.feedback_processor_thread = threading.Thread(
            target=self._process_feedback_queue, daemon=True
        )
        self.feedback_processor_thread.start()

        # Start retraining scheduler thread
        self.retraining_scheduler_thread = threading.Thread(
            target=self._retraining_scheduler, daemon=True
        )
        self.retraining_scheduler_thread.start()

        self.logger.info("Feedback processing started")

    def stop_feedback_processing(self) -> None:
        """Stop background feedback processing."""
        self.logger.info("Stopping feedback processing")
        self._running = False

        # Wait for threads to finish
        if self.feedback_processor_thread:
            self.feedback_processor_thread.join(timeout=5)
        if self.retraining_scheduler_thread:
            self.retraining_scheduler_thread.join(timeout=5)

        self.logger.info("Feedback processing stopped")

    def _process_feedback_queue(self) -> None:
        """Background thread to process feedback entries."""
        while self._running:
            try:
                # Process feedback entries from queue
                while not self.feedback_queue.empty():
                    feedback_entry = self.feedback_queue.get(timeout=1)
                    self._process_feedback_entry(feedback_entry)

                # Check for retraining triggers
                self._check_retraining_triggers()

                time.sleep(10)  # Process every 10 seconds

            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Feedback processing error: {e}")
                time.sleep(30)  # Wait before retrying

    def _process_feedback_entry(self, feedback_entry: FeedbackEntry) -> None:
        """Process individual feedback entry."""
        try:
            # Enrich feedback with prediction data
            self._enrich_feedback_entry(feedback_entry)

            # Store feedback
            self.feedback_entries.append(feedback_entry)

            # Update model performance metrics
            self._update_feedback_metrics(feedback_entry)

            self.logger.debug(f"Processed feedback entry: {feedback_entry.feedback_id}")

        except Exception as e:
            self.logger.error(
                f"Error processing feedback entry {feedback_entry.feedback_id}: {e}"
            )

    def _enrich_feedback_entry(self, feedback_entry: FeedbackEntry) -> None:
        """Enrich feedback entry with prediction and model information."""
        # This would typically query the prediction database/store
        # For now, using placeholder values
        feedback_entry.model_name = "random_forest"
        feedback_entry.model_version = "1.0"
        feedback_entry.prediction_probability = (
            0.75  # Would be retrieved from prediction store
        )

    def _update_feedback_metrics(self, feedback_entry: FeedbackEntry) -> None:
        """Update feedback-based performance metrics."""
        # This would update running statistics about model performance
        # based on feedback, such as:
        # - Precision based on investigator feedback
        # - False positive rate from customer disputes
        # - Investigation efficiency metrics
        pass

    def _check_retraining_triggers(self) -> None:
        """Check if any retraining triggers are activated."""
        try:
            # Check feedback count trigger
            recent_feedback = [
                f
                for f in self.feedback_entries
                if f.timestamp >= datetime.now() - timedelta(days=30)
            ]

            if len(recent_feedback) >= self.config["retraining_triggers"][
                "min_feedback_count"
            ] and not self._has_recent_retraining("feedback_threshold"):
                self._trigger_retraining(RetrainingTrigger.FEEDBACK_THRESHOLD)

            # Check scheduled retraining
            if self._should_schedule_retraining():
                self._trigger_retraining(RetrainingTrigger.SCHEDULED)

            # Check performance degradation (would integrate with monitor)
            # This is a simplified check
            false_positive_feedback = [
                f
                for f in recent_feedback
                if f.feedback_label == FeedbackLabel.FALSE_POSITIVE
            ]

            if len(recent_feedback) > 100:
                false_positive_rate = len(false_positive_feedback) / len(
                    recent_feedback
                )
                max_fp_rate = self.config["retraining_triggers"][
                    "max_false_positive_rate"
                ]

                if false_positive_rate > max_fp_rate:
                    self._trigger_retraining(RetrainingTrigger.PERFORMANCE_DEGRADATION)

        except Exception as e:
            self.logger.error(f"Error checking retraining triggers: {e}")

    def _has_recent_retraining(self, trigger_type: str) -> bool:
        """Check if there was recent retraining for given trigger type."""
        cutoff_time = datetime.now() - timedelta(
            days=7
        )  # Don't retrain more than once per week

        return any(
            job.trigger.value == trigger_type and job.triggered_at >= cutoff_time
            for job in self.retraining_jobs.values()
        )

    def _should_schedule_retraining(self) -> bool:
        """Check if scheduled retraining is due."""
        last_scheduled = None

        for job in self.retraining_jobs.values():
            if job.trigger == RetrainingTrigger.SCHEDULED and (
                last_scheduled is None or job.triggered_at > last_scheduled
            ):
                last_scheduled = job.triggered_at

        if last_scheduled is None:
            return True  # Never trained before

        interval = timedelta(
            days=self.config["retraining_triggers"]["scheduled_interval_days"]
        )
        return datetime.now() - last_scheduled >= interval

    def _trigger_retraining(self, trigger: RetrainingTrigger) -> str:
        """
        Trigger model retraining.

        Args:
            trigger: The trigger that initiated retraining

        Returns:
            Job ID of the retraining job
        """
        job_id = f"retrain_{trigger.value}_{int(time.time())}"

        self.logger.info(
            f"Triggering model retraining: {trigger.value} (Job ID: {job_id})"
        )

        # Create retraining job
        job = RetrainingJob(
            job_id=job_id,
            model_name="fraud_detection_ensemble",
            trigger=trigger,
            triggered_at=datetime.now(),
            status="queued",
            training_data_size=0,
            validation_metrics={},
        )

        self.retraining_jobs[job_id] = job

        # Start retraining in background thread
        retraining_thread = threading.Thread(
            target=self._execute_retraining, args=(job_id,), daemon=True
        )
        retraining_thread.start()

        return job_id

    def _retraining_scheduler(self) -> None:
        """Background scheduler for retraining jobs."""
        while self._running:
            try:
                # Check for completed jobs and update metrics
                self._update_retraining_job_status()

                # Clean up old jobs
                self._cleanup_old_jobs()

                time.sleep(60)  # Check every minute

            except Exception as e:
                self.logger.error(f"Retraining scheduler error: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying

    def _execute_retraining(self, job_id: str) -> None:
        """Execute model retraining job."""
        job = self.retraining_jobs.get(job_id)
        if not job:
            return

        try:
            self.logger.info(f"Starting retraining job {job_id}")
            job.status = "running"

            # Prepare training data with feedback
            training_df = self._prepare_training_data_with_feedback()
            job.training_data_size = training_df.count()

            # Train new models
            training_results = self.model_trainer.train_ensemble_models(training_df)

            # Evaluate model performance
            validation_metrics = self._evaluate_retrained_models(training_results)
            job.validation_metrics = validation_metrics

            # Deploy model if it performs better
            if self._should_deploy_retrained_model(validation_metrics):
                self._deploy_retrained_model(training_results, job_id)
                job.status = "completed_deployed"
            else:
                job.status = "completed_not_deployed"

            job.completion_time = datetime.now()

            # Trigger callbacks
            for callback in self.retraining_callbacks:
                try:
                    callback(job)
                except Exception as e:
                    self.logger.error(f"Retraining callback failed: {e}")

            self.logger.info(f"Retraining job {job_id} completed successfully")

        except Exception as e:
            self.logger.error(f"Retraining job {job_id} failed: {e}")
            job.status = "failed"
            job.error_message = str(e)
            job.completion_time = datetime.now()

    def _prepare_training_data_with_feedback(self) -> DataFrame:
        """Prepare training data incorporating feedback."""
        # This would combine historical transaction data with feedback labels
        # For now, returning a placeholder DataFrame

        # Get base transaction data
        base_query = """
        SELECT
            transaction_id,
            customer_id,
            merchant_id,
            price,
            payment_method,
            timestamp,
            0 as is_fraud  -- Default label
        FROM transactions
        WHERE timestamp >= current_date() - interval 90 days
        """

        base_df = self.spark.sql(base_query)

        # Create feedback DataFrame
        if self.feedback_entries:
            feedback_data = []
            for feedback in self.feedback_entries:
                feedback_data.append(
                    (
                        feedback.transaction_id,
                        1
                        if feedback.feedback_label in [FeedbackLabel.TRUE_POSITIVE]
                        else 0,
                        feedback.confidence_score,
                    )
                )

            feedback_schema = StructType(
                [
                    StructField("transaction_id", StringType(), True),
                    StructField("feedback_label", IntegerType(), True),
                    StructField("confidence_score", DoubleType(), True),
                ]
            )

            feedback_df = self.spark.createDataFrame(feedback_data, feedback_schema)

            # Join with base data to update labels
            training_df = (
                base_df.join(feedback_df, "transaction_id", "left")
                .withColumn(
                    "is_fraud", F.coalesce(F.col("feedback_label"), F.col("is_fraud"))
                )
                .drop("feedback_label", "confidence_score")
            )
        else:
            training_df = base_df

        return training_df

    def _evaluate_retrained_models(self, training_results: Dict) -> Dict[str, float]:
        """Evaluate retrained models and return metrics."""
        # This would perform comprehensive evaluation
        # For now, returning placeholder metrics
        return {
            "accuracy": 0.87,
            "precision": 0.82,
            "recall": 0.79,
            "f1_score": 0.80,
            "auc_roc": 0.85,
        }

    def _should_deploy_retrained_model(
        self, validation_metrics: Dict[str, float]
    ) -> bool:
        """Determine if retrained model should be deployed."""
        # Compare with current model performance
        # For now, using simple threshold
        min_improvement = self.config["model_selection"]["min_improvement_threshold"]

        current_f1 = 0.78  # Would get from current model metrics
        new_f1 = validation_metrics.get("f1_score", 0.0)

        return new_f1 >= current_f1 + min_improvement

    def _deploy_retrained_model(self, training_results: Dict, job_id: str) -> None:
        """Deploy retrained model with gradual rollout."""
        # Save model
        model_path = f"data/models/retrained_{job_id}"
        self.model_trainer.save_models(model_path)

        # Deploy with small traffic percentage for A/B testing
        traffic_percentage = self.config["model_selection"]["traffic_split_percentage"]

        success = self.model_serving.deploy_model(
            model_name="random_forest",
            model_path=model_path,
            version=f"retrain_{job_id}",
            traffic_percentage=traffic_percentage,
        )

        if success:
            self.logger.info(
                f"Deployed retrained model with {traffic_percentage}% traffic"
            )
        else:
            self.logger.error("Failed to deploy retrained model")

    def _update_retraining_job_status(self) -> None:
        """Update status of running retraining jobs."""
        # This would check job status in a job queue/scheduler
        pass

    def _cleanup_old_jobs(self) -> None:
        """Clean up old completed retraining jobs."""
        cutoff_time = datetime.now() - timedelta(days=30)

        jobs_to_remove = [
            job_id
            for job_id, job in self.retraining_jobs.items()
            if job.completion_time and job.completion_time < cutoff_time
        ]

        for job_id in jobs_to_remove:
            del self.retraining_jobs[job_id]

        if jobs_to_remove:
            self.logger.info(f"Cleaned up {len(jobs_to_remove)} old retraining jobs")

    def get_feedback_summary(self, days: int = 30) -> Dict[str, Any]:
        """Get summary of collected feedback."""
        cutoff_time = datetime.now() - timedelta(days=days)
        recent_feedback = [
            f for f in self.feedback_entries if f.timestamp >= cutoff_time
        ]

        if not recent_feedback:
            return {"message": "No feedback found in the specified time period"}

        # Count by feedback type
        type_counts = {}
        for feedback_type in FeedbackType:
            type_counts[feedback_type.value] = len(
                [f for f in recent_feedback if f.feedback_type == feedback_type]
            )

        # Count by label
        label_counts = {}
        for label in FeedbackLabel:
            label_counts[label.value] = len(
                [f for f in recent_feedback if f.feedback_label == label]
            )

        # Calculate accuracy from feedback
        correct_predictions = len(
            [
                f
                for f in recent_feedback
                if f.feedback_label
                in [FeedbackLabel.TRUE_POSITIVE, FeedbackLabel.TRUE_NEGATIVE]
            ]
        )

        feedback_accuracy = (
            correct_predictions / len(recent_feedback) if recent_feedback else 0.0
        )

        return {
            "time_period_days": days,
            "total_feedback_entries": len(recent_feedback),
            "feedback_by_type": type_counts,
            "feedback_by_label": label_counts,
            "feedback_based_accuracy": feedback_accuracy,
            "average_confidence": sum(f.confidence_score for f in recent_feedback)
            / len(recent_feedback),
            "unique_investigators": len(
                set(f.investigator_id for f in recent_feedback if f.investigator_id)
            ),
        }

    def get_retraining_status(self) -> Dict[str, Any]:
        """Get status of retraining jobs."""
        return {
            "total_jobs": len(self.retraining_jobs),
            "active_jobs": len(
                [
                    j
                    for j in self.retraining_jobs.values()
                    if j.status in ["queued", "running"]
                ]
            ),
            "completed_jobs": len(
                [
                    j
                    for j in self.retraining_jobs.values()
                    if j.status.startswith("completed")
                ]
            ),
            "failed_jobs": len(
                [j for j in self.retraining_jobs.values() if j.status == "failed"]
            ),
            "recent_jobs": [
                {
                    "job_id": job.job_id,
                    "trigger": job.trigger.value,
                    "status": job.status,
                    "triggered_at": job.triggered_at.isoformat(),
                    "completion_time": job.completion_time.isoformat()
                    if job.completion_time
                    else None,
                }
                for job in sorted(
                    self.retraining_jobs.values(),
                    key=lambda x: x.triggered_at,
                    reverse=True,
                )[:10]
            ],
        }

    def add_retraining_callback(self, callback: callable) -> None:
        """Add callback to be called when retraining completes."""
        self.retraining_callbacks.append(callback)

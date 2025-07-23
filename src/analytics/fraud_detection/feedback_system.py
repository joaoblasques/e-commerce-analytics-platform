"""
False positive feedback and model improvement system.

This module provides comprehensive feedback collection and processing for improving
fraud detection accuracy by learning from false positives and investigator decisions.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

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

from .case_management import CaseStatus, FraudCase, FraudCaseManager


class FeedbackType(Enum):
    """Types of feedback for model improvement."""

    FALSE_POSITIVE = "false_positive"
    FALSE_NEGATIVE = "false_negative"
    TRUE_POSITIVE = "true_positive"
    TRUE_NEGATIVE = "true_negative"
    INVESTIGATOR_OVERRIDE = "investigator_override"
    CUSTOMER_DISPUTE = "customer_dispute"


class FeedbackSource(Enum):
    """Sources of feedback."""

    INVESTIGATOR = "investigator"
    CUSTOMER = "customer"
    AUTOMATED_SYSTEM = "automated_system"
    EXTERNAL_SYSTEM = "external_system"
    CHARGEBACK = "chargeback"


@dataclass
class FeedbackEntry:
    """Individual feedback entry for model training."""

    feedback_id: str
    case_id: str
    transaction_id: str
    feedback_type: FeedbackType
    feedback_source: FeedbackSource
    original_prediction: float
    actual_outcome: bool
    confidence_score: float
    feedback_details: Dict[str, Any]
    submitted_by: str
    submitted_at: datetime
    processed: bool = False
    processing_notes: Optional[str] = None


class FalsePositiveFeedbackSystem:
    """
    Comprehensive false positive feedback system.

    Features:
    - Collect feedback from investigators and customers
    - Process chargebacks and disputes
    - Analyze false positive patterns
    - Generate model training data
    - Track feedback quality and accuracy
    - Provide feedback analytics and insights
    """

    def __init__(
        self,
        spark: SparkSession,
        case_manager: FraudCaseManager,
        config: Optional[Dict] = None,
    ):
        """Initialize the feedback system."""
        self.spark = spark
        self.case_manager = case_manager
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Feedback storage
        self.feedback_entries: List[FeedbackEntry] = []
        self.feedback_analytics = {}

        # Initialize storage
        self._initialize_feedback_storage()

    def _get_default_config(self) -> Dict:
        """Get default feedback system configuration."""
        return {
            "feedback_storage_path": "data/delta/fraud_feedback",
            "min_confidence_threshold": 0.7,
            "auto_processing_enabled": True,
            "feedback_validation": {
                "require_justification": True,
                "min_justification_length": 20,
                "allow_anonymous_feedback": False,
            },
            "quality_thresholds": {
                "investigator_accuracy_min": 0.85,
                "feedback_consistency_min": 0.80,
            },
            "retraining_triggers": {
                "false_positive_threshold": 50,
                "feedback_accumulation_days": 7,
                "confidence_degradation_threshold": 0.05,
            },
        }

    def _initialize_feedback_storage(self) -> None:
        """Initialize Delta Lake storage for feedback data."""
        try:
            feedback_schema = StructType(
                [
                    StructField("feedback_id", StringType(), False),
                    StructField("case_id", StringType(), False),
                    StructField("transaction_id", StringType(), False),
                    StructField("feedback_type", StringType(), False),
                    StructField("feedback_source", StringType(), False),
                    StructField("original_prediction", DoubleType(), False),
                    StructField("actual_outcome", BooleanType(), False),
                    StructField("confidence_score", DoubleType(), False),
                    StructField("feedback_details", StringType(), True),
                    StructField("submitted_by", StringType(), False),
                    StructField("submitted_at", TimestampType(), False),
                    StructField("processed", BooleanType(), False),
                    StructField("processing_notes", StringType(), True),
                ]
            )

            empty_feedback_df = self.spark.createDataFrame([], feedback_schema)
            empty_feedback_df.write.mode("ignore").format("delta").save(
                self.config["feedback_storage_path"]
            )

            self.logger.info("Feedback storage initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize feedback storage: {e}")
            raise

    def submit_investigator_feedback(
        self,
        case_id: str,
        feedback_type: FeedbackType,
        justification: str,
        investigator_id: str,
        confidence: float = 0.9,
    ) -> bool:
        """
        Submit feedback from a fraud investigator.

        Args:
            case_id: Case ID for the feedback
            feedback_type: Type of feedback
            justification: Explanation for the feedback
            investigator_id: ID of the investigator
            confidence: Confidence in the feedback

        Returns:
            True if feedback submitted successfully
        """
        try:
            # Validate input
            if not self._validate_feedback_input(justification, investigator_id):
                return False

            # Get case details
            case = self.case_manager.active_cases.get(case_id)
            if not case:
                self.logger.error(f"Case {case_id} not found")
                return False

            # Create feedback entry
            feedback_entry = FeedbackEntry(
                feedback_id=str(uuid.uuid4()),
                case_id=case_id,
                transaction_id=case.transaction_id,
                feedback_type=feedback_type,
                feedback_source=FeedbackSource.INVESTIGATOR,
                original_prediction=case.ml_prediction or case.fraud_score,
                actual_outcome=feedback_type
                in [FeedbackType.TRUE_POSITIVE, FeedbackType.FALSE_NEGATIVE],
                confidence_score=confidence,
                feedback_details={
                    "justification": justification,
                    "case_priority": case.priority.value,
                    "investigation_time_hours": (
                        datetime.now() - case.created_at
                    ).total_seconds()
                    / 3600,
                    "rule_triggers": case.rule_triggers,
                    "transaction_amount": case.transaction_amount,
                },
                submitted_by=investigator_id,
                submitted_at=datetime.now(),
            )

            # Store feedback
            self.feedback_entries.append(feedback_entry)
            self._save_feedback_to_storage(feedback_entry)

            # Process feedback if auto-processing is enabled
            if self.config["auto_processing_enabled"]:
                self._process_feedback(feedback_entry)

            # Update case with feedback information
            self.case_manager.add_case_comment(
                case_id,
                f"Investigator feedback: {feedback_type.value} - {justification}",
                investigator_id,
            )

            self.logger.info(
                f"Investigator feedback submitted for case {case_id}: {feedback_type.value}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to submit investigator feedback: {e}")
            return False

    def submit_customer_dispute(
        self,
        transaction_id: str,
        dispute_type: str,
        dispute_reason: str,
        resolution: str,
        customer_id: Optional[str] = None,
    ) -> bool:
        """
        Submit customer dispute as feedback.

        Args:
            transaction_id: Transaction ID being disputed
            dispute_type: Type of dispute (chargeback, inquiry, etc.)
            dispute_reason: Reason for the dispute
            resolution: Final resolution
            customer_id: Optional customer ID

        Returns:
            True if dispute submitted successfully
        """
        try:
            # Find related case
            related_case = None
            for case in self.case_manager.active_cases.values():
                if case.transaction_id == transaction_id:
                    related_case = case
                    break

            if not related_case:
                self.logger.warning(f"No case found for transaction {transaction_id}")
                # Create a minimal case record for feedback purposes
                case_id = f"dispute_{transaction_id}"
            else:
                case_id = related_case.case_id

            # Determine feedback type based on dispute resolution
            if resolution.lower() in [
                "customer_favor",
                "chargeback_upheld",
                "fraud_confirmed",
            ]:
                feedback_type = FeedbackType.FALSE_POSITIVE
                actual_outcome = False
            elif resolution.lower() in [
                "merchant_favor",
                "chargeback_denied",
                "legitimate_transaction",
            ]:
                feedback_type = FeedbackType.TRUE_POSITIVE
                actual_outcome = True
            else:
                feedback_type = FeedbackType.INVESTIGATOR_OVERRIDE
                actual_outcome = False

            # Create feedback entry
            feedback_entry = FeedbackEntry(
                feedback_id=str(uuid.uuid4()),
                case_id=case_id,
                transaction_id=transaction_id,
                feedback_type=feedback_type,
                feedback_source=FeedbackSource.CUSTOMER,
                original_prediction=related_case.ml_prediction if related_case else 0.5,
                actual_outcome=actual_outcome,
                confidence_score=0.8,  # Customer disputes are generally reliable
                feedback_details={
                    "dispute_type": dispute_type,
                    "dispute_reason": dispute_reason,
                    "resolution": resolution,
                    "customer_id": customer_id,
                    "dispute_submitted_at": datetime.now().isoformat(),
                },
                submitted_by=customer_id or "customer_system",
                submitted_at=datetime.now(),
            )

            # Store and process feedback
            self.feedback_entries.append(feedback_entry)
            self._save_feedback_to_storage(feedback_entry)

            if self.config["auto_processing_enabled"]:
                self._process_feedback(feedback_entry)

            self.logger.info(
                f"Customer dispute feedback submitted for transaction {transaction_id}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to submit customer dispute: {e}")
            return False

    def submit_chargeback_feedback(
        self,
        transaction_id: str,
        chargeback_reason: str,
        chargeback_amount: float,
        resolution: str,
        processing_date: datetime,
    ) -> bool:
        """
        Submit chargeback information as feedback.

        Args:
            transaction_id: Transaction ID for the chargeback
            chargeback_reason: Reason code for chargeback
            chargeback_amount: Amount of the chargeback
            resolution: Final resolution of the chargeback
            processing_date: Date when chargeback was processed

        Returns:
            True if chargeback feedback submitted successfully
        """
        try:
            # Map chargeback reason to feedback type
            fraud_related_reasons = [
                "4837",
                "4863",
                "4870",
                "4871",
            ]  # Common fraud reason codes

            if any(reason in chargeback_reason for reason in fraud_related_reasons):
                if resolution.lower() in ["upheld", "customer_favor"]:
                    feedback_type = FeedbackType.FALSE_POSITIVE
                    actual_outcome = False
                else:
                    feedback_type = FeedbackType.TRUE_POSITIVE
                    actual_outcome = True
            else:
                # Non-fraud chargebacks don't necessarily indicate false positives
                feedback_type = FeedbackType.INVESTIGATOR_OVERRIDE
                actual_outcome = False

            return self.submit_customer_dispute(
                transaction_id=transaction_id,
                dispute_type="chargeback",
                dispute_reason=f"Chargeback reason: {chargeback_reason}",
                resolution=resolution,
                customer_id=None,
            )

        except Exception as e:
            self.logger.error(f"Failed to submit chargeback feedback: {e}")
            return False

    def _validate_feedback_input(self, justification: str, submitted_by: str) -> bool:
        """Validate feedback input according to configuration."""
        if self.config["feedback_validation"]["require_justification"]:
            min_length = self.config["feedback_validation"]["min_justification_length"]
            if len(justification) < min_length:
                self.logger.error(
                    f"Justification too short: {len(justification)} < {min_length}"
                )
                return False

        if not self.config["feedback_validation"]["allow_anonymous_feedback"]:
            if not submitted_by or submitted_by.strip() == "":
                self.logger.error("Anonymous feedback not allowed")
                return False

        return True

    def _process_feedback(self, feedback_entry: FeedbackEntry) -> None:
        """Process feedback entry for model improvement."""
        try:
            # Mark as processed
            feedback_entry.processed = True
            feedback_entry.processing_notes = (
                f"Auto-processed at {datetime.now().isoformat()}"
            )

            # Analyze feedback for patterns
            self._analyze_feedback_patterns(feedback_entry)

            # Check for retraining triggers
            self._check_retraining_triggers()

            # Update feedback quality metrics
            self._update_feedback_quality_metrics(feedback_entry)

            self.logger.debug(f"Processed feedback {feedback_entry.feedback_id}")

        except Exception as e:
            self.logger.error(
                f"Failed to process feedback {feedback_entry.feedback_id}: {e}"
            )
            feedback_entry.processing_notes = f"Processing failed: {str(e)}"

    def _analyze_feedback_patterns(self, feedback_entry: FeedbackEntry) -> None:
        """Analyze feedback for patterns and insights."""
        # This would perform pattern analysis on feedback data
        # For now, storing basic analytics

        if "pattern_analysis" not in self.feedback_analytics:
            self.feedback_analytics["pattern_analysis"] = {
                "false_positive_patterns": {},
                "common_reasons": {},
                "investigator_accuracy": {},
            }

        # Track false positive patterns
        if feedback_entry.feedback_type == FeedbackType.FALSE_POSITIVE:
            details = feedback_entry.feedback_details

            # Analyze by transaction amount
            amount = details.get("transaction_amount", 0)
            amount_range = self._get_amount_range(amount)

            if (
                amount_range
                not in self.feedback_analytics["pattern_analysis"][
                    "false_positive_patterns"
                ]
            ):
                self.feedback_analytics["pattern_analysis"]["false_positive_patterns"][
                    amount_range
                ] = 0
            self.feedback_analytics["pattern_analysis"]["false_positive_patterns"][
                amount_range
            ] += 1

            # Track common justifications
            justification = details.get("justification", "").lower()
            for word in justification.split():
                if len(word) > 3:  # Ignore short words
                    if (
                        word
                        not in self.feedback_analytics["pattern_analysis"][
                            "common_reasons"
                        ]
                    ):
                        self.feedback_analytics["pattern_analysis"]["common_reasons"][
                            word
                        ] = 0
                    self.feedback_analytics["pattern_analysis"]["common_reasons"][
                        word
                    ] += 1

    def _get_amount_range(self, amount: float) -> str:
        """Get amount range category for analysis."""
        if amount < 50:
            return "0-50"
        elif amount < 200:
            return "50-200"
        elif amount < 500:
            return "200-500"
        elif amount < 1000:
            return "500-1000"
        else:
            return "1000+"

    def _check_retraining_triggers(self) -> None:
        """Check if model retraining should be triggered."""
        try:
            # Get recent false positive feedback
            recent_cutoff = datetime.now() - timedelta(
                days=self.config["retraining_triggers"]["feedback_accumulation_days"]
            )

            recent_false_positives = [
                entry
                for entry in self.feedback_entries
                if entry.feedback_type == FeedbackType.FALSE_POSITIVE
                and entry.submitted_at >= recent_cutoff
            ]

            # Check if threshold is exceeded
            fp_threshold = self.config["retraining_triggers"][
                "false_positive_threshold"
            ]
            if len(recent_false_positives) >= fp_threshold:
                self._trigger_model_retraining(
                    "false_positive_threshold_exceeded",
                    {
                        "false_positive_count": len(recent_false_positives),
                        "threshold": fp_threshold,
                        "time_period_days": self.config["retraining_triggers"][
                            "feedback_accumulation_days"
                        ],
                    },
                )

        except Exception as e:
            self.logger.error(f"Failed to check retraining triggers: {e}")

    def _trigger_model_retraining(self, reason: str, details: Dict[str, Any]) -> None:
        """Trigger model retraining based on feedback."""
        self.logger.info(f"Triggering model retraining: {reason}")

        # This would integrate with the ML model training pipeline
        # For now, just logging the trigger
        retraining_request = {
            "triggered_at": datetime.now().isoformat(),
            "reason": reason,
            "details": details,
            "feedback_data_ready": True,
        }

        # Store retraining request (would be picked up by training pipeline)
        self.logger.info(f"Model retraining requested: {retraining_request}")

    def _update_feedback_quality_metrics(self, feedback_entry: FeedbackEntry) -> None:
        """Update feedback quality and investigator accuracy metrics."""
        if feedback_entry.feedback_source == FeedbackSource.INVESTIGATOR:
            investigator_id = feedback_entry.submitted_by

            if "investigator_metrics" not in self.feedback_analytics:
                self.feedback_analytics["investigator_metrics"] = {}

            if investigator_id not in self.feedback_analytics["investigator_metrics"]:
                self.feedback_analytics["investigator_metrics"][investigator_id] = {
                    "total_feedback": 0,
                    "consistency_score": 0.0,
                    "accuracy_score": 0.0,
                }

            self.feedback_analytics["investigator_metrics"][investigator_id][
                "total_feedback"
            ] += 1

    def get_false_positive_analysis(self, time_period_days: int = 30) -> Dict[str, Any]:
        """
        Get comprehensive false positive analysis.

        Args:
            time_period_days: Time period for analysis

        Returns:
            False positive analysis report
        """
        try:
            cutoff_time = datetime.now() - timedelta(days=time_period_days)

            # Filter recent false positive feedback
            recent_fps = [
                entry
                for entry in self.feedback_entries
                if entry.feedback_type == FeedbackType.FALSE_POSITIVE
                and entry.submitted_at >= cutoff_time
            ]

            # Analyze by amount ranges
            amount_analysis = {}
            for entry in recent_fps:
                amount = entry.feedback_details.get("transaction_amount", 0)
                amount_range = self._get_amount_range(amount)
                amount_analysis[amount_range] = amount_analysis.get(amount_range, 0) + 1

            # Analyze by rule triggers
            rule_analysis = {}
            for entry in recent_fps:
                rules = entry.feedback_details.get("rule_triggers", [])
                for rule in rules:
                    rule_analysis[rule] = rule_analysis.get(rule, 0) + 1

            # Analyze by investigator
            investigator_analysis = {}
            for entry in recent_fps:
                if entry.feedback_source == FeedbackSource.INVESTIGATOR:
                    inv_id = entry.submitted_by
                    investigator_analysis[inv_id] = (
                        investigator_analysis.get(inv_id, 0) + 1
                    )

            # Common reasons analysis
            reason_analysis = {}
            for entry in recent_fps:
                justification = entry.feedback_details.get("justification", "").lower()
                # Extract key phrases (simplified)
                if "legitimate customer" in justification:
                    reason_analysis["legitimate_customer"] = (
                        reason_analysis.get("legitimate_customer", 0) + 1
                    )
                if "known good customer" in justification:
                    reason_analysis["known_good_customer"] = (
                        reason_analysis.get("known_good_customer", 0) + 1
                    )
                if "verified purchase" in justification:
                    reason_analysis["verified_purchase"] = (
                        reason_analysis.get("verified_purchase", 0) + 1
                    )

            return {
                "analysis_period_days": time_period_days,
                "total_false_positives": len(recent_fps),
                "false_positive_rate": self._calculate_false_positive_rate(
                    time_period_days
                ),
                "amount_distribution": amount_analysis,
                "rule_trigger_analysis": rule_analysis,
                "investigator_feedback_distribution": investigator_analysis,
                "common_reasons": reason_analysis,
                "improvement_recommendations": self._generate_fp_recommendations(
                    recent_fps
                ),
            }

        except Exception as e:
            self.logger.error(f"Failed to get false positive analysis: {e}")
            return {"error": str(e)}

    def _calculate_false_positive_rate(self, time_period_days: int) -> float:
        """Calculate false positive rate for the time period."""
        cutoff_time = datetime.now() - timedelta(days=time_period_days)

        # Get all feedback in time period
        recent_feedback = [
            entry
            for entry in self.feedback_entries
            if entry.submitted_at >= cutoff_time
        ]

        if not recent_feedback:
            return 0.0

        false_positives = [
            entry
            for entry in recent_feedback
            if entry.feedback_type == FeedbackType.FALSE_POSITIVE
        ]

        return len(false_positives) / len(recent_feedback)

    def _generate_fp_recommendations(
        self, false_positive_entries: List[FeedbackEntry]
    ) -> List[Dict[str, Any]]:
        """Generate recommendations to reduce false positives."""
        recommendations = []

        if not false_positive_entries:
            return recommendations

        # Analyze high-frequency patterns
        amount_counts = {}
        for entry in false_positive_entries:
            amount = entry.feedback_details.get("transaction_amount", 0)
            amount_range = self._get_amount_range(amount)
            amount_counts[amount_range] = amount_counts.get(amount_range, 0) + 1

        # Find the most problematic amount range
        if amount_counts:
            top_amount_range = max(amount_counts.items(), key=lambda x: x[1])
            if (
                top_amount_range[1] > len(false_positive_entries) * 0.3
            ):  # >30% of false positives
                recommendations.append(
                    {
                        "category": "rule_adjustment",
                        "priority": "high",
                        "recommendation": f"Review fraud rules for {top_amount_range[0]} amount range - causing {top_amount_range[1]} false positives",
                        "estimated_impact": f"Could reduce false positives by {top_amount_range[1]} cases",
                    }
                )

        # Check for rule-specific issues
        rule_counts = {}
        for entry in false_positive_entries:
            rules = entry.feedback_details.get("rule_triggers", [])
            for rule in rules:
                rule_counts[rule] = rule_counts.get(rule, 0) + 1

        if rule_counts:
            top_rule = max(rule_counts.items(), key=lambda x: x[1])
            if top_rule[1] > 5:  # Rule causing 5+ false positives
                recommendations.append(
                    {
                        "category": "rule_tuning",
                        "priority": "medium",
                        "recommendation": f"Tune or disable rule '{top_rule[0]}' - causing {top_rule[1]} false positives",
                        "estimated_impact": f"Could reduce false positives by {top_rule[1]} cases",
                    }
                )

        return recommendations

    def get_feedback_quality_report(
        self, investigator_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get feedback quality and investigator accuracy report.

        Args:
            investigator_id: Specific investigator or None for all

        Returns:
            Feedback quality report
        """
        try:
            # Filter feedback by investigator if specified
            if investigator_id:
                feedback_entries = [
                    entry
                    for entry in self.feedback_entries
                    if entry.submitted_by == investigator_id
                ]
            else:
                feedback_entries = self.feedback_entries

            if not feedback_entries:
                return {"message": "No feedback data found"}

            # Calculate overall metrics
            total_feedback = len(feedback_entries)
            processed_feedback = len(
                [entry for entry in feedback_entries if entry.processed]
            )

            # Feedback type distribution
            type_distribution = {}
            for feedback_type in FeedbackType:
                type_distribution[feedback_type.value] = len(
                    [
                        entry
                        for entry in feedback_entries
                        if entry.feedback_type == feedback_type
                    ]
                )

            # Source distribution
            source_distribution = {}
            for source in FeedbackSource:
                source_distribution[source.value] = len(
                    [
                        entry
                        for entry in feedback_entries
                        if entry.feedback_source == source
                    ]
                )

            # Average confidence
            avg_confidence = (
                sum(entry.confidence_score for entry in feedback_entries)
                / total_feedback
            )

            # Time-based analysis
            recent_feedback = [
                entry
                for entry in feedback_entries
                if entry.submitted_at >= datetime.now() - timedelta(days=7)
            ]

            return {
                "report_metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "investigator_id": investigator_id,
                    "total_feedback_entries": total_feedback,
                },
                "feedback_metrics": {
                    "total_feedback": total_feedback,
                    "processed_feedback": processed_feedback,
                    "processing_rate": processed_feedback / total_feedback
                    if total_feedback > 0
                    else 0,
                    "average_confidence": avg_confidence,
                },
                "distribution_analysis": {
                    "feedback_type_distribution": type_distribution,
                    "feedback_source_distribution": source_distribution,
                },
                "recent_activity": {
                    "feedback_last_7_days": len(recent_feedback),
                    "daily_average": len(recent_feedback) / 7,
                },
                "quality_indicators": {
                    "high_confidence_feedback_rate": len(
                        [
                            entry
                            for entry in feedback_entries
                            if entry.confidence_score >= 0.8
                        ]
                    )
                    / total_feedback,
                    "detailed_justification_rate": len(
                        [
                            entry
                            for entry in feedback_entries
                            if len(entry.feedback_details.get("justification", ""))
                            >= 50
                        ]
                    )
                    / total_feedback,
                },
            }

        except Exception as e:
            self.logger.error(f"Failed to get feedback quality report: {e}")
            return {"error": str(e)}

    def _save_feedback_to_storage(self, feedback_entry: FeedbackEntry) -> None:
        """Save feedback entry to Delta Lake storage."""
        try:
            import json

            feedback_data = {
                "feedback_id": feedback_entry.feedback_id,
                "case_id": feedback_entry.case_id,
                "transaction_id": feedback_entry.transaction_id,
                "feedback_type": feedback_entry.feedback_type.value,
                "feedback_source": feedback_entry.feedback_source.value,
                "original_prediction": feedback_entry.original_prediction,
                "actual_outcome": feedback_entry.actual_outcome,
                "confidence_score": feedback_entry.confidence_score,
                "feedback_details": json.dumps(feedback_entry.feedback_details),
                "submitted_by": feedback_entry.submitted_by,
                "submitted_at": feedback_entry.submitted_at,
                "processed": feedback_entry.processed,
                "processing_notes": feedback_entry.processing_notes,
            }

            feedback_df = self.spark.createDataFrame([feedback_data])
            feedback_df.write.mode("append").format("delta").save(
                self.config["feedback_storage_path"]
            )

        except Exception as e:
            self.logger.error(
                f"Failed to save feedback {feedback_entry.feedback_id} to storage: {e}"
            )

    def export_training_data(
        self, output_path: str, time_period_days: int = 90
    ) -> bool:
        """
        Export feedback data for model training.

        Args:
            output_path: Path to save training data
            time_period_days: Time period for training data

        Returns:
            True if export successful
        """
        try:
            cutoff_time = datetime.now() - timedelta(days=time_period_days)

            # Get processed feedback for training
            training_feedback = [
                entry
                for entry in self.feedback_entries
                if entry.processed and entry.submitted_at >= cutoff_time
            ]

            if not training_feedback:
                self.logger.warning(
                    "No processed feedback found for training data export"
                )
                return False

            # Convert to training format
            training_data = []
            for entry in training_feedback:
                training_record = {
                    "transaction_id": entry.transaction_id,
                    "original_prediction": entry.original_prediction,
                    "actual_outcome": entry.actual_outcome,
                    "feedback_confidence": entry.confidence_score,
                    "features": entry.feedback_details,
                    "feedback_type": entry.feedback_type.value,
                    "feedback_source": entry.feedback_source.value,
                    "timestamp": entry.submitted_at.isoformat(),
                }
                training_data.append(training_record)

            # Export as Parquet for ML pipeline
            training_df = self.spark.createDataFrame(training_data)
            training_df.write.mode("overwrite").parquet(output_path)

            self.logger.info(
                f"Exported {len(training_data)} feedback records for training to {output_path}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to export training data: {e}")
            return False

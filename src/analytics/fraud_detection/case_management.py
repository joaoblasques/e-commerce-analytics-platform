"""
Fraud case management system for investigation workflow.

This module provides comprehensive case management capabilities for fraud investigators,
including case creation, assignment, status tracking, and investigation workflows.
"""

import ast
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

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


class CaseStatus(Enum):
    """Case status for fraud investigations."""

    OPEN = "open"
    IN_PROGRESS = "in_progress"
    UNDER_REVIEW = "under_review"
    RESOLVED_FRAUD = "resolved_fraud"
    RESOLVED_LEGITIMATE = "resolved_legitimate"
    ESCALATED = "escalated"
    CLOSED = "closed"


class CasePriority(Enum):
    """Case priority levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class InvestigationAction(Enum):
    """Types of investigation actions."""

    ASSIGNED = "assigned"
    STATUS_CHANGED = "status_changed"
    COMMENT_ADDED = "comment_added"
    EVIDENCE_ADDED = "evidence_added"
    DECISION_MADE = "decision_made"
    ESCALATED = "escalated"
    REVIEWED = "reviewed"


@dataclass
class CaseEvidence:
    """Evidence associated with a fraud case."""

    evidence_id: str
    case_id: str
    evidence_type: str  # transaction, pattern, model_output, external
    evidence_data: Dict[str, Any]
    collected_by: str
    collected_at: datetime
    description: Optional[str] = None


@dataclass
class CaseAction:
    """Action taken on a fraud case."""

    action_id: str
    case_id: str
    action_type: InvestigationAction
    performed_by: str
    performed_at: datetime
    details: Dict[str, Any]
    comment: Optional[str] = None


@dataclass
class FraudCase:
    """Fraud investigation case."""

    case_id: str
    transaction_id: str
    alert_id: str
    status: CaseStatus
    priority: CasePriority
    assigned_to: Optional[str]
    created_at: datetime
    updated_at: datetime
    created_by: str

    # Case details
    customer_id: str
    merchant_id: str
    transaction_amount: float
    fraud_score: float
    rule_triggers: List[str]
    ml_prediction: Optional[float]

    # Investigation metadata
    investigation_deadline: Optional[datetime] = None
    resolution: Optional[str] = None
    resolution_confidence: Optional[float] = None
    resolution_reason: Optional[str] = None
    investigation_notes: List[str] = field(default_factory=list)

    # Evidence and actions
    evidence: List[CaseEvidence] = field(default_factory=list)
    actions: List[CaseAction] = field(default_factory=list)

    # SLA tracking
    first_response_time: Optional[datetime] = None
    resolution_time: Optional[datetime] = None


class FraudCaseManager:
    """
    Comprehensive fraud case management system.

    Features:
    - Case creation from fraud alerts
    - Case assignment and workload balancing
    - Investigation workflow management
    - Evidence collection and tracking
    - SLA monitoring and reporting
    - Case analytics and metrics
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """Initialize the fraud case manager."""
        self.spark = spark
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Active cases storage
        self.active_cases: Dict[str, FraudCase] = {}

        # Performance metrics
        self.performance_metrics = {
            "cases_created": 0,
            "cases_resolved": 0,
            "average_resolution_time": 0.0,
            "sla_compliance_rate": 0.0,
        }

        # Initialize case storage schema
        self._initialize_case_storage()

    def _get_default_config(self) -> Dict:
        """Get default case management configuration."""
        return {
            "case_storage_path": "data/delta/fraud_cases",
            "evidence_storage_path": "data/delta/case_evidence",
            "actions_storage_path": "data/delta/case_actions",
            "sla_settings": {
                "critical_response_hours": 1,
                "high_response_hours": 4,
                "medium_response_hours": 24,
                "low_response_hours": 72,
                "resolution_deadline_hours": {
                    "critical": 4,
                    "high": 24,
                    "medium": 72,
                    "low": 168,  # 1 week
                },
            },
            "assignment_strategy": "round_robin",  # round_robin, workload_based, skill_based
            "auto_escalation": {
                "enabled": True,
                "overdue_threshold_hours": 24,
                "high_value_threshold": 10000.0,
            },
            "case_retention_days": 365,
        }

    def _initialize_case_storage(self) -> None:
        """Initialize Delta Lake tables for case storage."""
        try:
            # Define case schema
            case_schema = StructType(
                [
                    StructField("case_id", StringType(), False),
                    StructField("transaction_id", StringType(), False),
                    StructField("alert_id", StringType(), False),
                    StructField("status", StringType(), False),
                    StructField("priority", StringType(), False),
                    StructField("assigned_to", StringType(), True),
                    StructField("created_at", TimestampType(), False),
                    StructField("updated_at", TimestampType(), False),
                    StructField("created_by", StringType(), False),
                    StructField("customer_id", StringType(), False),
                    StructField("merchant_id", StringType(), False),
                    StructField("transaction_amount", DoubleType(), False),
                    StructField("fraud_score", DoubleType(), False),
                    StructField("rule_triggers", StringType(), True),  # JSON string
                    StructField("ml_prediction", DoubleType(), True),
                    StructField("investigation_deadline", TimestampType(), True),
                    StructField("resolution", StringType(), True),
                    StructField("resolution_confidence", DoubleType(), True),
                    StructField("resolution_reason", StringType(), True),
                    StructField(
                        "investigation_notes", StringType(), True
                    ),  # JSON string
                    StructField("first_response_time", TimestampType(), True),
                    StructField("resolution_time", TimestampType(), True),
                ]
            )

            # Create empty DataFrame if tables don't exist
            empty_cases_df = self.spark.createDataFrame([], case_schema)

            # Write to Delta Lake (will create table if not exists)
            empty_cases_df.write.mode("ignore").format("delta").save(
                self.config["case_storage_path"]
            )

            self.logger.info("Case storage initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize case storage: {e}")
            raise

    def create_case_from_alert(
        self,
        alert_data: Dict[str, Any],
        created_by: str,
        priority: Optional[CasePriority] = None,
    ) -> FraudCase:
        """
        Create a new fraud case from an alert.

        Args:
            alert_data: Alert data dictionary
            created_by: User who created the case
            priority: Optional priority override

        Returns:
            Created fraud case
        """
        case_id = str(uuid.uuid4())
        current_time = datetime.now()

        # Determine priority if not specified
        if priority is None:
            priority = self._determine_case_priority(alert_data)

        # Calculate investigation deadline
        deadline_hours = self.config["sla_settings"]["resolution_deadline_hours"][
            priority.value
        ]
        investigation_deadline = current_time + timedelta(hours=deadline_hours)

        # Create the case
        fraud_case = FraudCase(
            case_id=case_id,
            transaction_id=alert_data.get("transaction_id", ""),
            alert_id=alert_data.get("alert_id", ""),
            status=CaseStatus.OPEN,
            priority=priority,
            assigned_to=None,
            created_at=current_time,
            updated_at=current_time,
            created_by=created_by,
            customer_id=alert_data.get("customer_id", ""),
            merchant_id=alert_data.get("merchant_id", ""),
            transaction_amount=float(alert_data.get("price", 0.0)),
            fraud_score=float(alert_data.get("fraud_score", 0.0)),
            rule_triggers=alert_data.get("triggered_rules", []),
            ml_prediction=alert_data.get("ml_fraud_probability"),
            investigation_deadline=investigation_deadline,
        )

        # Add initial evidence from alert
        self._add_alert_evidence(fraud_case, alert_data, created_by)

        # Record case creation action
        creation_action = CaseAction(
            action_id=str(uuid.uuid4()),
            case_id=case_id,
            action_type=InvestigationAction.STATUS_CHANGED,
            performed_by=created_by,
            performed_at=current_time,
            details={"old_status": None, "new_status": CaseStatus.OPEN.value},
            comment="Case created from fraud alert",
        )
        fraud_case.actions.append(creation_action)

        # Store case
        self.active_cases[case_id] = fraud_case
        self._save_case_to_storage(fraud_case)

        # Update metrics
        self.performance_metrics["cases_created"] += 1

        self.logger.info(
            f"Created fraud case {case_id} from alert {alert_data.get('alert_id', 'unknown')}"
        )
        return fraud_case

    def _determine_case_priority(self, alert_data: Dict[str, Any]) -> CasePriority:
        """Determine case priority based on alert data."""
        fraud_score = float(alert_data.get("fraud_score", 0.0))
        transaction_amount = float(alert_data.get("price", 0.0))
        alert_priority = alert_data.get("alert_priority", "medium")

        # High value transactions get elevated priority
        if transaction_amount > self.config["auto_escalation"]["high_value_threshold"]:
            return CasePriority.HIGH

        # Use fraud score to determine priority
        if fraud_score >= 0.9:
            return CasePriority.CRITICAL
        elif fraud_score >= 0.7:
            return CasePriority.HIGH
        elif fraud_score >= 0.4:
            return CasePriority.MEDIUM
        else:
            return CasePriority.LOW

    def _add_alert_evidence(
        self, case: FraudCase, alert_data: Dict[str, Any], collected_by: str
    ) -> None:
        """Add evidence from the original fraud alert."""
        evidence = CaseEvidence(
            evidence_id=str(uuid.uuid4()),
            case_id=case.case_id,
            evidence_type="alert_data",
            evidence_data=alert_data,
            collected_by=collected_by,
            collected_at=datetime.now(),
            description="Original fraud alert data",
        )
        case.evidence.append(evidence)

    def assign_case(self, case_id: str, investigator_id: str, assigned_by: str) -> bool:
        """
        Assign a case to an investigator.

        Args:
            case_id: Case ID to assign
            investigator_id: Investigator to assign to
            assigned_by: User making the assignment

        Returns:
            True if assignment successful
        """
        try:
            case = self.active_cases.get(case_id)
            if not case:
                self.logger.error(f"Case {case_id} not found")
                return False

            # Update case
            old_assignee = case.assigned_to
            case.assigned_to = investigator_id
            case.updated_at = datetime.now()

            # Record assignment action
            action = CaseAction(
                action_id=str(uuid.uuid4()),
                case_id=case_id,
                action_type=InvestigationAction.ASSIGNED,
                performed_by=assigned_by,
                performed_at=datetime.now(),
                details={
                    "old_assignee": old_assignee,
                    "new_assignee": investigator_id,
                },
                comment=f"Case assigned to {investigator_id}",
            )
            case.actions.append(action)

            # Update status if it's still open
            if case.status == CaseStatus.OPEN:
                self.update_case_status(case_id, CaseStatus.IN_PROGRESS, assigned_by)

            # Save to storage
            self._save_case_to_storage(case)

            self.logger.info(f"Assigned case {case_id} to {investigator_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to assign case {case_id}: {e}")
            return False

    def update_case_status(
        self,
        case_id: str,
        new_status: CaseStatus,
        updated_by: str,
        comment: Optional[str] = None,
    ) -> bool:
        """
        Update case status.

        Args:
            case_id: Case ID to update
            new_status: New status
            updated_by: User making the update
            comment: Optional comment

        Returns:
            True if update successful
        """
        try:
            case = self.active_cases.get(case_id)
            if not case:
                self.logger.error(f"Case {case_id} not found")
                return False

            old_status = case.status
            case.status = new_status
            case.updated_at = datetime.now()

            # Set first response time if moving from OPEN
            if old_status == CaseStatus.OPEN and case.first_response_time is None:
                case.first_response_time = datetime.now()

            # Set resolution time if resolving
            if new_status in [
                CaseStatus.RESOLVED_FRAUD,
                CaseStatus.RESOLVED_LEGITIMATE,
                CaseStatus.CLOSED,
            ]:
                case.resolution_time = datetime.now()
                self.performance_metrics["cases_resolved"] += 1

            # Record status change action
            action = CaseAction(
                action_id=str(uuid.uuid4()),
                case_id=case_id,
                action_type=InvestigationAction.STATUS_CHANGED,
                performed_by=updated_by,
                performed_at=datetime.now(),
                details={
                    "old_status": old_status.value,
                    "new_status": new_status.value,
                },
                comment=comment
                or f"Status changed from {old_status.value} to {new_status.value}",
            )
            case.actions.append(action)

            # Save to storage
            self._save_case_to_storage(case)

            self.logger.info(
                f"Updated case {case_id} status from {old_status.value} to {new_status.value}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to update case {case_id} status: {e}")
            return False

    def add_case_comment(self, case_id: str, comment: str, added_by: str) -> bool:
        """
        Add a comment to a case.

        Args:
            case_id: Case ID
            comment: Comment text
            added_by: User adding the comment

        Returns:
            True if comment added successfully
        """
        try:
            case = self.active_cases.get(case_id)
            if not case:
                self.logger.error(f"Case {case_id} not found")
                return False

            # Add to investigation notes
            case.investigation_notes.append(
                f"[{datetime.now().isoformat()}] {added_by}: {comment}"
            )
            case.updated_at = datetime.now()

            # Record comment action
            action = CaseAction(
                action_id=str(uuid.uuid4()),
                case_id=case_id,
                action_type=InvestigationAction.COMMENT_ADDED,
                performed_by=added_by,
                performed_at=datetime.now(),
                details={"comment_length": len(comment)},
                comment=comment,
            )
            case.actions.append(action)

            # Save to storage
            self._save_case_to_storage(case)

            self.logger.info(f"Added comment to case {case_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to add comment to case {case_id}: {e}")
            return False

    def add_evidence(
        self,
        case_id: str,
        evidence_type: str,
        evidence_data: Dict[str, Any],
        collected_by: str,
        description: Optional[str] = None,
    ) -> bool:
        """
        Add evidence to a case.

        Args:
            case_id: Case ID
            evidence_type: Type of evidence
            evidence_data: Evidence data
            collected_by: User collecting the evidence
            description: Optional description

        Returns:
            True if evidence added successfully
        """
        try:
            case = self.active_cases.get(case_id)
            if not case:
                self.logger.error(f"Case {case_id} not found")
                return False

            # Create evidence
            evidence = CaseEvidence(
                evidence_id=str(uuid.uuid4()),
                case_id=case_id,
                evidence_type=evidence_type,
                evidence_data=evidence_data,
                collected_by=collected_by,
                collected_at=datetime.now(),
                description=description,
            )
            case.evidence.append(evidence)
            case.updated_at = datetime.now()

            # Record evidence action
            action = CaseAction(
                action_id=str(uuid.uuid4()),
                case_id=case_id,
                action_type=InvestigationAction.EVIDENCE_ADDED,
                performed_by=collected_by,
                performed_at=datetime.now(),
                details={
                    "evidence_type": evidence_type,
                    "evidence_id": evidence.evidence_id,
                },
                comment=description or f"Added {evidence_type} evidence",
            )
            case.actions.append(action)

            # Save to storage
            self._save_case_to_storage(case)

            self.logger.info(f"Added {evidence_type} evidence to case {case_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to add evidence to case {case_id}: {e}")
            return False

    def resolve_case(
        self,
        case_id: str,
        resolution: str,
        confidence: float,
        reason: str,
        resolved_by: str,
    ) -> bool:
        """
        Resolve a fraud case.

        Args:
            case_id: Case ID to resolve
            resolution: Resolution decision (fraud/legitimate)
            confidence: Confidence in the decision (0-1)
            reason: Reason for the decision
            resolved_by: User resolving the case

        Returns:
            True if case resolved successfully
        """
        try:
            case = self.active_cases.get(case_id)
            if not case:
                self.logger.error(f"Case {case_id} not found")
                return False

            # Update case resolution details
            case.resolution = resolution
            case.resolution_confidence = confidence
            case.resolution_reason = reason
            case.updated_at = datetime.now()

            # Set appropriate status
            if resolution.lower() in ["fraud", "confirmed_fraud"]:
                new_status = CaseStatus.RESOLVED_FRAUD
            else:
                new_status = CaseStatus.RESOLVED_LEGITIMATE

            # Update status
            self.update_case_status(
                case_id,
                new_status,
                resolved_by,
                f"Case resolved as {resolution}: {reason}",
            )

            # Record resolution action
            action = CaseAction(
                action_id=str(uuid.uuid4()),
                case_id=case_id,
                action_type=InvestigationAction.DECISION_MADE,
                performed_by=resolved_by,
                performed_at=datetime.now(),
                details={
                    "resolution": resolution,
                    "confidence": confidence,
                    "reason": reason,
                },
                comment=f"Case resolved as {resolution} with {confidence:.2%} confidence",
            )
            case.actions.append(action)

            # Save to storage
            self._save_case_to_storage(case)

            self.logger.info(f"Resolved case {case_id} as {resolution}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to resolve case {case_id}: {e}")
            return False

    def get_cases_by_status(self, status: CaseStatus) -> List[FraudCase]:
        """Get all cases with a specific status."""
        return [case for case in self.active_cases.values() if case.status == status]

    def get_cases_by_investigator(self, investigator_id: str) -> List[FraudCase]:
        """Get all cases assigned to a specific investigator."""
        return [
            case
            for case in self.active_cases.values()
            if case.assigned_to == investigator_id
        ]

    def get_overdue_cases(self) -> List[FraudCase]:
        """Get all cases that are past their investigation deadline."""
        current_time = datetime.now()
        return [
            case
            for case in self.active_cases.values()
            if case.investigation_deadline
            and current_time > case.investigation_deadline
            and case.status
            not in [
                CaseStatus.RESOLVED_FRAUD,
                CaseStatus.RESOLVED_LEGITIMATE,
                CaseStatus.CLOSED,
            ]
        ]

    def get_case_workload_by_investigator(self) -> Dict[str, int]:
        """Get case workload count by investigator."""
        workload = {}
        for case in self.active_cases.values():
            if case.assigned_to and case.status in [
                CaseStatus.IN_PROGRESS,
                CaseStatus.UNDER_REVIEW,
            ]:
                workload[case.assigned_to] = workload.get(case.assigned_to, 0) + 1
        return workload

    def get_performance_metrics(self, time_period_days: int = 30) -> Dict[str, Any]:
        """
        Get case management performance metrics.

        Args:
            time_period_days: Time period for metrics calculation

        Returns:
            Performance metrics dictionary
        """
        cutoff_time = datetime.now() - timedelta(days=time_period_days)

        # Filter cases within time period
        recent_cases = [
            case
            for case in self.active_cases.values()
            if case.created_at >= cutoff_time
        ]

        resolved_cases = [
            case
            for case in recent_cases
            if case.status
            in [
                CaseStatus.RESOLVED_FRAUD,
                CaseStatus.RESOLVED_LEGITIMATE,
                CaseStatus.CLOSED,
            ]
        ]

        # Calculate metrics
        total_cases = len(recent_cases)
        total_resolved = len(resolved_cases)

        # Resolution time metrics
        resolution_times = []
        for case in resolved_cases:
            if case.created_at and case.resolution_time:
                resolution_time = (
                    case.resolution_time - case.created_at
                ).total_seconds() / 3600  # hours
                resolution_times.append(resolution_time)

        avg_resolution_time = (
            sum(resolution_times) / len(resolution_times) if resolution_times else 0
        )

        # SLA compliance
        sla_compliant = 0
        for case in resolved_cases:
            if case.investigation_deadline and case.resolution_time:
                if case.resolution_time <= case.investigation_deadline:
                    sla_compliant += 1

        sla_compliance_rate = (
            (sla_compliant / total_resolved) if total_resolved > 0 else 0
        )

        # Priority distribution
        priority_distribution = {}
        for priority in CasePriority:
            priority_distribution[priority.value] = len(
                [case for case in recent_cases if case.priority == priority]
            )

        return {
            "time_period_days": time_period_days,
            "total_cases": total_cases,
            "total_resolved": total_resolved,
            "resolution_rate": (total_resolved / total_cases) if total_cases > 0 else 0,
            "average_resolution_time_hours": avg_resolution_time,
            "sla_compliance_rate": sla_compliance_rate,
            "priority_distribution": priority_distribution,
            "overdue_cases": len(self.get_overdue_cases()),
            "active_cases_by_status": {
                status.value: len(self.get_cases_by_status(status))
                for status in CaseStatus
            },
            "investigator_workload": self.get_case_workload_by_investigator(),
        }

    def _save_case_to_storage(self, case: FraudCase) -> None:
        """Save case to Delta Lake storage."""
        try:
            # Convert case to DataFrame row
            import json

            case_data = {
                "case_id": case.case_id,
                "transaction_id": case.transaction_id,
                "alert_id": case.alert_id,
                "status": case.status.value,
                "priority": case.priority.value,
                "assigned_to": case.assigned_to,
                "created_at": case.created_at,
                "updated_at": case.updated_at,
                "created_by": case.created_by,
                "customer_id": case.customer_id,
                "merchant_id": case.merchant_id,
                "transaction_amount": case.transaction_amount,
                "fraud_score": case.fraud_score,
                "rule_triggers": json.dumps(case.rule_triggers),
                "ml_prediction": case.ml_prediction,
                "investigation_deadline": case.investigation_deadline,
                "resolution": case.resolution,
                "resolution_confidence": case.resolution_confidence,
                "resolution_reason": case.resolution_reason,
                "investigation_notes": json.dumps(case.investigation_notes),
                "first_response_time": case.first_response_time,
                "resolution_time": case.resolution_time,
            }

            # Create DataFrame and merge to Delta Lake
            case_df = self.spark.createDataFrame([case_data])
            case_df.write.mode("append").format("delta").save(
                self.config["case_storage_path"]
            )

        except Exception as e:
            self.logger.error(f"Failed to save case {case.case_id} to storage: {e}")

    def load_active_cases(self) -> None:
        """Load active cases from storage."""
        try:
            # Load cases from Delta Lake
            cases_df = self.spark.read.format("delta").load(
                self.config["case_storage_path"]
            )

            # Filter for active cases
            active_statuses = [
                CaseStatus.OPEN.value,
                CaseStatus.IN_PROGRESS.value,
                CaseStatus.UNDER_REVIEW.value,
                CaseStatus.ESCALATED.value,
            ]

            active_cases_df = cases_df.filter(F.col("status").isin(active_statuses))

            # Convert to FraudCase objects (simplified version)
            for row in active_cases_df.collect():
                # This is a simplified load - in production you'd also load evidence and actions
                case = FraudCase(
                    case_id=row["case_id"],
                    transaction_id=row["transaction_id"],
                    alert_id=row["alert_id"],
                    status=CaseStatus(row["status"]),
                    priority=CasePriority(row["priority"]),
                    assigned_to=row["assigned_to"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    created_by=row["created_by"],
                    customer_id=row["customer_id"],
                    merchant_id=row["merchant_id"],
                    transaction_amount=row["transaction_amount"],
                    fraud_score=row["fraud_score"],
                    rule_triggers=ast.literal_eval(row["rule_triggers"])
                    if row["rule_triggers"]
                    else [],
                    ml_prediction=row["ml_prediction"],
                    investigation_deadline=row["investigation_deadline"],
                    resolution=row["resolution"],
                    resolution_confidence=row["resolution_confidence"],
                    resolution_reason=row["resolution_reason"],
                    investigation_notes=ast.literal_eval(row["investigation_notes"])
                    if row["investigation_notes"]
                    else [],
                    first_response_time=row["first_response_time"],
                    resolution_time=row["resolution_time"],
                )

                self.active_cases[case.case_id] = case

            self.logger.info(
                f"Loaded {len(self.active_cases)} active cases from storage"
            )

        except Exception as e:
            self.logger.warning(f"Failed to load cases from storage: {e}")
            # Continue with empty case list

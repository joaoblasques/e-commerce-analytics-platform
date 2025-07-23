"""
Investigator dashboard and tools for fraud case management.

This module provides a comprehensive dashboard interface and toolkit for fraud investigators,
including case queues, investigation tools, and analytical capabilities.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from .case_management import CasePriority, CaseStatus, FraudCase, FraudCaseManager


class InvestigatorDashboard:
    """
    Comprehensive dashboard for fraud investigators.

    Features:
    - Case queue management
    - Investigation tools and utilities
    - Performance analytics
    - Workload management
    - Real-time case updates
    """

    def __init__(
        self,
        spark: SparkSession,
        case_manager: FraudCaseManager,
        investigator_id: str,
        config: Optional[Dict] = None,
    ):
        """Initialize the investigator dashboard."""
        self.spark = spark
        self.case_manager = case_manager
        self.investigator_id = investigator_id
        self.config = config or self._get_default_config()
        self.logger = logging.getLogger(__name__)

        # Dashboard state
        self.active_filters = {}
        self.display_preferences = {}

    def _get_default_config(self) -> Dict:
        """Get default dashboard configuration."""
        return {
            "default_page_size": 25,
            "auto_refresh_interval": 30,  # seconds
            "high_priority_alert_threshold": 5,  # cases
            "workload_warning_threshold": 15,  # cases
            "display_preferences": {
                "show_resolved_cases": False,
                "default_sort": "priority_desc",
                "compact_view": False,
            },
            "investigation_tools": {
                "transaction_analysis_enabled": True,
                "pattern_analysis_enabled": True,
                "customer_history_enabled": True,
                "merchant_analysis_enabled": True,
                "ml_model_explainability": True,
            },
        }

    def get_case_queue(
        self,
        status_filter: Optional[List[CaseStatus]] = None,
        priority_filter: Optional[List[CasePriority]] = None,
        assigned_only: bool = True,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get case queue for the investigator.

        Args:
            status_filter: Filter by case statuses
            priority_filter: Filter by case priorities
            assigned_only: Show only assigned cases
            limit: Maximum number of cases to return

        Returns:
            List of case summaries
        """
        try:
            # Get all relevant cases
            if assigned_only:
                cases = self.case_manager.get_cases_by_investigator(
                    self.investigator_id
                )
            else:
                cases = list(self.case_manager.active_cases.values())

            # Apply filters
            if status_filter:
                cases = [case for case in cases if case.status in status_filter]

            if priority_filter:
                cases = [case for case in cases if case.priority in priority_filter]

            # Sort cases (priority first, then by creation time)
            priority_order = {
                CasePriority.CRITICAL: 4,
                CasePriority.HIGH: 3,
                CasePriority.MEDIUM: 2,
                CasePriority.LOW: 1,
            }

            cases.sort(
                key=lambda c: (priority_order[c.priority], c.created_at), reverse=True
            )

            # Limit results
            cases = cases[:limit]

            # Convert to summary format
            case_summaries = []
            for case in cases:
                # Calculate time since creation
                time_elapsed = datetime.now() - case.created_at
                hours_elapsed = time_elapsed.total_seconds() / 3600

                # Calculate SLA status
                sla_status = "on_time"
                if case.investigation_deadline:
                    time_remaining = case.investigation_deadline - datetime.now()
                    if time_remaining.total_seconds() < 0:
                        sla_status = "overdue"
                    elif time_remaining.total_seconds() < 3600:  # Less than 1 hour
                        sla_status = "urgent"

                summary = {
                    "case_id": case.case_id,
                    "transaction_id": case.transaction_id,
                    "status": case.status.value,
                    "priority": case.priority.value,
                    "customer_id": case.customer_id,
                    "merchant_id": case.merchant_id,
                    "transaction_amount": case.transaction_amount,
                    "fraud_score": case.fraud_score,
                    "ml_prediction": case.ml_prediction,
                    "rule_triggers": case.rule_triggers,
                    "created_at": case.created_at.isoformat(),
                    "updated_at": case.updated_at.isoformat(),
                    "assigned_to": case.assigned_to,
                    "hours_elapsed": round(hours_elapsed, 1),
                    "sla_status": sla_status,
                    "evidence_count": len(case.evidence),
                    "actions_count": len(case.actions),
                    "has_notes": len(case.investigation_notes) > 0,
                }

                if case.investigation_deadline:
                    summary[
                        "investigation_deadline"
                    ] = case.investigation_deadline.isoformat()
                    time_remaining = case.investigation_deadline - datetime.now()
                    summary["hours_remaining"] = max(
                        0, round(time_remaining.total_seconds() / 3600, 1)
                    )

                case_summaries.append(summary)

            return case_summaries

        except Exception as e:
            self.logger.error(f"Failed to get case queue: {e}")
            return []

    def get_case_details(self, case_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific case.

        Args:
            case_id: Case ID to retrieve

        Returns:
            Detailed case information or None if not found
        """
        case = self.case_manager.active_cases.get(case_id)
        if not case:
            return None

        try:
            # Get additional context data
            transaction_context = self._get_transaction_context(case.transaction_id)
            customer_context = self._get_customer_context(case.customer_id)
            merchant_context = self._get_merchant_context(case.merchant_id)

            return {
                "case_info": {
                    "case_id": case.case_id,
                    "transaction_id": case.transaction_id,
                    "alert_id": case.alert_id,
                    "status": case.status.value,
                    "priority": case.priority.value,
                    "assigned_to": case.assigned_to,
                    "created_at": case.created_at.isoformat(),
                    "updated_at": case.updated_at.isoformat(),
                    "created_by": case.created_by,
                    "investigation_deadline": case.investigation_deadline.isoformat()
                    if case.investigation_deadline
                    else None,
                    "resolution": case.resolution,
                    "resolution_confidence": case.resolution_confidence,
                    "resolution_reason": case.resolution_reason,
                },
                "transaction_info": {
                    "customer_id": case.customer_id,
                    "merchant_id": case.merchant_id,
                    "transaction_amount": case.transaction_amount,
                    "fraud_score": case.fraud_score,
                    "ml_prediction": case.ml_prediction,
                    "rule_triggers": case.rule_triggers,
                },
                "context": {
                    "transaction_context": transaction_context,
                    "customer_context": customer_context,
                    "merchant_context": merchant_context,
                },
                "investigation": {
                    "evidence": [
                        {
                            "evidence_id": evidence.evidence_id,
                            "evidence_type": evidence.evidence_type,
                            "collected_by": evidence.collected_by,
                            "collected_at": evidence.collected_at.isoformat(),
                            "description": evidence.description,
                            "evidence_summary": self._summarize_evidence(
                                evidence.evidence_data
                            ),
                        }
                        for evidence in case.evidence
                    ],
                    "actions": [
                        {
                            "action_id": action.action_id,
                            "action_type": action.action_type.value,
                            "performed_by": action.performed_by,
                            "performed_at": action.performed_at.isoformat(),
                            "comment": action.comment,
                            "details": action.details,
                        }
                        for action in case.actions
                    ],
                    "investigation_notes": case.investigation_notes,
                },
                "timeline": self._build_case_timeline(case),
            }

        except Exception as e:
            self.logger.error(f"Failed to get case details for {case_id}: {e}")
            return None

    def _get_transaction_context(self, transaction_id: str) -> Dict[str, Any]:
        """Get additional context about the transaction."""
        try:
            # This would query the transaction database for additional context
            # For now, returning placeholder data
            return {
                "transaction_time": "2025-07-23T10:30:00Z",
                "payment_method": "credit_card",
                "device_info": {
                    "device_type": "mobile",
                    "browser": "Safari",
                    "ip_address": "192.168.1.100",
                    "location": "San Francisco, CA",
                },
                "transaction_flow": [
                    {"step": "cart_add", "timestamp": "2025-07-23T10:28:00Z"},
                    {"step": "checkout_start", "timestamp": "2025-07-23T10:29:00Z"},
                    {"step": "payment_submit", "timestamp": "2025-07-23T10:30:00Z"},
                ],
            }
        except Exception as e:
            self.logger.error(f"Failed to get transaction context: {e}")
            return {}

    def _get_customer_context(self, customer_id: str) -> Dict[str, Any]:
        """Get customer history and context."""
        try:
            # This would query customer data and transaction history
            return {
                "account_age_days": 45,
                "total_transactions": 12,
                "total_spent": 2450.75,
                "avg_transaction_amount": 204.23,
                "previous_fraud_cases": 0,
                "risk_score": 0.3,
                "recent_activity": [
                    {
                        "date": "2025-07-22",
                        "transaction_count": 2,
                        "total_amount": 145.50,
                    },
                    {
                        "date": "2025-07-21",
                        "transaction_count": 1,
                        "total_amount": 89.99,
                    },
                    {
                        "date": "2025-07-20",
                        "transaction_count": 0,
                        "total_amount": 0.00,
                    },
                ],
                "patterns": {
                    "typical_transaction_time": "evening",
                    "typical_amount_range": "50-250",
                    "typical_merchants": ["Electronics", "Clothing", "Food"],
                },
            }
        except Exception as e:
            self.logger.error(f"Failed to get customer context: {e}")
            return {}

    def _get_merchant_context(self, merchant_id: str) -> Dict[str, Any]:
        """Get merchant information and risk profile."""
        try:
            # This would query merchant data and risk scores
            return {
                "merchant_name": "TechGadgets Store",
                "merchant_category": "Electronics",
                "merchant_risk_score": 0.4,
                "fraud_rate": 0.02,
                "total_transactions_today": 156,
                "chargeback_rate": 0.008,
                "recent_fraud_cases": 3,
                "verification_status": "verified",
                "account_age_days": 245,
            }
        except Exception as e:
            self.logger.error(f"Failed to get merchant context: {e}")
            return {}

    def _summarize_evidence(self, evidence_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a summary of evidence data."""
        try:
            # Extract key information from evidence
            summary = {
                "data_points": len(evidence_data),
                "key_fields": list(evidence_data.keys())[:5],  # Top 5 fields
            }

            # Add specific summaries based on evidence type
            if "alert_priority" in evidence_data:
                summary["alert_priority"] = evidence_data["alert_priority"]

            if "fraud_probability" in evidence_data:
                summary["fraud_probability"] = evidence_data["fraud_probability"]

            if "triggered_rules" in evidence_data:
                summary["rules_triggered"] = len(evidence_data["triggered_rules"])

            return summary

        except Exception as e:
            self.logger.error(f"Failed to summarize evidence: {e}")
            return {"error": "Failed to summarize evidence"}

    def _build_case_timeline(self, case: FraudCase) -> List[Dict[str, Any]]:
        """Build a chronological timeline of case events."""
        timeline = []

        # Add case creation
        timeline.append(
            {
                "timestamp": case.created_at.isoformat(),
                "event_type": "case_created",
                "description": f"Case created by {case.created_by}",
                "details": {"priority": case.priority.value},
            }
        )

        # Add all actions
        for action in case.actions:
            timeline.append(
                {
                    "timestamp": action.performed_at.isoformat(),
                    "event_type": action.action_type.value,
                    "description": action.comment
                    or f"Action: {action.action_type.value}",
                    "performed_by": action.performed_by,
                    "details": action.details,
                }
            )

        # Add evidence collection
        for evidence in case.evidence:
            timeline.append(
                {
                    "timestamp": evidence.collected_at.isoformat(),
                    "event_type": "evidence_collected",
                    "description": f"Evidence collected: {evidence.evidence_type}",
                    "performed_by": evidence.collected_by,
                    "details": {"evidence_type": evidence.evidence_type},
                }
            )

        # Sort by timestamp
        timeline.sort(key=lambda x: x["timestamp"])

        return timeline

    def get_investigation_tools(self, case_id: str) -> Dict[str, Any]:
        """
        Get investigation tools and analysis for a specific case.

        Args:
            case_id: Case ID to analyze

        Returns:
            Investigation tools and analysis results
        """
        case = self.case_manager.active_cases.get(case_id)
        if not case:
            return {"error": "Case not found"}

        try:
            tools = {}

            # Transaction pattern analysis
            if self.config["investigation_tools"]["transaction_analysis_enabled"]:
                tools["transaction_analysis"] = self._analyze_transaction_patterns(case)

            # Customer behavior analysis
            if self.config["investigation_tools"]["customer_history_enabled"]:
                tools["customer_analysis"] = self._analyze_customer_behavior(case)

            # Merchant risk analysis
            if self.config["investigation_tools"]["merchant_analysis_enabled"]:
                tools["merchant_analysis"] = self._analyze_merchant_risk(case)

            # ML model explanation
            if self.config["investigation_tools"]["ml_model_explainability"]:
                tools["ml_explanation"] = self._explain_ml_prediction(case)

            # Similar cases analysis
            tools["similar_cases"] = self._find_similar_cases(case)

            # Risk assessment summary
            tools["risk_assessment"] = self._generate_risk_assessment(case)

            return tools

        except Exception as e:
            self.logger.error(
                f"Failed to get investigation tools for case {case_id}: {e}"
            )
            return {"error": str(e)}

    def _analyze_transaction_patterns(self, case: FraudCase) -> Dict[str, Any]:
        """Analyze transaction patterns for anomalies."""
        return {
            "velocity_analysis": {
                "transactions_last_hour": 1,
                "transactions_last_day": 3,
                "typical_daily_count": 1.2,
                "velocity_anomaly": False,
            },
            "amount_analysis": {
                "current_amount": case.transaction_amount,
                "customer_avg_amount": 150.50,
                "merchant_avg_amount": 89.75,
                "amount_anomaly": case.transaction_amount > 500,
            },
            "timing_analysis": {
                "transaction_hour": 14,
                "customer_typical_hours": [18, 19, 20],
                "timing_anomaly": True,
            },
            "location_analysis": {
                "current_location": "San Francisco, CA",
                "customer_typical_locations": ["San Francisco, CA", "Oakland, CA"],
                "location_anomaly": False,
            },
        }

    def _analyze_customer_behavior(self, case: FraudCase) -> Dict[str, Any]:
        """Analyze customer behavior patterns."""
        return {
            "behavior_score": 0.3,
            "account_maturity": "new_customer",  # new, established, long_term
            "spending_patterns": {
                "consistency": "variable",
                "trend": "increasing",
                "seasonal_patterns": False,
            },
            "device_patterns": {
                "device_consistency": "consistent",
                "new_device_usage": False,
                "suspicious_devices": 0,
            },
            "payment_patterns": {
                "payment_method_consistency": "consistent",
                "new_payment_methods": 0,
                "failed_payments": 1,
            },
        }

    def _analyze_merchant_risk(self, case: FraudCase) -> Dict[str, Any]:
        """Analyze merchant risk factors."""
        return {
            "merchant_risk_level": "medium",
            "risk_factors": [
                "High-value electronics category",
                "Recent increase in fraud reports",
            ],
            "merchant_metrics": {
                "fraud_rate": 0.02,
                "chargeback_rate": 0.008,
                "dispute_rate": 0.015,
            },
            "recent_activity": {
                "fraud_cases_last_week": 3,
                "transaction_volume_change": "+15%",
                "new_customer_ratio": 0.4,
            },
        }

    def _explain_ml_prediction(self, case: FraudCase) -> Dict[str, Any]:
        """Provide explanation for ML model prediction."""
        if case.ml_prediction is None:
            return {"error": "No ML prediction available"}

        return {
            "prediction_score": case.ml_prediction,
            "confidence": "high"
            if case.ml_prediction > 0.8
            else "medium"
            if case.ml_prediction > 0.5
            else "low",
            "key_factors": [
                {
                    "factor": "Transaction amount",
                    "impact": 0.3,
                    "direction": "increases_risk",
                },
                {"factor": "New device", "impact": 0.2, "direction": "increases_risk"},
                {
                    "factor": "Customer history",
                    "impact": -0.1,
                    "direction": "decreases_risk",
                },
                {
                    "factor": "Merchant reputation",
                    "impact": 0.15,
                    "direction": "increases_risk",
                },
            ],
            "model_version": "v1.0.0",
            "explanation": "High fraud probability due to large transaction amount on new device, but mitigated by positive customer history.",
        }

    def _find_similar_cases(
        self, case: FraudCase, limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Find similar cases for pattern analysis."""
        similar_cases = []

        # This would use ML similarity algorithms in production
        # For now, returning mock similar cases
        for i in range(min(limit, 3)):
            similar_cases.append(
                {
                    "case_id": f"case_{i+1}_similar",
                    "similarity_score": 0.85 - (i * 0.1),
                    "common_factors": [
                        "Same merchant category",
                        "Similar transaction amount",
                        "Same payment method",
                    ],
                    "resolution": "resolved_fraud" if i == 0 else "resolved_legitimate",
                    "resolution_confidence": 0.9 - (i * 0.1),
                }
            )

        return similar_cases

    def _generate_risk_assessment(self, case: FraudCase) -> Dict[str, Any]:
        """Generate overall risk assessment for the case."""
        # Combine all risk factors
        risk_factors = []
        risk_score = case.fraud_score

        if case.transaction_amount > 1000:
            risk_factors.append("High transaction amount")
            risk_score += 0.1

        if case.ml_prediction and case.ml_prediction > 0.7:
            risk_factors.append("High ML fraud probability")
            risk_score += 0.15

        if len(case.rule_triggers) > 2:
            risk_factors.append("Multiple rule triggers")
            risk_score += 0.1

        # Determine risk level
        if risk_score >= 0.8:
            risk_level = "critical"
        elif risk_score >= 0.6:
            risk_level = "high"
        elif risk_score >= 0.4:
            risk_level = "medium"
        else:
            risk_level = "low"

        return {
            "overall_risk_score": min(risk_score, 1.0),
            "risk_level": risk_level,
            "risk_factors": risk_factors,
            "recommendation": self._get_investigation_recommendation(risk_level, case),
            "confidence": 0.85,
        }

    def _get_investigation_recommendation(
        self, risk_level: str, case: FraudCase
    ) -> str:
        """Get investigation recommendation based on risk level."""
        recommendations = {
            "critical": "Block transaction immediately and conduct thorough investigation. Contact customer for verification.",
            "high": "Place transaction on hold and investigate within 2 hours. Verify with customer if needed.",
            "medium": "Review transaction details and customer history. Monitor for additional suspicious activity.",
            "low": "Routine review sufficient. Consider for training data if resolution is clear.",
        }
        return recommendations.get(
            risk_level, "Standard investigation procedures apply."
        )

    def get_workload_summary(self) -> Dict[str, Any]:
        """Get workload summary for the investigator."""
        try:
            assigned_cases = self.case_manager.get_cases_by_investigator(
                self.investigator_id
            )

            # Categorize by status
            status_counts = {}
            for status in CaseStatus:
                status_counts[status.value] = len(
                    [case for case in assigned_cases if case.status == status]
                )

            # Categorize by priority
            priority_counts = {}
            for priority in CasePriority:
                priority_counts[priority.value] = len(
                    [case for case in assigned_cases if case.priority == priority]
                )

            # Calculate metrics
            overdue_cases = [
                case
                for case in assigned_cases
                if case.investigation_deadline
                and datetime.now() > case.investigation_deadline
                and case.status
                not in [
                    CaseStatus.RESOLVED_FRAUD,
                    CaseStatus.RESOLVED_LEGITIMATE,
                    CaseStatus.CLOSED,
                ]
            ]

            urgent_cases = [
                case
                for case in assigned_cases
                if case.investigation_deadline
                and (case.investigation_deadline - datetime.now()).total_seconds()
                < 3600
                and case.status
                not in [
                    CaseStatus.RESOLVED_FRAUD,
                    CaseStatus.RESOLVED_LEGITIMATE,
                    CaseStatus.CLOSED,
                ]
            ]

            return {
                "investigator_id": self.investigator_id,
                "total_assigned_cases": len(assigned_cases),
                "active_cases": len(
                    [
                        case
                        for case in assigned_cases
                        if case.status
                        in [
                            CaseStatus.OPEN,
                            CaseStatus.IN_PROGRESS,
                            CaseStatus.UNDER_REVIEW,
                        ]
                    ]
                ),
                "status_breakdown": status_counts,
                "priority_breakdown": priority_counts,
                "overdue_cases": len(overdue_cases),
                "urgent_cases": len(urgent_cases),
                "workload_status": self._assess_workload_status(len(assigned_cases)),
                "next_deadline": self._get_next_deadline(assigned_cases),
            }

        except Exception as e:
            self.logger.error(f"Failed to get workload summary: {e}")
            return {"error": str(e)}

    def _assess_workload_status(self, case_count: int) -> str:
        """Assess workload status based on case count."""
        if case_count >= self.config["workload_warning_threshold"]:
            return "overloaded"
        elif case_count >= self.config["workload_warning_threshold"] * 0.7:
            return "busy"
        elif case_count >= self.config["workload_warning_threshold"] * 0.3:
            return "normal"
        else:
            return "light"

    def _get_next_deadline(self, cases: List[FraudCase]) -> Optional[str]:
        """Get the next upcoming deadline."""
        upcoming_deadlines = [
            case.investigation_deadline
            for case in cases
            if case.investigation_deadline
            and case.investigation_deadline > datetime.now()
            and case.status
            not in [
                CaseStatus.RESOLVED_FRAUD,
                CaseStatus.RESOLVED_LEGITIMATE,
                CaseStatus.CLOSED,
            ]
        ]

        if upcoming_deadlines:
            next_deadline = min(upcoming_deadlines)
            return next_deadline.isoformat()

        return None

    def get_performance_metrics(self, time_period_days: int = 30) -> Dict[str, Any]:
        """Get investigator performance metrics."""
        try:
            cutoff_time = datetime.now() - timedelta(days=time_period_days)

            # Get all cases handled by this investigator
            all_cases = [
                case
                for case in self.case_manager.active_cases.values()
                if case.assigned_to == self.investigator_id
                and case.created_at >= cutoff_time
            ]

            resolved_cases = [
                case
                for case in all_cases
                if case.status
                in [
                    CaseStatus.RESOLVED_FRAUD,
                    CaseStatus.RESOLVED_LEGITIMATE,
                    CaseStatus.CLOSED,
                ]
            ]

            # Calculate performance metrics
            total_cases = len(all_cases)
            total_resolved = len(resolved_cases)
            resolution_rate = (total_resolved / total_cases) if total_cases > 0 else 0

            # Calculate average resolution time
            resolution_times = []
            for case in resolved_cases:
                if case.created_at and case.resolution_time:
                    resolution_time = (
                        case.resolution_time - case.created_at
                    ).total_seconds() / 3600
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

            sla_compliance = (
                (sla_compliant / total_resolved) if total_resolved > 0 else 0
            )

            return {
                "investigator_id": self.investigator_id,
                "time_period_days": time_period_days,
                "cases_handled": total_cases,
                "cases_resolved": total_resolved,
                "resolution_rate": resolution_rate,
                "average_resolution_time_hours": avg_resolution_time,
                "sla_compliance_rate": sla_compliance,
                "fraud_detection_accuracy": self._calculate_fraud_accuracy(
                    resolved_cases
                ),
                "productivity_trend": "stable",  # Would calculate from historical data
            }

        except Exception as e:
            self.logger.error(f"Failed to get performance metrics: {e}")
            return {"error": str(e)}

    def _calculate_fraud_accuracy(self, resolved_cases: List[FraudCase]) -> float:
        """Calculate fraud detection accuracy for resolved cases."""
        if not resolved_cases:
            return 0.0

        # This would compare investigator decisions with ground truth
        # For now, using resolution confidence as a proxy
        total_confidence = sum(
            case.resolution_confidence or 0.8 for case in resolved_cases
        )
        return total_confidence / len(resolved_cases)

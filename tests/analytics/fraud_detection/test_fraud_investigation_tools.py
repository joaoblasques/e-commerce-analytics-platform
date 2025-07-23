"""
Tests for fraud investigation tools.

Comprehensive test suite for case management, investigator dashboard,
fraud reporting, and feedback system components.
"""

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.analytics.fraud_detection.case_management import (
    CasePriority,
    CaseStatus,
    FraudCase,
    FraudCaseManager,
    InvestigationAction,
)
from src.analytics.fraud_detection.feedback_system import (
    FalsePositiveFeedbackSystem,
    FeedbackSource,
    FeedbackType,
)
from src.analytics.fraud_detection.fraud_reporting import FraudReportingEngine
from src.analytics.fraud_detection.investigator_dashboard import InvestigatorDashboard


class TestFraudCaseManager:
    """Test cases for FraudCaseManager."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = MagicMock()
        spark.createDataFrame.return_value = MagicMock()
        return spark

    @pytest.fixture
    def case_manager(self, mock_spark):
        """Create case manager instance."""
        return FraudCaseManager(mock_spark)

    @pytest.fixture
    def sample_alert_data(self):
        """Sample alert data for testing."""
        return {
            "alert_id": "alert_123",
            "transaction_id": "txn_456",
            "customer_id": "cust_789",
            "merchant_id": "merchant_101",
            "price": 250.50,
            "fraud_score": 0.75,
            "triggered_rules": ["high_amount", "new_device"],
            "ml_fraud_probability": 0.82,
        }

    def test_create_case_from_alert(self, case_manager, sample_alert_data):
        """Test case creation from alert."""
        case = case_manager.create_case_from_alert(
            alert_data=sample_alert_data, created_by="system"
        )

        assert isinstance(case, FraudCase)
        assert case.transaction_id == "txn_456"
        assert case.customer_id == "cust_789"
        assert case.merchant_id == "merchant_101"
        assert case.transaction_amount == 250.50
        assert case.fraud_score == 0.75
        assert case.status == CaseStatus.OPEN
        assert case.priority == CasePriority.HIGH  # Based on fraud score
        assert len(case.evidence) == 1  # Alert evidence
        assert len(case.actions) == 1  # Creation action

    def test_determine_case_priority(self, case_manager):
        """Test case priority determination logic."""
        # High fraud score should result in critical priority
        high_score_alert = {"fraud_score": 0.95, "price": 100}
        priority = case_manager._determine_case_priority(high_score_alert)
        assert priority == CasePriority.CRITICAL

        # High value transaction should result in high priority
        high_value_alert = {"fraud_score": 0.5, "price": 15000}
        priority = case_manager._determine_case_priority(high_value_alert)
        assert priority == CasePriority.HIGH

        # Low fraud score should result in low priority
        low_score_alert = {"fraud_score": 0.2, "price": 50}
        priority = case_manager._determine_case_priority(low_score_alert)
        assert priority == CasePriority.LOW

    def test_assign_case(self, case_manager, sample_alert_data):
        """Test case assignment to investigator."""
        case = case_manager.create_case_from_alert(sample_alert_data, "system")

        success = case_manager.assign_case(
            case_id=case.case_id, investigator_id="inv_001", assigned_by="supervisor"
        )

        assert success is True
        assert case.assigned_to == "inv_001"
        assert case.status == CaseStatus.IN_PROGRESS
        assert len(case.actions) == 3  # Creation + assignment + status change

    def test_update_case_status(self, case_manager, sample_alert_data):
        """Test case status updates."""
        case = case_manager.create_case_from_alert(sample_alert_data, "system")

        success = case_manager.update_case_status(
            case_id=case.case_id,
            new_status=CaseStatus.UNDER_REVIEW,
            updated_by="inv_001",
            comment="Moving to review",
        )

        assert success is True
        assert case.status == CaseStatus.UNDER_REVIEW
        assert case.updated_at > case.created_at

        # Check action was recorded
        status_actions = [
            action
            for action in case.actions
            if action.action_type == InvestigationAction.STATUS_CHANGED
        ]
        assert len(status_actions) == 2  # Creation + this update

    def test_add_case_comment(self, case_manager, sample_alert_data):
        """Test adding comments to cases."""
        case = case_manager.create_case_from_alert(sample_alert_data, "system")

        success = case_manager.add_case_comment(
            case_id=case.case_id,
            comment="This looks suspicious due to velocity patterns",
            added_by="inv_001",
        )

        assert success is True
        assert len(case.investigation_notes) == 1
        assert "suspicious due to velocity patterns" in case.investigation_notes[0]

        # Check comment action was recorded
        comment_actions = [
            action
            for action in case.actions
            if action.action_type == InvestigationAction.COMMENT_ADDED
        ]
        assert len(comment_actions) == 1

    def test_add_evidence(self, case_manager, sample_alert_data):
        """Test adding evidence to cases."""
        case = case_manager.create_case_from_alert(sample_alert_data, "system")

        evidence_data = {
            "device_fingerprint": "abc123",
            "ip_location": "San Francisco, CA",
            "previous_transactions": 5,
        }

        success = case_manager.add_evidence(
            case_id=case.case_id,
            evidence_type="device_analysis",
            evidence_data=evidence_data,
            collected_by="inv_001",
            description="Device fingerprint analysis results",
        )

        assert success is True
        assert len(case.evidence) == 2  # Original alert + new evidence

        new_evidence = case.evidence[1]
        assert new_evidence.evidence_type == "device_analysis"
        assert new_evidence.evidence_data == evidence_data
        assert new_evidence.description == "Device fingerprint analysis results"

    def test_resolve_case(self, case_manager, sample_alert_data):
        """Test case resolution."""
        case = case_manager.create_case_from_alert(sample_alert_data, "system")

        success = case_manager.resolve_case(
            case_id=case.case_id,
            resolution="fraud",
            confidence=0.95,
            reason="Clear indicators of account takeover",
            resolved_by="inv_001",
        )

        assert success is True
        assert case.resolution == "fraud"
        assert case.resolution_confidence == 0.95
        assert case.resolution_reason == "Clear indicators of account takeover"
        assert case.status == CaseStatus.RESOLVED_FRAUD
        assert case.resolution_time is not None

    def test_get_performance_metrics(self, case_manager, sample_alert_data):
        """Test performance metrics calculation."""
        # Create multiple cases with different outcomes
        for i in range(5):
            alert_data = sample_alert_data.copy()
            alert_data["transaction_id"] = f"txn_{i}"
            alert_data["alert_id"] = f"alert_{i}"

            case = case_manager.create_case_from_alert(alert_data, "system")

            # Resolve some cases
            if i < 3:
                case_manager.resolve_case(
                    case_id=case.case_id,
                    resolution="fraud" if i % 2 == 0 else "legitimate",
                    confidence=0.9,
                    reason="Test resolution",
                    resolved_by="inv_001",
                )

        metrics = case_manager.get_performance_metrics(time_period_days=30)

        assert metrics["total_cases"] == 5
        assert metrics["total_resolved"] == 3
        assert metrics["resolution_rate"] == 0.6  # 3/5
        assert "priority_distribution" in metrics
        assert "overdue_cases" in metrics


class TestInvestigatorDashboard:
    """Test cases for InvestigatorDashboard."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        return MagicMock()

    @pytest.fixture
    def mock_case_manager(self):
        """Mock case manager."""
        case_manager = MagicMock()

        # Create sample cases
        case1 = FraudCase(
            case_id="case_1",
            transaction_id="txn_1",
            alert_id="alert_1",
            status=CaseStatus.IN_PROGRESS,
            priority=CasePriority.HIGH,
            assigned_to="inv_001",
            created_at=datetime.now() - timedelta(hours=2),
            updated_at=datetime.now() - timedelta(minutes=30),
            created_by="system",
            customer_id="cust_1",
            merchant_id="merchant_1",
            transaction_amount=500.0,
            fraud_score=0.8,
            rule_triggers=["velocity", "device"],
            ml_prediction=0.75,
            investigation_deadline=datetime.now() + timedelta(hours=22),
            evidence=[],
            actions=[],
        )

        case_manager.get_cases_by_investigator.return_value = [case1]
        case_manager.active_cases = {"case_1": case1}

        return case_manager

    @pytest.fixture
    def dashboard(self, mock_spark, mock_case_manager):
        """Create dashboard instance."""
        return InvestigatorDashboard(
            spark=mock_spark, case_manager=mock_case_manager, investigator_id="inv_001"
        )

    def test_get_case_queue(self, dashboard):
        """Test case queue retrieval."""
        queue = dashboard.get_case_queue(limit=10)

        assert len(queue) == 1
        case_summary = queue[0]

        assert case_summary["case_id"] == "case_1"
        assert case_summary["status"] == "in_progress"
        assert case_summary["priority"] == "high"
        assert case_summary["transaction_amount"] == 500.0
        assert case_summary["fraud_score"] == 0.8
        assert case_summary["sla_status"] == "on_time"
        assert "hours_elapsed" in case_summary
        assert "hours_remaining" in case_summary

    def test_get_case_details(self, dashboard):
        """Test detailed case information retrieval."""
        with patch.object(
            dashboard, "_get_transaction_context"
        ) as mock_txn_ctx, patch.object(
            dashboard, "_get_customer_context"
        ) as mock_cust_ctx, patch.object(
            dashboard, "_get_merchant_context"
        ) as mock_merch_ctx:
            mock_txn_ctx.return_value = {"transaction_time": "2025-07-23T10:30:00Z"}
            mock_cust_ctx.return_value = {"account_age_days": 45}
            mock_merch_ctx.return_value = {"merchant_risk_score": 0.4}

            details = dashboard.get_case_details("case_1")

            assert details is not None
            assert details["case_info"]["case_id"] == "case_1"
            assert details["transaction_info"]["transaction_amount"] == 500.0
            assert "context" in details
            assert "investigation" in details
            assert "timeline" in details

    def test_get_investigation_tools(self, dashboard):
        """Test investigation tools and analysis."""
        with patch.object(
            dashboard, "_analyze_transaction_patterns"
        ) as mock_txn_analysis, patch.object(
            dashboard, "_analyze_customer_behavior"
        ) as mock_cust_analysis, patch.object(
            dashboard, "_analyze_merchant_risk"
        ) as mock_merch_analysis:
            mock_txn_analysis.return_value = {"velocity_anomaly": True}
            mock_cust_analysis.return_value = {"behavior_score": 0.3}
            mock_merch_analysis.return_value = {"merchant_risk_level": "medium"}

            tools = dashboard.get_investigation_tools("case_1")

            assert "transaction_analysis" in tools
            assert "customer_analysis" in tools
            assert "merchant_analysis" in tools
            assert "similar_cases" in tools
            assert "risk_assessment" in tools

    def test_get_workload_summary(self, dashboard):
        """Test workload summary calculation."""
        summary = dashboard.get_workload_summary()

        assert summary["investigator_id"] == "inv_001"
        assert summary["total_assigned_cases"] == 1
        assert summary["active_cases"] == 1
        assert "status_breakdown" in summary
        assert "priority_breakdown" in summary
        assert "workload_status" in summary


class TestFraudReportingEngine:
    """Test cases for FraudReportingEngine."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        return MagicMock()

    @pytest.fixture
    def mock_case_manager(self):
        """Mock case manager with sample data."""
        case_manager = MagicMock()
        case_manager.get_performance_metrics.return_value = {
            "total_cases": 100,
            "total_resolved": 85,
            "resolution_rate": 0.85,
            "sla_compliance_rate": 0.92,
            "average_resolution_time_hours": 18.5,
            "overdue_cases": 3,
        }
        return case_manager

    @pytest.fixture
    def reporting_engine(self, mock_spark, mock_case_manager):
        """Create reporting engine instance."""
        return FraudReportingEngine(mock_spark, mock_case_manager)

    def test_generate_executive_dashboard(self, reporting_engine):
        """Test executive dashboard generation."""
        with patch.object(
            reporting_engine, "_calculate_financial_impact"
        ) as mock_financial, patch.object(
            reporting_engine, "_analyze_fraud_trends"
        ) as mock_trends, patch.object(
            reporting_engine, "_calculate_system_performance"
        ) as mock_performance:
            mock_financial.return_value = {"total_fraud_prevented": 125000}
            mock_trends.return_value = {"trend_direction": "stable"}
            mock_performance.return_value = {"detection_accuracy": 0.87}

            dashboard = reporting_engine.generate_executive_dashboard(
                time_period_days=30
            )

            assert "report_metadata" in dashboard
            assert "key_metrics" in dashboard
            assert "financial_impact" in dashboard
            assert "trend_analysis" in dashboard
            assert "system_performance" in dashboard

            assert dashboard["key_metrics"]["total_cases_investigated"] == 100
            assert dashboard["key_metrics"]["resolution_rate"] == 0.85

    def test_generate_investigator_performance_report(self, reporting_engine):
        """Test investigator performance report generation."""
        # Mock case manager to return sample cases
        reporting_engine.case_manager.active_cases = {
            "case_1": MagicMock(
                assigned_to="inv_001",
                created_at=datetime.now() - timedelta(days=5),
                status=CaseStatus.RESOLVED_FRAUD,
                priority=CasePriority.HIGH,
                resolution_time=datetime.now() - timedelta(days=4),
                investigation_deadline=datetime.now(),
            )
        }

        report = reporting_engine.generate_investigator_performance_report(
            investigator_id="inv_001", time_period_days=30
        )

        assert "report_metadata" in report
        assert "investigator_reports" in report
        assert "team_summary" in report

        assert len(report["investigator_reports"]) == 1
        inv_report = report["investigator_reports"][0]
        assert inv_report["investigator_id"] == "inv_001"
        assert "performance_metrics" in inv_report
        assert "case_distribution" in inv_report

    def test_generate_fraud_trend_analysis(self, reporting_engine):
        """Test fraud trend analysis generation."""
        with patch.object(
            reporting_engine, "_generate_time_series_analysis"
        ) as mock_timeseries, patch.object(
            reporting_engine, "_analyze_fraud_patterns"
        ) as mock_patterns, patch.object(
            reporting_engine, "_analyze_seasonal_patterns"
        ) as mock_seasonal:
            mock_timeseries.return_value = {
                "trend_indicators": {"volume_trend": "stable"}
            }
            mock_patterns.return_value = {"top_attack_vectors": []}
            mock_seasonal.return_value = {
                "hourly_patterns": {"peak_hours": [14, 15, 16]}
            }

            analysis = reporting_engine.generate_fraud_trend_analysis(
                time_period_days=90
            )

            assert "report_metadata" in analysis
            assert "time_series_analysis" in analysis
            assert "pattern_analysis" in analysis
            assert "seasonal_analysis" in analysis
            assert "recommendations" in analysis

    def test_generate_compliance_report(self, reporting_engine):
        """Test compliance report generation."""
        # Mock active cases
        reporting_engine.case_manager.active_cases = {
            "case_1": MagicMock(
                created_at=datetime.now() - timedelta(days=5),
                investigation_notes=["Note 1", "Note 2"],
                resolution_time=datetime.now() - timedelta(days=4),
                investigation_deadline=datetime.now(),
            )
        }

        report = reporting_engine.generate_compliance_report(
            time_period_days=30, report_type="regulatory"
        )

        assert "report_metadata" in report
        assert "compliance_metrics" in report
        assert "audit_trail" in report
        assert "data_quality_assessment" in report
        assert "regulatory_requirements" in report


class TestFalsePositiveFeedbackSystem:
    """Test cases for FalsePositiveFeedbackSystem."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = MagicMock()
        spark.createDataFrame.return_value = MagicMock()
        return spark

    @pytest.fixture
    def mock_case_manager(self):
        """Mock case manager."""
        case_manager = MagicMock()

        # Create sample case
        sample_case = FraudCase(
            case_id="case_1",
            transaction_id="txn_1",
            alert_id="alert_1",
            status=CaseStatus.RESOLVED_LEGITIMATE,
            priority=CasePriority.MEDIUM,
            assigned_to="inv_001",
            created_at=datetime.now() - timedelta(hours=4),
            updated_at=datetime.now() - timedelta(minutes=30),
            created_by="system",
            customer_id="cust_1",
            merchant_id="merchant_1",
            transaction_amount=200.0,
            fraud_score=0.6,
            rule_triggers=["new_device"],
            ml_prediction=0.65,
        )

        case_manager.active_cases = {"case_1": sample_case}
        return case_manager

    @pytest.fixture
    def feedback_system(self, mock_spark, mock_case_manager):
        """Create feedback system instance."""
        return FalsePositiveFeedbackSystem(mock_spark, mock_case_manager)

    def test_submit_investigator_feedback(self, feedback_system):
        """Test investigator feedback submission."""
        success = feedback_system.submit_investigator_feedback(
            case_id="case_1",
            feedback_type=FeedbackType.FALSE_POSITIVE,
            justification="Customer verified purchase with additional documentation. Legitimate transaction from known good customer.",
            investigator_id="inv_001",
            confidence=0.95,
        )

        assert success is True
        assert len(feedback_system.feedback_entries) == 1

        feedback = feedback_system.feedback_entries[0]
        assert feedback.case_id == "case_1"
        assert feedback.feedback_type == FeedbackType.FALSE_POSITIVE
        assert feedback.feedback_source == FeedbackSource.INVESTIGATOR
        assert feedback.confidence_score == 0.95
        assert feedback.actual_outcome is False  # False positive means not fraud

    def test_submit_customer_dispute(self, feedback_system):
        """Test customer dispute submission."""
        success = feedback_system.submit_customer_dispute(
            transaction_id="txn_1",
            dispute_type="chargeback",
            dispute_reason="Customer did not authorize transaction",
            resolution="customer_favor",
            customer_id="cust_1",
        )

        assert success is True
        assert len(feedback_system.feedback_entries) == 1

        feedback = feedback_system.feedback_entries[0]
        assert feedback.transaction_id == "txn_1"
        assert feedback.feedback_type == FeedbackType.FALSE_POSITIVE
        assert feedback.feedback_source == FeedbackSource.CUSTOMER
        assert feedback.feedback_details["dispute_type"] == "chargeback"

    def test_feedback_validation(self, feedback_system):
        """Test feedback input validation."""
        # Test short justification
        success = feedback_system.submit_investigator_feedback(
            case_id="case_1",
            feedback_type=FeedbackType.FALSE_POSITIVE,
            justification="Too short",  # Less than minimum length
            investigator_id="inv_001",
        )

        assert success is False
        assert len(feedback_system.feedback_entries) == 0

    def test_get_false_positive_analysis(self, feedback_system):
        """Test false positive analysis."""
        # Submit some feedback first
        feedback_system.submit_investigator_feedback(
            case_id="case_1",
            feedback_type=FeedbackType.FALSE_POSITIVE,
            justification="Legitimate customer transaction with proper verification",
            investigator_id="inv_001",
        )

        analysis = feedback_system.get_false_positive_analysis(time_period_days=30)

        assert "total_false_positives" in analysis
        assert "false_positive_rate" in analysis
        assert "amount_distribution" in analysis
        assert "improvement_recommendations" in analysis
        assert analysis["total_false_positives"] == 1

    def test_get_feedback_quality_report(self, feedback_system):
        """Test feedback quality report generation."""
        # Submit feedback with different types and sources
        feedback_system.submit_investigator_feedback(
            case_id="case_1",
            feedback_type=FeedbackType.FALSE_POSITIVE,
            justification="Legitimate transaction confirmed through customer contact",
            investigator_id="inv_001",
            confidence=0.9,
        )

        feedback_system.submit_customer_dispute(
            transaction_id="txn_1",
            dispute_type="inquiry",
            dispute_reason="Customer needs clarification",
            resolution="merchant_favor",
        )

        report = feedback_system.get_feedback_quality_report()

        assert "feedback_metrics" in report
        assert "distribution_analysis" in report
        assert "quality_indicators" in report

        assert report["feedback_metrics"]["total_feedback"] == 2
        assert (
            report["distribution_analysis"]["feedback_source_distribution"][
                "investigator"
            ]
            == 1
        )
        assert (
            report["distribution_analysis"]["feedback_source_distribution"]["customer"]
            == 1
        )

    def test_export_training_data(self, feedback_system):
        """Test training data export functionality."""
        # Submit and process feedback
        feedback_system.submit_investigator_feedback(
            case_id="case_1",
            feedback_type=FeedbackType.FALSE_POSITIVE,
            justification="Clear false positive case",
            investigator_id="inv_001",
        )

        # Mark as processed
        feedback_system.feedback_entries[0].processed = True

        with patch.object(feedback_system.spark, "createDataFrame") as mock_create_df:
            mock_df = MagicMock()
            mock_create_df.return_value = mock_df

            success = feedback_system.export_training_data(
                output_path="test/path", time_period_days=30
            )

            assert success is True
            mock_create_df.assert_called_once()
            mock_df.write.mode.assert_called_with("overwrite")


class TestIntegration:
    """Integration tests for fraud investigation tools."""

    @pytest.fixture
    def integrated_system(self):
        """Create integrated system with all components."""
        mock_spark = MagicMock()
        mock_spark.createDataFrame.return_value = MagicMock()

        case_manager = FraudCaseManager(mock_spark)
        dashboard = InvestigatorDashboard(mock_spark, case_manager, "inv_001")
        reporting_engine = FraudReportingEngine(mock_spark, case_manager)
        feedback_system = FalsePositiveFeedbackSystem(mock_spark, case_manager)

        return {
            "case_manager": case_manager,
            "dashboard": dashboard,
            "reporting": reporting_engine,
            "feedback": feedback_system,
        }

    def test_end_to_end_case_workflow(self, integrated_system):
        """Test complete case workflow from creation to resolution."""
        case_manager = integrated_system["case_manager"]
        dashboard = integrated_system["dashboard"]
        feedback_system = integrated_system["feedback"]

        # Create case from alert
        alert_data = {
            "alert_id": "alert_123",
            "transaction_id": "txn_456",
            "customer_id": "cust_789",
            "merchant_id": "merchant_101",
            "price": 750.0,
            "fraud_score": 0.8,
            "triggered_rules": ["velocity", "amount"],
            "ml_fraud_probability": 0.75,
        }

        case = case_manager.create_case_from_alert(alert_data, "system")
        assert case.case_id in case_manager.active_cases

        # Assign case
        case_manager.assign_case(case.case_id, "inv_001", "supervisor")

        # Get case queue from dashboard
        queue = dashboard.get_case_queue()
        assert len(queue) == 1
        assert queue[0]["case_id"] == case.case_id

        # Add investigation notes
        case_manager.add_case_comment(
            case.case_id,
            "Contacted customer - confirmed legitimate purchase",
            "inv_001",
        )

        # Resolve as false positive
        case_manager.resolve_case(
            case.case_id,
            "legitimate",
            0.9,
            "Customer confirmed purchase with receipt",
            "inv_001",
        )

        # Submit feedback
        feedback_success = feedback_system.submit_investigator_feedback(
            case.case_id,
            FeedbackType.FALSE_POSITIVE,
            "Customer provided valid receipt and ID verification",
            "inv_001",
        )

        assert feedback_success is True
        assert len(feedback_system.feedback_entries) == 1

        # Verify case status
        assert case.status == CaseStatus.RESOLVED_LEGITIMATE
        assert case.resolution == "legitimate"
        assert case.resolution_time is not None

    def test_performance_metrics_consistency(self, integrated_system):
        """Test consistency of performance metrics across components."""
        case_manager = integrated_system["case_manager"]
        reporting_engine = integrated_system["reporting"]

        # Create and resolve multiple cases
        for i in range(10):
            alert_data = {
                "alert_id": f"alert_{i}",
                "transaction_id": f"txn_{i}",
                "customer_id": f"cust_{i}",
                "merchant_id": "merchant_101",
                "price": 100.0 + i * 50,
                "fraud_score": 0.5 + i * 0.05,
                "triggered_rules": ["test_rule"],
                "ml_fraud_probability": 0.6,
            }

            case = case_manager.create_case_from_alert(alert_data, "system")

            # Resolve half the cases
            if i < 5:
                case_manager.resolve_case(
                    case.case_id,
                    "fraud" if i % 2 == 0 else "legitimate",
                    0.9,
                    "Test resolution",
                    "inv_001",
                )

        # Get metrics from case manager
        case_metrics = case_manager.get_performance_metrics(30)

        # Verify metrics are consistent
        assert case_metrics["total_cases"] == 10
        assert case_metrics["total_resolved"] == 5
        assert case_metrics["resolution_rate"] == 0.5


if __name__ == "__main__":
    pytest.main([__file__])

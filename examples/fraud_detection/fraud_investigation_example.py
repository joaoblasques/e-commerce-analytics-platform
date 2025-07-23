"""
Fraud Investigation Tools Example

This example demonstrates the complete fraud investigation workflow using
the case management system, investigator dashboard, reporting engine,
and feedback system.
"""

import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from src.analytics.fraud_detection.case_management import (
    CasePriority,
    CaseStatus,
    FraudCaseManager,
)
from src.analytics.fraud_detection.feedback_system import (
    FalsePositiveFeedbackSystem,
    FeedbackType,
)
from src.analytics.fraud_detection.fraud_reporting import FraudReportingEngine
from src.analytics.fraud_detection.investigator_dashboard import InvestigatorDashboard


def create_spark_session():
    """Create Spark session for the example."""
    return (
        SparkSession.builder.appName("FraudInvestigationExample")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def generate_sample_alerts():
    """Generate sample fraud alerts for demonstration."""
    alerts = []

    # High-risk transaction
    alerts.append(
        {
            "alert_id": "alert_001",
            "transaction_id": "txn_001",
            "customer_id": "customer_12345",
            "merchant_id": "merchant_tech_store",
            "price": 2500.00,
            "fraud_score": 0.92,
            "triggered_rules": ["high_amount", "new_device", "velocity_check"],
            "ml_fraud_probability": 0.89,
            "alert_priority": "critical",
            "timestamp": datetime.now() - timedelta(hours=2),
        }
    )

    # Medium-risk transaction
    alerts.append(
        {
            "alert_id": "alert_002",
            "transaction_id": "txn_002",
            "customer_id": "customer_67890",
            "merchant_id": "merchant_fashion_shop",
            "price": 450.00,
            "fraud_score": 0.68,
            "triggered_rules": ["location_change", "payment_method_change"],
            "ml_fraud_probability": 0.72,
            "alert_priority": "high",
            "timestamp": datetime.now() - timedelta(hours=1),
        }
    )

    # Lower-risk transaction (potential false positive)
    alerts.append(
        {
            "alert_id": "alert_003",
            "transaction_id": "txn_003",
            "customer_id": "customer_11111",
            "merchant_id": "merchant_book_store",
            "price": 89.99,
            "fraud_score": 0.45,
            "triggered_rules": ["new_merchant"],
            "ml_fraud_probability": 0.52,
            "alert_priority": "medium",
            "timestamp": datetime.now() - timedelta(minutes=30),
        }
    )

    return alerts


def demonstrate_case_management(spark):
    """Demonstrate case management functionality."""
    print("\\n" + "=" * 60)
    print("FRAUD CASE MANAGEMENT DEMONSTRATION")
    print("=" * 60)

    # Initialize case manager
    case_manager = FraudCaseManager(spark)

    # Generate sample alerts
    alerts = generate_sample_alerts()

    print(f"\\n1. Creating cases from {len(alerts)} fraud alerts...")

    cases = []
    for alert in alerts:
        case = case_manager.create_case_from_alert(
            alert_data=alert, created_by="fraud_detection_system"
        )
        cases.append(case)
        print(f"   Created case {case.case_id} for transaction {case.transaction_id}")
        print(f"   Priority: {case.priority.value}, Fraud Score: {case.fraud_score}")

    print(f"\\n2. Assigning cases to investigators...")

    investigators = ["inv_alice", "inv_bob", "inv_charlie"]

    for i, case in enumerate(cases):
        investigator = investigators[i % len(investigators)]
        success = case_manager.assign_case(
            case_id=case.case_id,
            investigator_id=investigator,
            assigned_by="supervisor_jane",
        )
        if success:
            print(f"   Assigned case {case.case_id} to {investigator}")

    print(f"\\n3. Adding investigation evidence and comments...")

    # Add evidence to the first case
    case = cases[0]
    evidence_success = case_manager.add_evidence(
        case_id=case.case_id,
        evidence_type="device_analysis",
        evidence_data={
            "device_fingerprint": "d1a2b3c4e5f6",
            "ip_address": "192.168.1.100",
            "geolocation": "San Francisco, CA",
            "device_type": "mobile",
            "browser": "Safari 14.0",
        },
        collected_by="inv_alice",
        description="Device fingerprint analysis shows new device from unusual location",
    )

    comment_success = case_manager.add_case_comment(
        case_id=case.case_id,
        comment="High-value transaction on new device. Customer contacted but no response yet. Escalating for immediate review.",
        added_by="inv_alice",
    )

    if evidence_success and comment_success:
        print(f"   Added evidence and comments to case {case.case_id}")

    print(f"\\n4. Resolving cases...")

    # Resolve cases with different outcomes
    resolutions = [
        (
            "fraud",
            0.95,
            "Confirmed fraudulent transaction - unauthorized account access",
        ),
        (
            "legitimate",
            0.85,
            "Customer confirmed purchase - legitimate transaction after verification",
        ),
        (
            "fraud",
            0.90,
            "Suspicious pattern confirmed - multiple failed authentication attempts",
        ),
    ]

    for i, (resolution, confidence, reason) in enumerate(resolutions):
        case = cases[i]
        investigator = investigators[i % len(investigators)]

        success = case_manager.resolve_case(
            case_id=case.case_id,
            resolution=resolution,
            confidence=confidence,
            reason=reason,
            resolved_by=investigator,
        )

        if success:
            print(
                f"   Resolved case {case.case_id} as {resolution} (confidence: {confidence})"
            )

    print(f"\\n5. Case Management Performance Metrics:")
    metrics = case_manager.get_performance_metrics(time_period_days=1)

    print(f"   Total Cases: {metrics['total_cases']}")
    print(f"   Resolved Cases: {metrics['total_resolved']}")
    print(f"   Resolution Rate: {metrics['resolution_rate']:.1%}")
    print(
        f"   Average Resolution Time: {metrics['average_resolution_time_hours']:.1f} hours"
    )
    print(f"   SLA Compliance: {metrics['sla_compliance_rate']:.1%}")

    return case_manager, cases


def demonstrate_investigator_dashboard(
    spark, case_manager, investigator_id="inv_alice"
):
    """Demonstrate investigator dashboard functionality."""
    print("\\n" + "=" * 60)
    print("INVESTIGATOR DASHBOARD DEMONSTRATION")
    print("=" * 60)

    # Initialize dashboard
    dashboard = InvestigatorDashboard(
        spark=spark, case_manager=case_manager, investigator_id=investigator_id
    )

    print(f"\\n1. Case Queue for {investigator_id}:")

    case_queue = dashboard.get_case_queue(limit=10)

    for case_summary in case_queue:
        print(f"   Case ID: {case_summary['case_id']}")
        print(f"   Status: {case_summary['status']}")
        print(f"   Priority: {case_summary['priority']}")
        print(f"   Amount: ${case_summary['transaction_amount']:.2f}")
        print(f"   Fraud Score: {case_summary['fraud_score']:.2f}")
        print(f"   SLA Status: {case_summary['sla_status']}")
        print(f"   Hours Elapsed: {case_summary['hours_elapsed']}")
        print("")

    if case_queue:
        case_id = case_queue[0]["case_id"]

        print(f"\\n2. Detailed Case Information for {case_id}:")

        case_details = dashboard.get_case_details(case_id)

        if case_details:
            print("   Case Information:")
            case_info = case_details["case_info"]
            print(f"     Created: {case_info['created_at']}")
            print(f"     Status: {case_info['status']}")
            print(f"     Priority: {case_info['priority']}")
            print(f"     Assigned To: {case_info['assigned_to']}")

            print("   Transaction Information:")
            txn_info = case_details["transaction_info"]
            print(f"     Customer ID: {txn_info['customer_id']}")
            print(f"     Merchant ID: {txn_info['merchant_id']}")
            print(f"     Amount: ${txn_info['transaction_amount']:.2f}")
            print(f"     Fraud Score: {txn_info['fraud_score']:.2f}")
            print(f"     ML Prediction: {txn_info['ml_prediction']:.2f}")
            print(f"     Triggered Rules: {', '.join(txn_info['rule_triggers'])}")

        print(f"\\n3. Investigation Tools for {case_id}:")

        investigation_tools = dashboard.get_investigation_tools(case_id)

        if "transaction_analysis" in investigation_tools:
            print("   Transaction Pattern Analysis:")
            analysis = investigation_tools["transaction_analysis"]
            print(
                f"     Velocity Anomaly: {analysis['velocity_analysis']['velocity_anomaly']}"
            )
            print(
                f"     Amount Anomaly: {analysis['amount_analysis']['amount_anomaly']}"
            )
            print(
                f"     Timing Anomaly: {analysis['timing_analysis']['timing_anomaly']}"
            )
            print(
                f"     Location Anomaly: {analysis['location_analysis']['location_anomaly']}"
            )

        if "risk_assessment" in investigation_tools:
            print("   Risk Assessment:")
            risk = investigation_tools["risk_assessment"]
            print(f"     Overall Risk Score: {risk['overall_risk_score']:.2f}")
            print(f"     Risk Level: {risk['risk_level']}")
            print(f"     Risk Factors: {', '.join(risk['risk_factors'])}")
            print(f"     Recommendation: {risk['recommendation']}")

    print(f"\\n4. Workload Summary for {investigator_id}:")

    workload = dashboard.get_workload_summary()

    print(f"   Total Assigned Cases: {workload['total_assigned_cases']}")
    print(f"   Active Cases: {workload['active_cases']}")
    print(f"   Overdue Cases: {workload['overdue_cases']}")
    print(f"   Urgent Cases: {workload['urgent_cases']}")
    print(f"   Workload Status: {workload['workload_status']}")

    status_breakdown = workload["status_breakdown"]
    print("   Status Breakdown:")
    for status, count in status_breakdown.items():
        if count > 0:
            print(f"     {status}: {count}")

    return dashboard


def demonstrate_fraud_reporting(spark, case_manager):
    """Demonstrate fraud reporting functionality."""
    print("\\n" + "=" * 60)
    print("FRAUD REPORTING DEMONSTRATION")
    print("=" * 60)

    # Initialize reporting engine
    reporting_engine = FraudReportingEngine(spark, case_manager)

    print("\\n1. Executive Dashboard Report:")

    executive_dashboard = reporting_engine.generate_executive_dashboard(
        time_period_days=30
    )

    print("   Key Metrics:")
    key_metrics = executive_dashboard["key_metrics"]
    print(f"     Total Cases Investigated: {key_metrics['total_cases_investigated']}")
    print(f"     Cases Resolved: {key_metrics['cases_resolved']}")
    print(f"     Resolution Rate: {key_metrics['resolution_rate']:.1%}")
    print(f"     SLA Compliance Rate: {key_metrics['sla_compliance_rate']:.1%}")
    print(
        f"     Average Resolution Time: {key_metrics['average_resolution_time_hours']:.1f} hours"
    )
    print(f"     Overdue Cases: {key_metrics['overdue_cases']}")

    print("   Financial Impact:")
    financial = executive_dashboard["financial_impact"]
    print(
        f"     Total Investigated Amount: ${financial['total_investigated_amount']:,.2f}"
    )
    print(f"     Total Fraud Prevented: ${financial['total_fraud_prevented']:,.2f}")
    print(f"     False Positive Amount: ${financial['false_positive_amount']:,.2f}")
    print(f"     Fraud Prevention Rate: {financial['fraud_prevention_rate']:.1%}")
    print(f"     Estimated Savings: ${financial['estimated_savings']:,.2f}")

    print("\\n2. Investigator Performance Report:")

    performance_report = reporting_engine.generate_investigator_performance_report(
        time_period_days=30
    )

    print(f"   Team Summary:")
    team_summary = performance_report["team_summary"]
    print(f"     Team Size: {team_summary['team_size']}")
    print(f"     Total Team Cases: {team_summary['total_team_cases']}")
    print(f"     Team Resolution Rate: {team_summary['team_resolution_rate']:.1%}")
    print(f"     Team SLA Compliance: {team_summary['team_sla_compliance']:.1%}")
    if team_summary["top_performer"]:
        print(f"     Top Performer: {team_summary['top_performer']}")

    print("   Individual Performance:")
    for inv_report in performance_report["investigator_reports"][:3]:  # Show top 3
        print(f"     Investigator: {inv_report['investigator_id']}")
        metrics = inv_report["performance_metrics"]
        print(f"       Cases Handled: {metrics['total_cases']}")
        print(f"       Resolution Rate: {metrics['resolution_rate']:.1%}")
        print(
            f"       Avg Resolution Time: {metrics['average_resolution_time_hours']:.1f}h"
        )
        print(f"       Ranking: #{inv_report['performance_ranking']}")
        print("")

    print("\\n3. Fraud Trend Analysis:")

    trend_analysis = reporting_engine.generate_fraud_trend_analysis(time_period_days=90)

    print("   Pattern Analysis:")
    patterns = trend_analysis["pattern_analysis"]
    print("     Top Attack Vectors:")
    for vector in patterns["top_attack_vectors"][:3]:
        print(
            f"       {vector['vector']}: {vector['frequency']} cases ({vector['success_rate']:.1%} success rate)"
        )

    print("     Merchant Categories at Risk:")
    for category in patterns["merchant_category_analysis"]:
        print(
            f"       {category['category']}: {category['case_count']} cases ({category['fraud_rate']:.1%} fraud rate)"
        )

    print("   Seasonal Patterns:")
    seasonal = trend_analysis["seasonal_analysis"]
    print(
        f"     Peak Hours: {', '.join(map(str, seasonal['hourly_patterns']['peak_hours']))}"
    )
    print(f"     Peak Days: {', '.join(seasonal['weekly_patterns']['peak_days'])}")

    print("\\n4. Compliance Report:")

    compliance_report = reporting_engine.generate_compliance_report(time_period_days=30)

    print("   Compliance Metrics:")
    compliance = compliance_report["compliance_metrics"]
    print(
        f"     Documentation Completeness: {compliance['documentation_completeness_rate']:.1%}"
    )
    print(f"     Resolution Timeliness: {compliance['resolution_timeliness_rate']:.1%}")
    print(
        f"     Audit Trail Completeness: {compliance['audit_trail_completeness']:.1%}"
    )

    print("   Regulatory Compliance:")
    regulatory = compliance_report["regulatory_requirements"]
    for requirement, status in regulatory.items():
        print(f"     {requirement.upper()}: {status}")

    return reporting_engine


def demonstrate_feedback_system(spark, case_manager, cases):
    """Demonstrate false positive feedback system."""
    print("\\n" + "=" * 60)
    print("FALSE POSITIVE FEEDBACK SYSTEM DEMONSTRATION")
    print("=" * 60)

    # Initialize feedback system
    feedback_system = FalsePositiveFeedbackSystem(spark, case_manager)

    print("\\n1. Submitting Investigator Feedback:")

    # Submit feedback for resolved cases
    feedback_cases = [
        {
            "case": cases[1],  # The one resolved as legitimate
            "feedback_type": FeedbackType.FALSE_POSITIVE,
            "justification": "Customer provided valid purchase receipt and ID verification. Transaction confirmed as legitimate after thorough investigation. Customer has good history and explanation for unusual location.",
            "investigator": "inv_bob",
            "confidence": 0.92,
        },
        {
            "case": cases[0],  # The one resolved as fraud
            "feedback_type": FeedbackType.TRUE_POSITIVE,
            "justification": "Confirmed fraudulent transaction. Customer account was compromised and unauthorized purchases were made. Evidence includes failed authentication attempts and suspicious device fingerprints.",
            "investigator": "inv_alice",
            "confidence": 0.96,
        },
    ]

    for feedback_data in feedback_cases:
        success = feedback_system.submit_investigator_feedback(
            case_id=feedback_data["case"].case_id,
            feedback_type=feedback_data["feedback_type"],
            justification=feedback_data["justification"],
            investigator_id=feedback_data["investigator"],
            confidence=feedback_data["confidence"],
        )

        if success:
            print(
                f"   Submitted {feedback_data['feedback_type'].value} feedback for case {feedback_data['case'].case_id}"
            )

    print("\\n2. Submitting Customer Dispute:")

    # Simulate customer dispute
    dispute_success = feedback_system.submit_customer_dispute(
        transaction_id="txn_004",
        dispute_type="chargeback",
        dispute_reason="Customer claims transaction was unauthorized",
        resolution="customer_favor",
        customer_id="customer_99999",
    )

    if dispute_success:
        print("   Customer dispute submitted successfully")

    print("\\n3. False Positive Analysis:")

    fp_analysis = feedback_system.get_false_positive_analysis(time_period_days=30)

    print(f"   Total False Positives: {fp_analysis['total_false_positives']}")
    print(f"   False Positive Rate: {fp_analysis['false_positive_rate']:.1%}")

    if fp_analysis["amount_distribution"]:
        print("   Amount Distribution:")
        for amount_range, count in fp_analysis["amount_distribution"].items():
            print(f"     {amount_range}: {count} cases")

    if fp_analysis["common_reasons"]:
        print("   Common Reasons:")
        for reason, count in list(fp_analysis["common_reasons"].items())[:5]:
            print(f"     {reason}: {count} mentions")

    if fp_analysis["improvement_recommendations"]:
        print("   Improvement Recommendations:")
        for rec in fp_analysis["improvement_recommendations"]:
            print(f"     {rec['category']}: {rec['recommendation']}")

    print("\\n4. Feedback Quality Report:")

    quality_report = feedback_system.get_feedback_quality_report()

    print("   Feedback Metrics:")
    metrics = quality_report["feedback_metrics"]
    print(f"     Total Feedback Entries: {metrics['total_feedback']}")
    print(f"     Processing Rate: {metrics['processing_rate']:.1%}")
    print(f"     Average Confidence: {metrics['average_confidence']:.2f}")

    print("   Quality Indicators:")
    quality = quality_report["quality_indicators"]
    print(
        f"     High Confidence Feedback Rate: {quality['high_confidence_feedback_rate']:.1%}"
    )
    print(
        f"     Detailed Justification Rate: {quality['detailed_justification_rate']:.1%}"
    )

    print("   Distribution Analysis:")
    distribution = quality_report["distribution_analysis"]
    print("     By Feedback Type:")
    for feedback_type, count in distribution["feedback_type_distribution"].items():
        if count > 0:
            print(f"       {feedback_type}: {count}")

    print("     By Source:")
    for source, count in distribution["feedback_source_distribution"].items():
        if count > 0:
            print(f"       {source}: {count}")

    return feedback_system


def main():
    """Main demonstration function."""
    print("FRAUD INVESTIGATION TOOLS DEMONSTRATION")
    print("=" * 80)
    print("This example demonstrates the complete fraud investigation workflow")
    print(
        "including case management, investigation dashboard, reporting, and feedback."
    )
    print("=" * 80)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Demonstrate each component
        case_manager, cases = demonstrate_case_management(spark)
        dashboard = demonstrate_investigator_dashboard(spark, case_manager)
        reporting_engine = demonstrate_fraud_reporting(spark, case_manager)
        feedback_system = demonstrate_feedback_system(spark, case_manager, cases)

        print("\\n" + "=" * 60)
        print("DEMONSTRATION SUMMARY")
        print("=" * 60)
        print("\\nSuccessfully demonstrated:")
        print(
            "✓ Case Management System - Create, assign, investigate, and resolve fraud cases"
        )
        print(
            "✓ Investigator Dashboard - Case queues, investigation tools, and workload management"
        )
        print(
            "✓ Fraud Reporting Engine - Executive dashboards, performance reports, and trend analysis"
        )
        print(
            "✓ Feedback System - False positive feedback, quality analysis, and model improvement"
        )

        print("\\n" + "=" * 60)
        print("NEXT STEPS")
        print("=" * 60)
        print("\\n1. Integration with existing fraud detection pipeline")
        print("2. Connect to production data sources")
        print("3. Configure real-time alerting and notifications")
        print("4. Set up automated reporting schedules")
        print("5. Train investigators on the new tools")
        print("6. Establish SLA and performance benchmarks")

        print("\\nFor more information, see the documentation in:")
        print("- src/analytics/fraud_detection/case_management.py")
        print("- src/analytics/fraud_detection/investigator_dashboard.py")
        print("- src/analytics/fraud_detection/fraud_reporting.py")
        print("- src/analytics/fraud_detection/feedback_system.py")

    except Exception as e:
        print(f"\\nError during demonstration: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Clean up Spark session
        spark.stop()


if __name__ == "__main__":
    main()

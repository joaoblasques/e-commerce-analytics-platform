"""
Comprehensive Data Governance Demo.

Demonstrates all data governance capabilities including:
- Data catalog management
- Data lineage tracking
- Privacy and compliance management
- Data quality monitoring
- Access auditing and controls
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from src.data_governance import (
    AccessAuditor,
    DataCatalog,
    DataLineageTracker,
    DataQualityMonitor,
    PrivacyManager,
)
from src.data_governance.audit import AccessEventType
from src.data_governance.lineage import LineageEventType
from src.data_governance.privacy import ConsentType, DataSubjectRight
from src.data_governance.quality import AlertSeverity, QualityCheckType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_demo_environment():
    """Set up demo environment with sample data governance components."""
    base_path = Path("data/governance_demo")
    base_path.mkdir(parents=True, exist_ok=True)

    # Initialize all governance components
    catalog = DataCatalog(catalog_path=str(base_path / "catalog"))
    lineage_tracker = DataLineageTracker(lineage_path=str(base_path / "lineage"))
    privacy_manager = PrivacyManager(privacy_path=str(base_path / "privacy"))
    quality_monitor = DataQualityMonitor(quality_path=str(base_path / "quality"))
    access_auditor = AccessAuditor(audit_path=str(base_path / "audit"))

    return catalog, lineage_tracker, privacy_manager, quality_monitor, access_auditor


def demonstrate_data_catalog(catalog: DataCatalog):
    """Demonstrate data catalog functionality."""
    logger.info("🗂️ === Data Catalog Demo ===")

    # Register various data assets
    logger.info("📋 Registering data assets...")

    # Customer data asset
    customer_asset = catalog.register_asset(
        name="customer_data",
        path="/data/lake/customers",
        format="delta",
        description="Customer master data including demographics and preferences",
        owner="data_team",
        classification="confidential",
        tags=["customers", "pii", "gdpr"],
        retention_days=2555,  # 7 years
        auto_discover_schema=False,
    )
    logger.info(f"✅ Registered customer_data asset: {customer_asset.name}")

    # Transaction data asset
    transaction_asset = catalog.register_asset(
        name="transaction_data",
        path="/data/lake/transactions",
        format="delta",
        description="E-commerce transaction records with payment and shipping details",
        owner="analytics_team",
        classification="internal",
        tags=["transactions", "revenue", "analytics"],
        retention_days=2555,
        auto_discover_schema=False,
    )
    logger.info(f"✅ Registered transaction_data asset: {transaction_asset.name}")

    # Analytics results asset
    analytics_asset = catalog.register_asset(
        name="customer_analytics",
        path="/data/warehouse/customer_analytics",
        format="parquet",
        description="Customer analytics including RFM segmentation and CLV calculations",
        owner="ml_team",
        classification="internal",
        tags=["analytics", "ml", "insights"],
        retention_days=1095,  # 3 years
        auto_discover_schema=False,
    )
    logger.info(f"✅ Registered customer_analytics asset: {analytics_asset.name}")

    # List assets with different filters
    logger.info("🔍 Searching and filtering assets...")

    confidential_assets = catalog.list_assets(classification="confidential")
    logger.info(f"📊 Found {len(confidential_assets)} confidential assets")

    pii_assets = catalog.search_assets("pii")
    logger.info(f"🔒 Found {len(pii_assets)} assets containing PII")

    analytics_assets = catalog.list_assets(tags=["analytics"])
    logger.info(f"📈 Found {len(analytics_assets)} analytics assets")

    # Get catalog statistics
    stats = catalog.get_catalog_stats()
    logger.info("📊 Catalog Statistics:")
    logger.info(f"   Total assets: {stats['total_assets']}")
    logger.info(f"   By classification: {stats['by_classification']}")
    logger.info(f"   By format: {stats['by_format']}")

    return catalog


def demonstrate_data_lineage(lineage_tracker: DataLineageTracker):
    """Demonstrate data lineage tracking."""
    logger.info("🔗 === Data Lineage Demo ===")

    # Simulate a data pipeline with lineage tracking
    logger.info("📊 Tracking data pipeline lineage...")

    # Raw data ingestion
    lineage_tracker.track_event(
        event_type=LineageEventType.READ,
        source_assets=[],
        target_assets=["raw_customer_data"],
        transformation="Data ingestion from CRM system",
        job_id="ingestion_job_001",
        user="data_engineer",
        metadata={"source_system": "salesforce", "batch_id": "20250127_001"},
    )

    lineage_tracker.track_event(
        event_type=LineageEventType.READ,
        source_assets=[],
        target_assets=["raw_transaction_data"],
        transformation="Data ingestion from payment system",
        job_id="ingestion_job_002",
        user="data_engineer",
        metadata={"source_system": "stripe", "batch_id": "20250127_002"},
    )

    # Data cleaning and transformation
    lineage_tracker.track_event(
        event_type=LineageEventType.TRANSFORM,
        source_assets=["raw_customer_data"],
        target_assets=["clean_customer_data"],
        transformation="Data cleaning: remove duplicates, standardize formats",
        job_id="etl_job_001",
        user="data_engineer",
        metadata={"transformation_type": "cleaning", "records_processed": 50000},
    )

    lineage_tracker.track_event(
        event_type=LineageEventType.TRANSFORM,
        source_assets=["raw_transaction_data"],
        target_assets=["clean_transaction_data"],
        transformation="Data cleaning: validate amounts, standardize currencies",
        job_id="etl_job_002",
        user="data_engineer",
        metadata={"transformation_type": "cleaning", "records_processed": 150000},
    )

    # Feature engineering
    lineage_tracker.track_event(
        event_type=LineageEventType.TRANSFORM,
        source_assets=["clean_customer_data", "clean_transaction_data"],
        target_assets=["customer_features"],
        transformation="Feature engineering: RFM calculation, customer metrics",
        job_id="feature_job_001",
        user="ml_engineer",
        metadata={"feature_count": 25, "algorithm": "rfm_analysis"},
    )

    # Analytics and ML model training
    lineage_tracker.track_event(
        event_type=LineageEventType.TRANSFORM,
        source_assets=["customer_features"],
        target_assets=["churn_model"],
        transformation="ML model training: churn prediction model",
        job_id="ml_job_001",
        user="data_scientist",
        metadata={"model_type": "random_forest", "accuracy": 0.87},
    )

    lineage_tracker.track_event(
        event_type=LineageEventType.TRANSFORM,
        source_assets=["customer_features"],
        target_assets=["customer_segments"],
        transformation="Customer segmentation based on RFM scores",
        job_id="analytics_job_001",
        user="analyst",
        metadata={"segments_created": 11, "algorithm": "rfm_segmentation"},
    )

    # Analyze lineage
    logger.info("🔍 Analyzing data lineage...")

    # Get upstream lineage for customer segments
    upstream_lineage = lineage_tracker.get_upstream_lineage(
        "customer_segments", max_depth=10
    )
    logger.info(
        f"📈 Upstream lineage for customer_segments: {upstream_lineage['total_assets']} assets"
    )

    # Get downstream lineage for raw customer data
    downstream_lineage = lineage_tracker.get_downstream_lineage(
        "raw_customer_data", max_depth=10
    )
    logger.info(
        f"📉 Downstream lineage for raw_customer_data: {downstream_lineage['total_assets']} assets"
    )

    # Perform impact analysis
    impact_analysis = lineage_tracker.get_impact_analysis("clean_customer_data")
    logger.info("💥 Impact analysis for clean_customer_data:")
    logger.info(f"   Total impacted assets: {impact_analysis['total_impacted_assets']}")
    logger.info(f"   Impacted assets: {impact_analysis['impacted_assets']}")

    # Get lineage summary
    summary = lineage_tracker.get_lineage_summary()
    logger.info("📊 Lineage Summary:")
    logger.info(f"   Total events: {summary['total_events']}")
    logger.info(f"   Total nodes: {summary['total_nodes']}")
    logger.info(f"   Total edges: {summary['total_edges']}")
    logger.info(f"   Event types: {summary['event_types']}")

    return lineage_tracker


def demonstrate_privacy_management(privacy_manager: PrivacyManager):
    """Demonstrate privacy and compliance management."""
    logger.info("🔒 === Privacy Management Demo ===")

    # Record user consents
    logger.info("✅ Recording user consents...")

    privacy_manager.record_consent(
        user_id="user_001",
        consent_type=ConsentType.MARKETING,
        granted=True,
        source="website",
        version="1.2",
        metadata={"campaign": "newsletter_signup", "ip": "192.168.1.100"},
    )

    privacy_manager.record_consent(
        user_id="user_001",
        consent_type=ConsentType.ANALYTICS,
        granted=True,
        source="website",
        version="1.2",
        metadata={"tracking_type": "behavioral"},
    )

    privacy_manager.record_consent(
        user_id="user_002",
        consent_type=ConsentType.MARKETING,
        granted=False,
        source="mobile_app",
        version="1.2",
        metadata={"opt_out_reason": "privacy_concerns"},
    )

    # Check user consent status
    user_001_consent = privacy_manager.get_user_consent("user_001")
    logger.info(f"👤 User 001 consent status: {user_001_consent}")

    user_002_consent = privacy_manager.get_user_consent("user_002")
    logger.info(f"👤 User 002 consent status: {user_002_consent}")

    # Submit data subject requests
    logger.info("📝 Processing data subject requests...")

    # Right to access request
    access_request_id = privacy_manager.submit_data_subject_request(
        user_id="user_001",
        request_type=DataSubjectRight.ACCESS,
        requested_by="user_001",
        details="I would like to see all personal data you have about me",
        metadata={"verification_method": "email", "contact": "user001@example.com"},
    )
    logger.info(f"📋 Submitted access request: {access_request_id}")

    # Right to be forgotten request
    erasure_request_id = privacy_manager.submit_data_subject_request(
        user_id="user_002",
        request_type=DataSubjectRight.ERASURE,
        requested_by="user_002",
        details="Please delete all my personal data due to privacy concerns",
        metadata={"verification_method": "phone", "urgency": "high"},
    )
    logger.info(f"🗑️ Submitted erasure request: {erasure_request_id}")

    # Data portability request
    portability_request_id = privacy_manager.submit_data_subject_request(
        user_id="user_001",
        request_type=DataSubjectRight.PORTABILITY,
        requested_by="user_001",
        details="Please provide my data in a machine-readable format",
        metadata={"preferred_format": "json", "delivery_method": "email"},
    )
    logger.info(f"📦 Submitted portability request: {portability_request_id}")

    # Generate privacy compliance report
    privacy_report = privacy_manager.generate_privacy_report()
    logger.info("📊 Privacy Compliance Report:")
    logger.info(
        f"   Consent records: {privacy_report['recent_activity']['consent_records_last_30_days']}"
    )
    logger.info(
        f"   Subject requests: {privacy_report['recent_activity']['subject_requests_last_30_days']}"
    )
    logger.info(f"   GDPR enabled: {privacy_report['configuration']['gdpr_enabled']}")
    logger.info(f"   CCPA enabled: {privacy_report['configuration']['ccpa_enabled']}")
    logger.info(
        f"   Pending requests: {privacy_report['compliance_status']['pending_requests']}"
    )

    return privacy_manager


def demonstrate_quality_monitoring(quality_monitor: DataQualityMonitor):
    """Demonstrate data quality monitoring."""
    logger.info("📊 === Data Quality Monitoring Demo ===")

    # Create quality rules
    logger.info("📋 Creating data quality rules...")

    # Completeness rule for customer email
    email_completeness_rule = quality_monitor.create_rule(
        name="Customer Email Completeness",
        description="Ensure customer email field is complete",
        check_type=QualityCheckType.COMPLETENESS,
        dataset="customer_data",
        column="email",
        threshold=0.95,
        severity=AlertSeverity.WARNING,
        created_by="data_engineer",
    )
    logger.info(f"✅ Created email completeness rule: {email_completeness_rule}")

    # Uniqueness rule for customer ID
    id_uniqueness_rule = quality_monitor.create_rule(
        name="Customer ID Uniqueness",
        description="Ensure customer IDs are unique",
        check_type=QualityCheckType.UNIQUENESS,
        dataset="customer_data",
        column="customer_id",
        threshold=1.0,
        severity=AlertSeverity.ERROR,
        created_by="data_engineer",
    )
    logger.info(f"✅ Created ID uniqueness rule: {id_uniqueness_rule}")

    # Validity rule for email format
    email_validity_rule = quality_monitor.create_rule(
        name="Email Format Validation",
        description="Validate email format using regex",
        check_type=QualityCheckType.VALIDITY,
        dataset="customer_data",
        column="email",
        rule_definition={
            "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        },
        threshold=0.98,
        severity=AlertSeverity.WARNING,
        created_by="data_engineer",
    )
    logger.info(f"✅ Created email validity rule: {email_validity_rule}")

    # Timeliness rule for transaction data
    timeliness_rule = quality_monitor.create_rule(
        name="Transaction Data Freshness",
        description="Ensure transaction data is recent",
        check_type=QualityCheckType.TIMELINESS,
        dataset="transaction_data",
        column="created_at",
        rule_definition={"max_age_hours": 24},
        threshold=0.90,
        severity=AlertSeverity.ERROR,
        created_by="data_engineer",
    )
    logger.info(f"✅ Created timeliness rule: {timeliness_rule}")

    # Simulate quality check results (since we don't have actual Spark data)
    logger.info("🔍 Simulating quality check results...")

    import uuid

    from src.data_governance.quality import QualityResult

    # Simulate some passing and failing quality checks
    now = datetime.now()

    # Good completeness result
    good_result = QualityResult(
        check_id=str(uuid.uuid4()),
        rule_id=email_completeness_rule,
        dataset="customer_data",
        column="email",
        check_type=QualityCheckType.COMPLETENESS,
        timestamp=now,
        score=0.97,
        passed=True,
        threshold=0.95,
        actual_value=0.97,
        total_records=10000,
        failed_records=300,
        details={"completeness_score": 0.97},
    )
    quality_monitor.results.append(good_result)

    # Poor uniqueness result (triggers alert)
    poor_result = QualityResult(
        check_id=str(uuid.uuid4()),
        rule_id=id_uniqueness_rule,
        dataset="customer_data",
        column="customer_id",
        check_type=QualityCheckType.UNIQUENESS,
        timestamp=now,
        score=0.94,
        passed=False,
        threshold=1.0,
        actual_value=0.94,
        total_records=10000,
        failed_records=600,
        details={"distinct_count": 9400, "duplicate_count": 600},
    )
    quality_monitor.results.append(poor_result)

    # Generate alert for failed check
    quality_monitor._generate_alert(
        quality_monitor.rules[id_uniqueness_rule], poor_result
    )

    # Average validity result
    average_result = QualityResult(
        check_id=str(uuid.uuid4()),
        rule_id=email_validity_rule,
        dataset="customer_data",
        column="email",
        check_type=QualityCheckType.VALIDITY,
        timestamp=now,
        score=0.99,
        passed=True,
        threshold=0.98,
        actual_value=0.99,
        total_records=10000,
        failed_records=100,
        details={"valid_count": 9900, "pattern": "email_regex"},
    )
    quality_monitor.results.append(average_result)

    # Get quality dashboard
    dashboard = quality_monitor.get_quality_dashboard()
    logger.info("📊 Quality Dashboard:")
    logger.info(f"   Overall quality score: {dashboard['overall_quality_score']:.3f}")
    logger.info(f"   Total checks: {dashboard['total_checks']}")
    logger.info(f"   Passed checks: {dashboard['passed_checks']}")
    logger.info(f"   Failed checks: {dashboard['failed_checks']}")
    logger.info(f"   Recent alerts: {len(dashboard['recent_alerts'])}")

    # Generate quality report
    quality_report = quality_monitor.get_quality_report(days=30)
    logger.info("📋 Quality Report (30 days):")
    logger.info(f"   Success rate: {quality_report['summary']['success_rate']:.3f}")
    logger.info(f"   Average score: {quality_report['summary']['average_score']:.3f}")
    logger.info(f"   Active rules: {quality_report['rules_active']}")

    return quality_monitor


def demonstrate_access_auditing(access_auditor: AccessAuditor):
    """Demonstrate access auditing and controls."""
    logger.info("🔐 === Access Auditing Demo ===")

    # Grant permissions to users
    logger.info("🔑 Granting user permissions...")

    # Grant read permissions to analyst
    access_auditor.grant_permission(
        user_id="analyst_001",
        resource="customer_data",
        resource_type="table",
        permissions={"read"},
        granted_by="admin",
        expires_at=datetime.now() + timedelta(days=90),
        conditions={"time_range": "9-17"},  # Business hours only
    )

    # Grant broader permissions to admin
    access_auditor.grant_permission(
        user_id="admin_001",
        resource="customer_data",
        resource_type="table",
        permissions={"read", "write", "delete"},
        granted_by="super_admin",
        expires_at=datetime.now() + timedelta(days=365),
    )

    # Grant limited permissions to viewer
    access_auditor.grant_permission(
        user_id="viewer_001",
        resource="analytics_data",
        resource_type="table",
        permissions={"read"},
        granted_by="admin",
        expires_at=datetime.now() + timedelta(days=30),
    )

    # Simulate user access events
    logger.info("📊 Logging access events...")

    # Normal successful access
    access_auditor.log_access_event(
        event_type=AccessEventType.LOGIN,
        user_id="analyst_001",
        user_role="analyst",
        resource="system",
        resource_type="auth",
        action="login",
        source_ip="192.168.1.100",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        session_id="session_001",
        success=True,
        details={"login_method": "sso", "department": "analytics"},
    )

    access_auditor.log_access_event(
        event_type=AccessEventType.DATA_ACCESS,
        user_id="analyst_001",
        user_role="analyst",
        resource="customer_data",
        resource_type="table",
        action="read",
        source_ip="192.168.1.100",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        session_id="session_001",
        success=True,
        details={"query": "SELECT COUNT(*) FROM customers", "rows_returned": 1},
    )

    # Failed access attempt (higher risk)
    access_auditor.log_access_event(
        event_type=AccessEventType.FAILED_ACCESS,
        user_id="analyst_001",
        user_role="analyst",
        resource="admin_panel",
        resource_type="application",
        action="access",
        source_ip="203.0.113.50",  # External IP
        user_agent="curl/7.68.0",
        session_id="session_002",
        success=False,
        details={"error": "insufficient_permissions", "attempted_escalation": True},
    )

    # Suspicious data export
    access_auditor.log_access_event(
        event_type=AccessEventType.DATA_EXPORT,
        user_id="analyst_001",
        user_role="analyst",
        resource="customer_data",
        resource_type="table",
        action="export",
        source_ip="192.168.1.100",
        user_agent="Python/requests",
        session_id="session_001",
        success=True,
        details={"export_format": "csv", "size_mb": 150, "rows_exported": 50000},
    )

    # Admin activities
    access_auditor.log_access_event(
        event_type=AccessEventType.PERMISSION_CHANGE,
        user_id="admin_001",
        user_role="admin",
        resource="user_management",
        resource_type="system",
        action="grant_permission",
        source_ip="192.168.1.10",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        session_id="admin_session_001",
        success=True,
        details={"target_user": "new_analyst", "permissions_granted": ["read"]},
    )

    # Multiple failed login attempts (should trigger alert)
    for i in range(6):
        access_auditor.log_access_event(
            event_type=AccessEventType.FAILED_ACCESS,
            user_id="suspicious_user",
            user_role="unknown",
            resource="system",
            resource_type="auth",
            action="login",
            source_ip="203.0.113.100",
            user_agent="AttackTool/1.0",
            session_id=f"failed_session_{i}",
            success=False,
            details={"error": "invalid_credentials", "attempt": i + 1},
        )

    # Check permissions
    logger.info("🔍 Checking user permissions...")

    # Valid permission check
    has_read_permission = access_auditor.check_permission(
        user_id="analyst_001", resource="customer_data", action="read"
    )
    logger.info(
        f"👤 Analyst_001 read permission for customer_data: {has_read_permission}"
    )

    # Invalid permission check
    has_write_permission = access_auditor.check_permission(
        user_id="analyst_001", resource="customer_data", action="write"
    )
    logger.info(
        f"👤 Analyst_001 write permission for customer_data: {has_write_permission}"
    )

    # Generate user access report
    user_report = access_auditor.get_user_access_report("analyst_001", days=30)
    logger.info("📋 User Access Report for analyst_001:")
    logger.info(
        f"   Total accesses: {user_report['access_statistics']['total_accesses']}"
    )
    logger.info(
        f"   Success rate: {user_report['access_statistics']['success_rate']:.3f}"
    )
    logger.info(f"   Resources accessed: {len(user_report['resources_accessed'])}")
    logger.info(
        f"   High risk events: {user_report['risk_analysis']['high_risk_events']}"
    )
    logger.info(f"   Security alerts: {len(user_report['security_alerts'])}")

    # Generate compliance report
    compliance_report = access_auditor.get_compliance_report(days=30)
    logger.info("📊 Compliance Report:")
    logger.info(f"   Total events: {compliance_report['overview']['total_events']}")
    logger.info(f"   Unique users: {compliance_report['overview']['unique_users']}")
    logger.info(
        f"   Security alerts: {compliance_report['overview']['security_alerts']}"
    )
    logger.info(
        f"   Failed accesses: {compliance_report['overview']['failed_accesses']}"
    )

    return access_auditor


def demonstrate_integrated_governance_workflow():
    """Demonstrate integrated data governance workflow."""
    logger.info("🔄 === Integrated Governance Workflow Demo ===")

    # Set up all governance components
    (
        catalog,
        lineage_tracker,
        privacy_manager,
        quality_monitor,
        access_auditor,
    ) = setup_demo_environment()

    logger.info("🎯 Simulating end-to-end data governance scenario...")

    # 1. Data Engineer registers new dataset
    logger.info("1️⃣ Data Engineer registers customer dataset...")
    catalog.register_asset(
        name="new_customer_dataset",
        path="/data/lake/new_customers",
        format="delta",
        description="New customer onboarding data from registration system",
        owner="data_engineering_team",
        classification="confidential",
        tags=["customers", "pii", "onboarding"],
        auto_discover_schema=False,
    )

    # 2. Track lineage for data processing
    logger.info("2️⃣ Tracking data lineage for ETL pipeline...")
    lineage_tracker.track_event(
        event_type=LineageEventType.TRANSFORM,
        source_assets=["raw_registration_data"],
        target_assets=["new_customer_dataset"],
        transformation="Customer data standardization and enrichment",
        job_id="customer_etl_20250127",
        user="data_engineer",
        metadata={"pipeline": "customer_onboarding", "records_processed": 5000},
    )

    # 3. Set up quality monitoring
    logger.info("3️⃣ Setting up quality rules for new dataset...")
    quality_monitor.create_rule(
        name="New Customer Email Quality",
        description="Monitor email quality in new customer dataset",
        check_type=QualityCheckType.COMPLETENESS,
        dataset="new_customer_dataset",
        column="email",
        threshold=0.98,
        severity=AlertSeverity.ERROR,
    )

    # 4. Configure privacy controls
    logger.info("4️⃣ Recording customer consent and privacy preferences...")
    for i in range(10):
        user_id = f"new_customer_{i:03d}"

        # Record various consent preferences
        privacy_manager.record_consent(
            user_id=user_id,
            consent_type=ConsentType.NECESSARY,
            granted=True,
            source="registration_form",
        )

        # Some customers consent to marketing
        if i % 3 == 0:
            privacy_manager.record_consent(
                user_id=user_id,
                consent_type=ConsentType.MARKETING,
                granted=True,
                source="registration_form",
            )

    # 5. Log access to the new dataset
    logger.info("5️⃣ Logging data access for compliance...")
    access_auditor.log_access_event(
        event_type=AccessEventType.DATA_ACCESS,
        user_id="analyst_002",
        user_role="analyst",
        resource="new_customer_dataset",
        resource_type="table",
        action="read",
        source_ip="192.168.1.150",
        user_agent="Jupyter/6.4.8",
        session_id="analytics_session_001",
        success=True,
        details={"purpose": "customer_analysis", "query_type": "aggregation"},
    )

    # 6. Generate comprehensive governance report
    logger.info("6️⃣ Generating comprehensive governance report...")

    governance_report = {
        "timestamp": datetime.now().isoformat(),
        "catalog_stats": catalog.get_catalog_stats(),
        "lineage_summary": lineage_tracker.get_lineage_summary(),
        "privacy_report": privacy_manager.generate_privacy_report(),
        "quality_dashboard": quality_monitor.get_quality_dashboard(),
        "compliance_report": access_auditor.get_compliance_report(),
    }

    logger.info("📊 Integrated Governance Report Generated:")
    logger.info(
        f"   Assets cataloged: {governance_report['catalog_stats']['total_assets']}"
    )
    logger.info(
        f"   Lineage events: {governance_report['lineage_summary']['total_events']}"
    )
    logger.info(
        f"   Consent records: {governance_report['privacy_report']['recent_activity']['consent_records_last_30_days']}"
    )
    logger.info(
        f"   Quality checks: {governance_report['quality_dashboard'].get('total_checks', 0)}"
    )
    logger.info(
        f"   Access events: {governance_report['compliance_report']['overview']['total_events']}"
    )

    return governance_report


def main():
    """Run the comprehensive data governance demo."""
    logger.info("🚀 Starting Comprehensive Data Governance Demo")
    logger.info("=" * 60)

    try:
        # Set up demo environment
        (
            catalog,
            lineage_tracker,
            privacy_manager,
            quality_monitor,
            access_auditor,
        ) = setup_demo_environment()

        # Run individual component demos
        demonstrate_data_catalog(catalog)
        print("\n" + "=" * 60 + "\n")

        demonstrate_data_lineage(lineage_tracker)
        print("\n" + "=" * 60 + "\n")

        demonstrate_privacy_management(privacy_manager)
        print("\n" + "=" * 60 + "\n")

        demonstrate_quality_monitoring(quality_monitor)
        print("\n" + "=" * 60 + "\n")

        demonstrate_access_auditing(access_auditor)
        print("\n" + "=" * 60 + "\n")

        # Run integrated workflow demo
        governance_report = demonstrate_integrated_governance_workflow()

        logger.info("✅ Data Governance Demo Completed Successfully!")
        logger.info("📁 Demo data saved to: data/governance_demo/")

        # Export reports
        import json

        report_path = Path("data/governance_demo/integrated_report.json")
        with open(report_path, "w") as f:
            json.dump(governance_report, f, indent=2, default=str)

        logger.info(f"📋 Integrated governance report exported to: {report_path}")

    except Exception as e:
        logger.error(f"❌ Demo failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    main()

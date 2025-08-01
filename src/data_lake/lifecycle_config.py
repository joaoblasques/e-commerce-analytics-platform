"""
Data Lifecycle Configuration Module

Provides predefined configurations for data lifecycle management,
including retention policies, archiving strategies, and cost optimization
settings for e-commerce analytics data.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from .lifecycle_manager import ArchivePolicy, DataTier, RetentionRule

# Predefined retention rules for e-commerce data tables
DEFAULT_RETENTION_RULES = {
    "transactions": RetentionRule(
        table_name="transactions",
        hot_days=30,  # Keep recent transactions hot for reporting
        warm_days=90,  # Quarterly analysis needs
        cold_days=365,  # Annual analysis and compliance
        archive_after_days=2190,  # Archive after 6 years
        delete_after_days=2555,  # Delete after 7 years (legal requirement)
        partition_column="transaction_date",
        enabled=True,
    ),
    "user_events": RetentionRule(
        table_name="user_events",
        hot_days=7,  # Recent user behavior for real-time analytics
        warm_days=30,  # Monthly user analysis
        cold_days=90,  # Quarterly behavior analysis
        archive_after_days=365,  # Archive after 1 year
        delete_after_days=1095,  # Delete after 3 years
        partition_column="event_timestamp",
        enabled=True,
    ),
    "customer_profiles": RetentionRule(
        table_name="customer_profiles",
        hot_days=90,  # Keep customer data hot for personalization
        warm_days=365,  # Annual customer lifecycle analysis
        cold_days=1095,  # Keep for 3 years for trend analysis
        archive_after_days=None,  # Don't auto-archive customer data
        delete_after_days=None,  # Manual deletion only (GDPR compliance)
        partition_column="last_updated",
        enabled=True,
    ),
    "product_catalog": RetentionRule(
        table_name="product_catalog",
        hot_days=30,  # Current catalog needs to be fast
        warm_days=90,  # Historical product data
        cold_days=365,  # Annual product lifecycle analysis
        archive_after_days=1095,  # Archive after 3 years
        delete_after_days=None,  # Keep product history
        partition_column="created_at",
        enabled=True,
    ),
    "marketing_campaigns": RetentionRule(
        table_name="marketing_campaigns",
        hot_days=30,  # Active campaign analysis
        warm_days=90,  # Campaign performance analysis
        cold_days=365,  # Annual marketing effectiveness
        archive_after_days=1095,  # Archive after 3 years
        delete_after_days=2190,  # Delete after 6 years
        partition_column="campaign_start_date",
        enabled=True,
    ),
    "fraud_detection_logs": RetentionRule(
        table_name="fraud_detection_logs",
        hot_days=30,  # Recent fraud analysis
        warm_days=90,  # Quarterly fraud review
        cold_days=365,  # Annual fraud analysis
        archive_after_days=1095,  # Archive after 3 years
        delete_after_days=2555,  # Keep for 7 years (compliance)
        partition_column="detection_timestamp",
        enabled=True,
    ),
    "inventory_snapshots": RetentionRule(
        table_name="inventory_snapshots",
        hot_days=7,  # Recent inventory for operations
        warm_days=30,  # Monthly inventory analysis
        cold_days=90,  # Quarterly inventory trends
        archive_after_days=365,  # Archive after 1 year
        delete_after_days=1095,  # Delete after 3 years
        partition_column="snapshot_date",
        enabled=True,
    ),
    "web_analytics": RetentionRule(
        table_name="web_analytics",
        hot_days=7,  # Recent web data for real-time dashboards
        warm_days=30,  # Monthly web analytics
        cold_days=365,  # Annual web performance analysis
        archive_after_days=1095,  # Archive after 3 years
        delete_after_days=2190,  # Delete after 6 years
        partition_column="page_view_timestamp",
        enabled=True,
    ),
    "recommendation_logs": RetentionRule(
        table_name="recommendation_logs",
        hot_days=14,  # Recent recommendations for model training
        warm_days=30,  # Monthly recommendation analysis
        cold_days=90,  # Quarterly model performance
        archive_after_days=365,  # Archive after 1 year
        delete_after_days=1095,  # Delete after 3 years
        partition_column="recommendation_timestamp",
        enabled=True,
    ),
    "system_logs": RetentionRule(
        table_name="system_logs",
        hot_days=3,  # Recent logs for debugging
        warm_days=7,  # Weekly system analysis
        cold_days=30,  # Monthly system review
        archive_after_days=90,  # Archive after 3 months
        delete_after_days=365,  # Delete after 1 year
        partition_column="log_timestamp",
        enabled=True,
    ),
}


# Predefined archive policies
DEFAULT_ARCHIVE_POLICIES = {
    "transactions": ArchivePolicy(
        table_name="transactions",
        archive_path="s3a://data-lake-archive/transactions",
        compression="gzip",
        format="parquet",
        partition_by=["year", "month"],
        enabled=True,
    ),
    "user_events": ArchivePolicy(
        table_name="user_events",
        archive_path="s3a://data-lake-archive/user_events",
        compression="gzip",
        format="parquet",
        partition_by=["event_date"],
        enabled=True,
    ),
    "marketing_campaigns": ArchivePolicy(
        table_name="marketing_campaigns",
        archive_path="s3a://data-lake-archive/marketing",
        compression="gzip",
        format="parquet",
        partition_by=["campaign_year"],
        enabled=True,
    ),
    "fraud_detection_logs": ArchivePolicy(
        table_name="fraud_detection_logs",
        archive_path="s3a://data-lake-archive/fraud_logs",
        compression="gzip",
        format="parquet",
        partition_by=["detection_year", "detection_month"],
        enabled=True,
    ),
    "inventory_snapshots": ArchivePolicy(
        table_name="inventory_snapshots",
        archive_path="s3a://data-lake-archive/inventory",
        compression="gzip",
        format="parquet",
        partition_by=["snapshot_year"],
        enabled=True,
    ),
    "web_analytics": ArchivePolicy(
        table_name="web_analytics",
        archive_path="s3a://data-lake-archive/web_analytics",
        compression="gzip",
        format="parquet",
        partition_by=["year", "month"],
        enabled=True,
    ),
    "recommendation_logs": ArchivePolicy(
        table_name="recommendation_logs",
        archive_path="s3a://data-lake-archive/recommendations",
        compression="gzip",
        format="parquet",
        partition_by=["recommendation_date"],
        enabled=True,
    ),
    "system_logs": ArchivePolicy(
        table_name="system_logs",
        archive_path="s3a://data-lake-archive/system_logs",
        compression="gzip",
        format="parquet",
        partition_by=["log_date"],
        enabled=True,
    ),
}


# Cost optimization configurations
STORAGE_TIER_CONFIGURATIONS = {
    "cost_optimized": {
        DataTier.STANDARD: {
            "max_age_days": 7,
            "cost_per_gb_month": 0.023,
            "access_cost_per_request": 0.0004,
        },
        DataTier.INFREQUENT: {
            "max_age_days": 30,
            "cost_per_gb_month": 0.0125,
            "access_cost_per_request": 0.001,
        },
        DataTier.ARCHIVE: {
            "max_age_days": 90,
            "cost_per_gb_month": 0.004,
            "access_cost_per_request": 0.05,
            "retrieval_time_hours": 3,
        },
        DataTier.DEEP_ARCHIVE: {
            "max_age_days": None,
            "cost_per_gb_month": 0.00099,
            "access_cost_per_request": 0.05,
            "retrieval_time_hours": 12,
        },
    },
    "performance_optimized": {
        DataTier.STANDARD: {
            "max_age_days": 30,
            "cost_per_gb_month": 0.023,
            "access_cost_per_request": 0.0004,
        },
        DataTier.INFREQUENT: {
            "max_age_days": 90,
            "cost_per_gb_month": 0.0125,
            "access_cost_per_request": 0.001,
        },
        DataTier.ARCHIVE: {
            "max_age_days": 365,
            "cost_per_gb_month": 0.004,
            "access_cost_per_request": 0.05,
            "retrieval_time_hours": 3,
        },
        DataTier.DEEP_ARCHIVE: {
            "max_age_days": None,
            "cost_per_gb_month": 0.00099,
            "access_cost_per_request": 0.05,
            "retrieval_time_hours": 12,
        },
    },
    "compliance_optimized": {
        # Longer retention periods for compliance requirements
        DataTier.STANDARD: {
            "max_age_days": 90,
            "cost_per_gb_month": 0.023,
            "access_cost_per_request": 0.0004,
        },
        DataTier.INFREQUENT: {
            "max_age_days": 365,
            "cost_per_gb_month": 0.0125,
            "access_cost_per_request": 0.001,
        },
        DataTier.ARCHIVE: {
            "max_age_days": 2555,  # 7 years
            "cost_per_gb_month": 0.004,
            "access_cost_per_request": 0.05,
            "retrieval_time_hours": 3,
        },
        DataTier.DEEP_ARCHIVE: {
            "max_age_days": None,
            "cost_per_gb_month": 0.00099,
            "access_cost_per_request": 0.05,
            "retrieval_time_hours": 12,
        },
    },
}


# Lifecycle automation schedules
AUTOMATION_SCHEDULES = {
    "retention_policy_check": {
        "cron": "0 2 * * *",  # Daily at 2 AM
        "description": "Apply retention policies to all tables",
    },
    "storage_optimization": {
        "cron": "0 3 * * 1",  # Weekly on Monday at 3 AM
        "description": "Analyze and optimize storage tiers",
    },
    "cost_analysis": {
        "cron": "0 4 1 * *",  # Monthly on 1st at 4 AM
        "description": "Generate monthly cost analysis report",
    },
    "compliance_audit": {
        "cron": "0 5 1 1,4,7,10 *",  # Quarterly on 1st at 5 AM
        "description": "Run compliance audit and generate reports",
    },
    "lineage_cleanup": {
        "cron": "0 1 1 * *",  # Monthly on 1st at 1 AM
        "description": "Clean up old lineage records",
    },
}


class LifecycleConfigManager:
    """Manages lifecycle configuration for the data lake."""

    def __init__(self):
        self.retention_rules = DEFAULT_RETENTION_RULES.copy()
        self.archive_policies = DEFAULT_ARCHIVE_POLICIES.copy()
        self.tier_configurations = STORAGE_TIER_CONFIGURATIONS.copy()
        self.automation_schedules = AUTOMATION_SCHEDULES.copy()

    def get_retention_rule(self, table_name: str) -> RetentionRule:
        """Get retention rule for a specific table."""
        if table_name in self.retention_rules:
            return self.retention_rules[table_name]

        # Return default rule for unknown tables
        return RetentionRule(
            table_name=table_name,
            hot_days=30,
            warm_days=90,
            cold_days=365,
            archive_after_days=1095,
            delete_after_days=None,
            partition_column="created_at",
            enabled=False,  # Disabled by default for safety
        )

    def get_archive_policy(self, table_name: str) -> ArchivePolicy:
        """Get archive policy for a specific table."""
        if table_name in self.archive_policies:
            return self.archive_policies[table_name]

        # Return default policy for unknown tables
        return ArchivePolicy(
            table_name=table_name,
            archive_path=f"s3a://data-lake-archive/{table_name}",
            compression="gzip",
            format="parquet",
            partition_by=None,
            enabled=False,  # Disabled by default for safety
        )

    def get_tier_configuration(
        self, optimization_strategy: str = "cost_optimized"
    ) -> Dict:
        """Get storage tier configuration for a specific strategy."""
        return self.tier_configurations.get(
            optimization_strategy, self.tier_configurations["cost_optimized"]
        )

    def update_retention_rule(self, table_name: str, rule: RetentionRule) -> None:
        """Update retention rule for a table."""
        self.retention_rules[table_name] = rule

    def update_archive_policy(self, table_name: str, policy: ArchivePolicy) -> None:
        """Update archive policy for a table."""
        self.archive_policies[table_name] = policy

    def export_configuration(self, file_path: str) -> None:
        """Export current configuration to a JSON file."""
        config = {
            "retention_rules": {
                name: {
                    "table_name": rule.table_name,
                    "hot_days": rule.hot_days,
                    "warm_days": rule.warm_days,
                    "cold_days": rule.cold_days,
                    "archive_after_days": rule.archive_after_days,
                    "delete_after_days": rule.delete_after_days,
                    "partition_column": rule.partition_column,
                    "enabled": rule.enabled,
                }
                for name, rule in self.retention_rules.items()
            },
            "archive_policies": {
                name: {
                    "table_name": policy.table_name,
                    "archive_path": policy.archive_path,
                    "compression": policy.compression,
                    "format": policy.format,
                    "partition_by": policy.partition_by,
                    "enabled": policy.enabled,
                }
                for name, policy in self.archive_policies.items()
            },
            "tier_configurations": self.tier_configurations,
            "automation_schedules": self.automation_schedules,
            "export_timestamp": datetime.now().isoformat(),
        }

        with open(file_path, "w") as f:
            json.dump(config, f, indent=2, default=str)

    def import_configuration(self, file_path: str) -> None:
        """Import configuration from a JSON file."""
        with open(file_path, "r") as f:
            config = json.load(f)

        # Import retention rules
        for name, rule_config in config.get("retention_rules", {}).items():
            rule = RetentionRule(**rule_config)
            self.retention_rules[name] = rule

        # Import archive policies
        for name, policy_config in config.get("archive_policies", {}).items():
            policy = ArchivePolicy(**policy_config)
            self.archive_policies[name] = policy

        # Import tier configurations
        if "tier_configurations" in config:
            self.tier_configurations.update(config["tier_configurations"])

        # Import automation schedules
        if "automation_schedules" in config:
            self.automation_schedules.update(config["automation_schedules"])

    def validate_configuration(self) -> List[str]:
        """Validate current configuration and return list of issues."""
        issues = []

        # Validate retention rules
        for name, rule in self.retention_rules.items():
            if rule.hot_days >= rule.warm_days:
                issues.append(f"Table {name}: hot_days should be less than warm_days")

            if rule.warm_days >= rule.cold_days:
                issues.append(f"Table {name}: warm_days should be less than cold_days")

            if rule.archive_after_days and rule.archive_after_days <= rule.cold_days:
                issues.append(
                    f"Table {name}: archive_after_days should be greater than cold_days"
                )

            if (
                rule.delete_after_days
                and rule.archive_after_days
                and rule.delete_after_days <= rule.archive_after_days
            ):
                issues.append(
                    f"Table {name}: delete_after_days should be greater than archive_after_days"
                )

        # Validate archive policies
        for name, policy in self.archive_policies.items():
            if not policy.archive_path.startswith(("s3a://", "s3://", "/", "hdfs://")):
                issues.append(
                    f"Table {name}: archive_path should be a valid storage path"
                )

            if policy.format not in ["parquet", "orc", "delta", "json", "csv"]:
                issues.append(f"Table {name}: unsupported format {policy.format}")

            if policy.compression not in ["gzip", "snappy", "lz4", "zstd", None]:
                issues.append(
                    f"Table {name}: unsupported compression {policy.compression}"
                )

        return issues

    def get_table_lifecycle_summary(self, table_name: str) -> Dict[str, any]:
        """Get complete lifecycle configuration summary for a table."""
        retention_rule = self.get_retention_rule(table_name)
        archive_policy = self.get_archive_policy(table_name)

        return {
            "table_name": table_name,
            "retention": {
                "hot_days": retention_rule.hot_days,
                "warm_days": retention_rule.warm_days,
                "cold_days": retention_rule.cold_days,
                "archive_after_days": retention_rule.archive_after_days,
                "delete_after_days": retention_rule.delete_after_days,
                "enabled": retention_rule.enabled,
            },
            "archiving": {
                "archive_path": archive_policy.archive_path,
                "format": archive_policy.format,
                "compression": archive_policy.compression,
                "partition_by": archive_policy.partition_by,
                "enabled": archive_policy.enabled,
            },
            "estimated_lifecycle": self._estimate_data_lifecycle(retention_rule),
        }

    def _estimate_data_lifecycle(self, rule: RetentionRule) -> Dict[str, str]:
        """Estimate data lifecycle stages for a retention rule."""
        lifecycle = {
            "hot_period": f"{rule.hot_days} days",
            "warm_period": f"{rule.warm_days - rule.hot_days} days",
            "cold_period": f"{rule.cold_days - rule.warm_days} days",
        }

        if rule.archive_after_days:
            lifecycle[
                "archive_period"
            ] = f"{rule.archive_after_days - rule.cold_days} days"

        if rule.delete_after_days:
            if rule.archive_after_days:
                lifecycle[
                    "archived_storage"
                ] = f"{rule.delete_after_days - rule.archive_after_days} days"
            lifecycle["total_lifetime"] = f"{rule.delete_after_days} days"
        else:
            lifecycle["total_lifetime"] = "Indefinite (manual deletion only)"

        return lifecycle

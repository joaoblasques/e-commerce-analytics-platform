"""
Data Governance Module for E-Commerce Analytics Platform.

This module provides comprehensive data governance capabilities including:
- Data lineage tracking and cataloging
- Data privacy and compliance controls (GDPR, CCPA)
- Data quality monitoring and alerting
- Data access auditing and controls

Components:
- lineage: Data lineage tracking and catalog management
- privacy: Privacy controls and compliance management
- quality: Data quality monitoring and validation
- audit: Access auditing and security controls
"""

from src.data_governance.audit import AccessAuditor
from src.data_governance.catalog import DataCatalog
from src.data_governance.lineage import DataLineageTracker
from src.data_governance.privacy import PrivacyManager
from src.data_governance.quality import DataQualityMonitor

__all__ = [
    "DataCatalog",
    "DataLineageTracker",
    "PrivacyManager",
    "DataQualityMonitor",
    "AccessAuditor",
]

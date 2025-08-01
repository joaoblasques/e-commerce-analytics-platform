"""
Fraud Detection Module for E-Commerce Analytics Platform.

This module provides comprehensive fraud detection capabilities including:
- Rule-based fraud detection engine
- Transaction pattern analysis
- Merchant risk scoring
- Fraud alert prioritization
- Real-time streaming integration
"""

from .alert_prioritizer import (
    AlertPriority,
    AlertPriorityFactors,
    AlertStatus,
    FraudAlertPrioritizer,
    PrioritizedAlert,
)
from .fraud_detection_orchestrator import FraudDetectionOrchestrator
from .merchant_risk_scorer import MerchantProfile, MerchantRiskLevel, MerchantRiskScorer
from .pattern_analyzer import TransactionPatternAnalyzer
from .rules_engine import (
    ConfigurableRulesEngine,
    FraudAlert,
    FraudRule,
    RuleSeverity,
    RuleType,
)

__all__ = [
    # Rules Engine
    "ConfigurableRulesEngine",
    "FraudRule",
    "RuleSeverity",
    "RuleType",
    "FraudAlert",
    # Pattern Analysis
    "TransactionPatternAnalyzer",
    # Merchant Risk Scoring
    "MerchantRiskScorer",
    "MerchantProfile",
    "MerchantRiskLevel",
    # Alert Prioritization
    "FraudAlertPrioritizer",
    "AlertPriority",
    "AlertStatus",
    "PrioritizedAlert",
    "AlertPriorityFactors",
    # Main Orchestrator
    "FraudDetectionOrchestrator",
]

__version__ = "1.0.0"

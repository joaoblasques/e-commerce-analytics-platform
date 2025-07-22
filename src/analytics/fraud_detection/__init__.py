"""
Fraud Detection Module for E-Commerce Analytics Platform.

This module provides comprehensive fraud detection capabilities including:
- Rule-based fraud detection engine
- Transaction pattern analysis
- Merchant risk scoring
- Fraud alert prioritization
- Real-time streaming integration
"""

from .rules_engine import (
    ConfigurableRulesEngine,
    FraudRule,
    RuleSeverity,
    RuleType,
    FraudAlert
)

from .pattern_analyzer import TransactionPatternAnalyzer

from .merchant_risk_scorer import (
    MerchantRiskScorer,
    MerchantProfile,
    MerchantRiskLevel
)

from .alert_prioritizer import (
    FraudAlertPrioritizer,
    AlertPriority,
    AlertStatus,
    PrioritizedAlert,
    AlertPriorityFactors
)

from .fraud_detection_orchestrator import FraudDetectionOrchestrator

__all__ = [
    # Rules Engine
    'ConfigurableRulesEngine',
    'FraudRule', 
    'RuleSeverity',
    'RuleType',
    'FraudAlert',
    
    # Pattern Analysis
    'TransactionPatternAnalyzer',
    
    # Merchant Risk Scoring
    'MerchantRiskScorer',
    'MerchantProfile',
    'MerchantRiskLevel',
    
    # Alert Prioritization
    'FraudAlertPrioritizer',
    'AlertPriority',
    'AlertStatus',
    'PrioritizedAlert',
    'AlertPriorityFactors',
    
    # Main Orchestrator
    'FraudDetectionOrchestrator'
]

__version__ = "1.0.0"
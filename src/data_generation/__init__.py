"""
Data Generation Framework for E-Commerce Analytics Platform.

This module provides comprehensive data generation capabilities for realistic
e-commerce analytics scenarios including:
- Transaction data generation
- User behavior event simulation
- Product catalog updates
- Fraud pattern injection
- Temporal and geographical patterns

Usage:
    from src.data_generation import ECommerceDataGenerator

    generator = ECommerceDataGenerator()
    transactions = generator.generate_transactions(1000)
    events = generator.generate_user_events(5000)
"""

from .config import DataGenerationConfig
from .generator import ECommerceDataGenerator
from .patterns import (
    FraudPatterns,
    GeographicPatterns,
    SeasonalPatterns,
    TemporalPatterns,
)
from .producers import DataGenerationOrchestrator, KafkaDataProducer
from .reliability import (
    DeadLetterQueue,
    HealthMonitor,
    Message,
    MessageDeduplicator,
    MessageStatus,
    RetryConfig,
    RetryManager,
)
from .reliable_producer import ReliableKafkaProducer, default_alert_handler

__all__ = [
    "ECommerceDataGenerator",
    "KafkaDataProducer",
    "DataGenerationOrchestrator",
    "ReliableKafkaProducer",
    "default_alert_handler",
    "TemporalPatterns",
    "GeographicPatterns",
    "FraudPatterns",
    "SeasonalPatterns",
    "DataGenerationConfig",
    "Message",
    "MessageStatus",
    "RetryConfig",
    "DeadLetterQueue",
    "MessageDeduplicator",
    "RetryManager",
    "HealthMonitor",
]

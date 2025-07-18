"""
Data Generation Framework for E-Commerce Analytics Platform

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

from .generator import ECommerceDataGenerator
from .producers import KafkaDataProducer
from .patterns import (
    TemporalPatterns,
    GeographicPatterns,
    FraudPatterns,
    SeasonalPatterns
)
from .config import DataGenerationConfig

__all__ = [
    'ECommerceDataGenerator',
    'KafkaDataProducer',
    'TemporalPatterns',
    'GeographicPatterns',
    'FraudPatterns',
    'SeasonalPatterns',
    'DataGenerationConfig'
]
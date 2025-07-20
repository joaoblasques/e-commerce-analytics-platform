"""Kafka streaming and real-time data processing module."""

from .consumer_manager import (
    ConsumerHealthMonitor,
    StreamingConsumerManager,
    create_default_consumers,
)
from .consumers import (
    BaseStreamingConsumer,
    DataQualityChecker,
    SchemaValidator,
    StreamingConsumerError,
    TransactionStreamConsumer,
    UserBehaviorStreamConsumer,
)

__all__ = [
    # Core consumers
    "BaseStreamingConsumer",
    "TransactionStreamConsumer",
    "UserBehaviorStreamConsumer",
    # Utilities
    "SchemaValidator",
    "DataQualityChecker",
    "StreamingConsumerError",
    # Management
    "StreamingConsumerManager",
    "ConsumerHealthMonitor",
    "create_default_consumers",
]

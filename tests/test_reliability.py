"""
Tests for reliability features including dead letter queue, retry mechanisms,
message deduplication, and health monitoring.
"""

import json
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest
from kafka.errors import KafkaTimeoutError, NotLeaderForPartitionError

from src.data_generation.reliability import (
    DeadLetterQueue,
    HealthMonitor,
    Message,
    MessageDeduplicator,
    MessageStatus,
    RetryConfig,
    RetryManager,
)
from src.data_generation.reliable_producer import ReliableKafkaProducer


class TestMessage:
    """Test Message class."""

    def test_message_creation(self):
        """Test message creation with default values."""
        message = Message(
            id="test-123",
            topic="test-topic",
            key="test-key",
            value={"data": "test"},
            timestamp=datetime.now(),
        )

        assert message.id == "test-123"
        assert message.topic == "test-topic"
        assert message.key == "test-key"
        assert message.value == {"data": "test"}
        assert message.status == MessageStatus.PENDING
        assert message.retry_count == 0
        assert message.last_error is None
        assert message.checksum is None


class TestDeadLetterQueue:
    """Test DeadLetterQueue functionality."""

    def test_dlq_creation(self):
        """Test DLQ creation."""
        dlq = DeadLetterQueue(max_size=100)
        assert dlq.max_size == 100
        assert len(dlq.messages) == 0
        assert len(dlq.topic_stats) == 0

    def test_add_message_to_dlq(self):
        """Test adding message to DLQ."""
        dlq = DeadLetterQueue()
        message = Message(
            id="test-123",
            topic="test-topic",
            key="test-key",
            value={"data": "test"},
            timestamp=datetime.now(),
        )

        dlq.add_message(message, "Test error")

        assert len(dlq.messages) == 1
        assert message.status == MessageStatus.DLQ
        assert message.last_error == "Test error"
        assert dlq.topic_stats["test-topic"] == 1

    def test_get_messages_by_topic(self):
        """Test getting messages by topic."""
        dlq = DeadLetterQueue()

        message1 = Message("1", "topic1", None, {"data": "1"}, datetime.now())
        message2 = Message("2", "topic2", None, {"data": "2"}, datetime.now())
        message3 = Message("3", "topic1", None, {"data": "3"}, datetime.now())

        dlq.add_message(message1, "Error 1")
        dlq.add_message(message2, "Error 2")
        dlq.add_message(message3, "Error 3")

        topic1_messages = dlq.get_messages_by_topic("topic1")
        assert len(topic1_messages) == 2
        assert all(msg.topic == "topic1" for msg in topic1_messages)

    def test_dlq_stats(self):
        """Test DLQ statistics."""
        dlq = DeadLetterQueue()

        message1 = Message("1", "topic1", None, {"data": "1"}, datetime.now())
        message2 = Message("2", "topic2", None, {"data": "2"}, datetime.now())

        dlq.add_message(message1, "Error 1")
        dlq.add_message(message2, "Error 2")

        stats = dlq.get_stats()
        assert stats["total_messages"] == 2
        assert stats["topic_breakdown"]["topic1"] == 1
        assert stats["topic_breakdown"]["topic2"] == 1
        assert "oldest_message" in stats
        assert "newest_message" in stats

    def test_clear_topic(self):
        """Test clearing messages for specific topic."""
        dlq = DeadLetterQueue()

        message1 = Message("1", "topic1", None, {"data": "1"}, datetime.now())
        message2 = Message("2", "topic2", None, {"data": "2"}, datetime.now())
        message3 = Message("3", "topic1", None, {"data": "3"}, datetime.now())

        dlq.add_message(message1, "Error 1")
        dlq.add_message(message2, "Error 2")
        dlq.add_message(message3, "Error 3")

        cleared = dlq.clear_topic("topic1")
        assert cleared == 2
        assert len(dlq.messages) == 1
        assert dlq.messages[0].topic == "topic2"


class TestMessageDeduplicator:
    """Test MessageDeduplicator functionality."""

    def test_deduplicator_creation(self):
        """Test deduplicator creation."""
        dedup = MessageDeduplicator(ttl_seconds=60)
        assert dedup.ttl_seconds == 60
        assert dedup.redis_client is None
        assert len(dedup._memory_cache) == 0

    def test_checksum_calculation(self):
        """Test checksum calculation."""
        dedup = MessageDeduplicator()

        message1 = {"user_id": "123", "amount": 100.0, "timestamp": "2024-01-01"}
        message2 = {
            "user_id": "123",
            "amount": 100.0,
            "timestamp": "2024-01-02",
        }  # Different timestamp
        message3 = {
            "user_id": "123",
            "amount": 200.0,
            "timestamp": "2024-01-01",
        }  # Different amount

        checksum1 = dedup._calculate_checksum(message1)
        checksum2 = dedup._calculate_checksum(message2)
        checksum3 = dedup._calculate_checksum(message3)

        # Same stable data should produce same checksum
        assert checksum1 == checksum2
        # Different stable data should produce different checksum
        assert checksum1 != checksum3

    def test_duplicate_detection_memory(self):
        """Test duplicate detection with memory cache."""
        dedup = MessageDeduplicator(ttl_seconds=1)

        message = {"user_id": "123", "amount": 100.0}
        topic = "test-topic"

        # First check should not be duplicate
        assert not dedup.is_duplicate(message, topic)
        assert dedup.stats["total_checked"] == 1
        assert dedup.stats["duplicates_found"] == 0

        # Second check should be duplicate
        assert dedup.is_duplicate(message, topic)
        assert dedup.stats["total_checked"] == 2
        assert dedup.stats["duplicates_found"] == 1

        # Wait for TTL and check again
        time.sleep(1.1)
        assert not dedup.is_duplicate(message, topic)

    def test_deduplicator_stats(self):
        """Test deduplicator statistics."""
        dedup = MessageDeduplicator()

        message = {"user_id": "123", "amount": 100.0}
        topic = "test-topic"

        dedup.is_duplicate(message, topic)  # Cache miss
        dedup.is_duplicate(message, topic)  # Cache hit (duplicate)

        stats = dedup.get_stats()
        assert stats["total_checked"] == 2
        assert stats["duplicates_found"] == 1
        assert stats["cache_hits"] == 1
        assert stats["cache_misses"] == 1
        assert stats["duplicate_rate"] == 0.5
        assert stats["cache_hit_rate"] == 0.5


class TestRetryManager:
    """Test RetryManager functionality."""

    def test_retry_config(self):
        """Test retry configuration."""
        config = RetryConfig(max_retries=5, initial_delay_ms=50, backoff_multiplier=3.0)

        assert config.max_retries == 5
        assert config.initial_delay_ms == 50
        assert config.backoff_multiplier == 3.0

    def test_should_retry(self):
        """Test retry decision logic."""
        config = RetryConfig(max_retries=3)
        retry_manager = RetryManager(config)

        # Retriable error under limit
        error = KafkaTimeoutError("Timeout")
        assert retry_manager.should_retry(error, 2)

        # Retriable error at limit
        assert not retry_manager.should_retry(error, 3)

        # Non-retriable error
        error = ValueError("Invalid value")
        assert not retry_manager.should_retry(error, 0)

    def test_calculate_delay(self):
        """Test delay calculation with exponential backoff."""
        config = RetryConfig(
            initial_delay_ms=100,
            backoff_multiplier=2.0,
            max_delay_ms=1000,
            jitter=False,
        )
        retry_manager = RetryManager(config)

        # Test exponential backoff
        assert retry_manager.calculate_delay(0) == 0.1  # 100ms
        assert retry_manager.calculate_delay(1) == 0.2  # 200ms
        assert retry_manager.calculate_delay(2) == 0.4  # 400ms
        assert retry_manager.calculate_delay(3) == 0.8  # 800ms
        assert retry_manager.calculate_delay(4) == 1.0  # 1000ms (capped)

    def test_schedule_retry(self):
        """Test retry scheduling."""
        retry_manager = RetryManager()
        retry_manager.start()

        message = Message("test", "topic", None, {"data": "test"}, datetime.now())
        error = KafkaTimeoutError("Timeout")

        retry_manager.schedule_retry(message, error)

        assert message.retry_count == 1
        assert message.status == MessageStatus.RETRYING
        assert "Timeout" in message.last_error

        stats = retry_manager.get_stats()
        assert stats["total_retries"] == 1

        retry_manager.stop()


class TestHealthMonitor:
    """Test HealthMonitor functionality."""

    def test_health_monitor_creation(self):
        """Test health monitor creation."""
        monitor = HealthMonitor(check_interval_seconds=10)
        assert monitor.check_interval == 10
        assert len(monitor.health_history) == 0
        assert len(monitor.alert_callbacks) == 0

    def test_add_alert_callback(self):
        """Test adding alert callbacks."""
        monitor = HealthMonitor()
        callback = Mock()

        monitor.add_alert_callback(callback)
        assert len(monitor.alert_callbacks) == 1
        assert callback in monitor.alert_callbacks

    def test_health_evaluation(self):
        """Test health evaluation and alerting."""
        monitor = HealthMonitor()
        alert_callback = Mock()
        monitor.add_alert_callback(alert_callback)

        # Set low thresholds for testing
        monitor.thresholds["error_rate_threshold"] = 0.01
        monitor.thresholds["dlq_size_threshold"] = 5

        # Healthy data
        healthy_data = {"error_rate": 0.005, "dlq_size": 2, "avg_latency_ms": 50}
        monitor._evaluate_health(healthy_data)
        assert not alert_callback.called

        # Unhealthy data
        unhealthy_data = {
            "error_rate": 0.05,  # Above threshold
            "dlq_size": 10,  # Above threshold
            "avg_latency_ms": 50,
        }
        monitor._evaluate_health(unhealthy_data)
        assert alert_callback.call_count == 2  # Two alerts triggered


class TestReliableKafkaProducer:
    """Test ReliableKafkaProducer functionality."""

    @patch("src.data_generation.reliable_producer.KafkaProducer")
    def test_producer_creation(self, mock_kafka_producer):
        """Test reliable producer creation."""
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance

        producer = ReliableKafkaProducer(
            bootstrap_servers=["localhost:9092"], enable_deduplication=True
        )

        assert producer.bootstrap_servers == ["localhost:9092"]
        assert producer.enable_deduplication is True
        assert producer.deduplicator is not None
        assert producer.dlq is not None
        assert producer.retry_manager is not None
        assert producer.health_monitor is not None

        producer.close()

    @patch("src.data_generation.reliable_producer.KafkaProducer")
    def test_send_message_success(self, mock_kafka_producer):
        """Test successful message sending."""
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_record_metadata.partition = 0
        mock_record_metadata.offset = 123

        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance

        producer = ReliableKafkaProducer(enable_deduplication=False)

        # Send message
        message_id = producer.send_message("test-topic", {"data": "test"}, "test-key")

        # Verify send was called
        assert mock_producer_instance.send.called
        assert message_id is not None

        # Simulate success callback
        success_callback = mock_future.add_callback.call_args[0][0]
        message = list(producer.pending_messages.values())[0]
        success_callback(mock_record_metadata, message, time.time())

        # Verify stats updated
        assert producer.stats["messages_sent"] == 1
        assert producer.stats["messages_failed"] == 0

        producer.close()

    @patch("src.data_generation.reliable_producer.KafkaProducer")
    def test_send_message_with_deduplication(self, mock_kafka_producer):
        """Test message sending with deduplication."""
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance

        producer = ReliableKafkaProducer(enable_deduplication=True)

        message_data = {"user_id": "123", "amount": 100.0}

        # First send should work
        message_id1 = producer.send_message("test-topic", message_data)
        assert message_id1 is not None

        # Second send should be detected as duplicate
        message_id2 = producer.send_message("test-topic", message_data)
        assert message_id2 is None

        # Verify duplicate stats
        assert producer.stats["messages_duplicate"] == 1

        producer.close()

    @patch("src.data_generation.reliable_producer.KafkaProducer")
    def test_send_message_failure_and_retry(self, mock_kafka_producer):
        """Test message failure and retry logic."""
        mock_producer_instance = Mock()
        mock_future = Mock()

        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance

        producer = ReliableKafkaProducer(enable_deduplication=False)

        # Send message
        message_id = producer.send_message("test-topic", {"data": "test"})

        # Simulate failure callback
        error_callback = mock_future.add_errback.call_args[0][0]
        error = KafkaTimeoutError("Connection timeout")
        message = list(producer.pending_messages.values())[0]
        error_callback(error, message, time.time())

        # Verify retry was scheduled
        assert producer.stats["messages_failed"] == 1
        assert producer.stats["messages_retried"] == 1
        assert message.retry_count == 1
        assert message.status == MessageStatus.RETRYING

        producer.close()

    @patch("src.data_generation.reliable_producer.KafkaProducer")
    def test_get_stats(self, mock_kafka_producer):
        """Test statistics collection."""
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance

        producer = ReliableKafkaProducer()

        # Simulate some activity
        producer.stats["messages_sent"] = 100
        producer.stats["messages_failed"] = 5
        producer.stats["total_bytes_sent"] = 10000
        producer.stats["latency_history"] = [10, 20, 30, 40, 50]

        stats = producer.get_stats()

        assert stats["messages_sent"] == 100
        assert stats["messages_failed"] == 5
        assert stats["error_rate"] == 5 / 105  # 5 failures out of 105 total
        assert "elapsed_seconds" in stats
        assert "messages_per_second" in stats
        assert "latency_stats" in stats
        assert "dlq_stats" in stats
        assert "retry_stats" in stats

        producer.close()

    @patch("src.data_generation.reliable_producer.KafkaProducer")
    def test_health_status(self, mock_kafka_producer):
        """Test health status monitoring."""
        mock_producer_instance = Mock()
        mock_producer_instance.list_topics.return_value = Mock(
            topics={"test-topic": {}}
        )
        mock_kafka_producer.return_value = mock_producer_instance

        producer = ReliableKafkaProducer()

        health_status = producer.get_health_status()

        assert "status" in health_status
        assert "timestamp" in health_status
        assert "metrics" in health_status

        producer.close()

    @patch("src.data_generation.reliable_producer.KafkaProducer")
    def test_dlq_operations(self, mock_kafka_producer):
        """Test dead letter queue operations."""
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance

        producer = ReliableKafkaProducer()

        # Add message to DLQ manually for testing
        message = Message("test", "test-topic", None, {"data": "test"}, datetime.now())
        producer.dlq.add_message(message, "Test error")

        # Test getting DLQ messages
        dlq_messages = producer.get_dlq_messages("test-topic")
        assert len(dlq_messages) == 1
        assert dlq_messages[0].id == "test"

        # Test clearing DLQ
        cleared = producer.clear_dlq("test-topic")
        assert cleared == 1
        assert len(producer.get_dlq_messages("test-topic")) == 0

        producer.close()


@patch("src.data_generation.producers.ECommerceDataGenerator")
@patch("src.data_generation.reliable_producer.KafkaProducer")
def test_integration_reliable_producer_with_orchestrator(
    mock_kafka_producer, mock_generator
):
    """Test integration of reliable producer with orchestrator."""
    from src.data_generation import DataGenerationOrchestrator

    # Mock the generator to avoid numpy issues
    mock_generator_instance = Mock()
    mock_generator.return_value = mock_generator_instance

    orchestrator = DataGenerationOrchestrator(use_reliable_producer=True)

    # Verify reliable producer is used
    from src.data_generation.reliable_producer import ReliableKafkaProducer

    assert isinstance(orchestrator.producer, ReliableKafkaProducer)

    orchestrator.close()


def test_default_alert_handler():
    """Test default alert handler."""
    from src.data_generation.reliable_producer import default_alert_handler

    alert = {
        "type": "high_error_rate",
        "value": 0.1,
        "threshold": 0.05,
        "severity": "high",
    }

    # Should not raise exception
    default_alert_handler(alert)


if __name__ == "__main__":
    pytest.main([__file__])

"""
Reliable Kafka producer with enhanced error handling, retry mechanisms,
dead letter queue, message deduplication, and health monitoring.
"""

import json
import logging
import statistics
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import redis
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

from .reliability import (
    DeadLetterQueue,
    HealthMonitor,
    Message,
    MessageDeduplicator,
    MessageStatus,
    RetryConfig,
    RetryManager,
)

logger = logging.getLogger(__name__)


class ReliableKafkaProducer:
    """
    Enhanced Kafka producer with reliability features:
    - Dead letter queue for failed messages
    - Retry mechanisms with exponential backoff
    - Message deduplication
    - Health monitoring and alerting
    """

    def __init__(
        self,
        bootstrap_servers: List[str] = None,
        config: Dict[str, Any] = None,
        retry_config: RetryConfig = None,
        redis_client: Optional[redis.Redis] = None,
        dlq_max_size: int = 10000,
        enable_deduplication: bool = True,
        health_check_interval: int = 30,
    ):
        """Initialize reliable Kafka producer."""

        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.enable_deduplication = enable_deduplication

        # Enhanced producer configuration
        producer_config = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": f"reliable-producer-{uuid.uuid4().hex[:8]}",
            "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
            "key_serializer": lambda k: str(k).encode("utf-8") if k else None,
            "acks": "all",  # Wait for all replicas
            "retries": 3,  # Required for idempotent producer
            "max_in_flight_requests_per_connection": 1,  # Ensure ordering
            "enable_idempotence": True,  # Prevent duplicates at broker level
            "compression_type": "lz4",
            "batch_size": 16384,
            "linger_ms": 10,
            "buffer_memory": 67108864,  # 64MB
            "request_timeout_ms": 30000,
            "delivery_timeout_ms": 120000,
        }

        # Apply custom configuration
        if config:
            producer_config.update(config)

        self.producer = KafkaProducer(**producer_config)

        # Reliability components
        self.dlq = DeadLetterQueue(max_size=dlq_max_size)
        self.deduplicator = (
            MessageDeduplicator(redis_client) if enable_deduplication else None
        )
        self.retry_manager = RetryManager(retry_config or RetryConfig())
        self.health_monitor = HealthMonitor(health_check_interval)

        # Message tracking
        self.pending_messages: Dict[str, Message] = {}
        self.message_lock = threading.Lock()

        # Statistics
        self.stats = {
            "messages_sent": 0,
            "messages_failed": 0,
            "messages_duplicate": 0,
            "messages_retried": 0,
            "messages_dlq": 0,
            "total_bytes_sent": 0,
            "start_time": datetime.now(),
            "topic_stats": defaultdict(
                lambda: {"sent": 0, "failed": 0, "duplicate": 0, "retried": 0, "dlq": 0}
            ),
            "latency_history": [],
        }

        # Start reliability components
        self.retry_manager.start()
        self.health_monitor.start(self._get_health_data)

        logger.info(
            f"Reliable Kafka producer initialized with client_id: {producer_config['client_id']}"
        )

    def send_message(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
        callback: Optional[Callable] = None,
    ) -> str:
        """Send a message with reliability features."""

        # Check for duplicates
        if self.enable_deduplication and self.deduplicator:
            if self.deduplicator.is_duplicate(value, topic):
                self.stats["messages_duplicate"] += 1
                self.stats["topic_stats"][topic]["duplicate"] += 1
                logger.debug(f"Duplicate message detected for topic {topic}, skipping")
                return None

        # Create message wrapper
        message_id = str(uuid.uuid4())
        message = Message(
            id=message_id, topic=topic, key=key, value=value, timestamp=datetime.now()
        )

        # Add checksum for integrity checking
        if self.deduplicator:
            message.checksum = self.deduplicator._calculate_checksum(value)

        # Track message
        with self.message_lock:
            self.pending_messages[message_id] = message

        # Send message
        self._send_message_internal(message, callback)

        return message_id

    def _send_message_internal(
        self, message: Message, callback: Optional[Callable] = None
    ) -> None:
        """Internal method to send message with error handling."""

        start_time = time.time()

        try:
            # Send to Kafka
            future = self.producer.send(
                message.topic, value=message.value, key=message.key
            )

            # Add callback for success/failure handling
            future.add_callback(self._on_send_success, message, start_time, callback)
            future.add_errback(self._on_send_error, message, start_time, callback)

        except Exception as e:
            logger.error(f"Error sending message {message.id}: {e}")
            self._handle_send_failure(message, e, start_time, callback)

    def _on_send_success(
        self,
        record_metadata,
        message: Message,
        start_time: float,
        callback: Optional[Callable] = None,
    ) -> None:
        """Handle successful message send."""

        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000

        # Update message status
        message.status = MessageStatus.SENT

        # Update statistics
        self.stats["messages_sent"] += 1
        self.stats["topic_stats"][message.topic]["sent"] += 1
        self.stats["total_bytes_sent"] += len(json.dumps(message.value, default=str))
        self.stats["latency_history"].append(latency_ms)

        # Keep only last 1000 latency measurements
        if len(self.stats["latency_history"]) > 1000:
            self.stats["latency_history"] = self.stats["latency_history"][-1000:]

        # Remove from pending
        with self.message_lock:
            self.pending_messages.pop(message.id, None)

        logger.debug(
            f"Message {message.id} sent successfully to {message.topic} "
            f"(partition: {record_metadata.partition}, offset: {record_metadata.offset}, "
            f"latency: {latency_ms:.2f}ms)"
        )

        # Call user callback
        if callback:
            try:
                callback(True, message, record_metadata)
            except Exception as e:
                logger.error(f"Error in user callback: {e}")

    def _on_send_error(
        self,
        exception,
        message: Message,
        start_time: float,
        callback: Optional[Callable] = None,
    ) -> None:
        """Handle message send error."""
        self._handle_send_failure(message, exception, start_time, callback)

    def _handle_send_failure(
        self,
        message: Message,
        error: Exception,
        start_time: float,
        callback: Optional[Callable] = None,
    ) -> None:
        """Handle message send failure with retry logic."""

        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000

        message.status = MessageStatus.FAILED
        message.last_error = str(error)

        # Update statistics
        self.stats["messages_failed"] += 1
        self.stats["topic_stats"][message.topic]["failed"] += 1
        self.stats["latency_history"].append(latency_ms)

        logger.warning(f"Message {message.id} failed to send: {error}")

        # Try to retry or move to DLQ
        if self.retry_manager.should_retry(error, message.retry_count):
            self.stats["messages_retried"] += 1
            self.stats["topic_stats"][message.topic]["retried"] += 1
            self.retry_manager.schedule_retry(message, error)
        else:
            # Move to DLQ
            self.stats["messages_dlq"] += 1
            self.stats["topic_stats"][message.topic]["dlq"] += 1
            self.dlq.add_message(message, str(error))

            # Remove from pending
            with self.message_lock:
                self.pending_messages.pop(message.id, None)

        # Call user callback
        if callback:
            try:
                callback(False, message, error)
            except Exception as e:
                logger.error(f"Error in user callback: {e}")

    def _retry_message(self, message: Message) -> None:
        """Retry sending a message."""
        logger.info(f"Retrying message {message.id} (attempt {message.retry_count})")
        self._send_message_internal(message)

    def send_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        key_extractor: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """Send batch of messages with reliability features."""

        results = {
            "total": len(messages),
            "sent": 0,
            "failed": 0,
            "duplicate": 0,
            "message_ids": [],
        }

        logger.info(f"Sending batch of {len(messages)} messages to {topic}")

        for message_data in messages:
            key = key_extractor(message_data) if key_extractor else None
            message_id = self.send_message(topic, message_data, key)

            if message_id:
                results["message_ids"].append(message_id)
                results["sent"] += 1
            else:
                results["duplicate"] += 1

        # Wait for all messages to be sent or failed
        self.flush()

        logger.info(f"Batch send completed: {results}")
        return results

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush all pending messages."""
        try:
            self.producer.flush(timeout)
        except Exception as e:
            logger.error(f"Error flushing producer: {e}")

    def get_message_status(self, message_id: str) -> Optional[MessageStatus]:
        """Get status of a specific message."""
        with self.message_lock:
            message = self.pending_messages.get(message_id)
            return message.status if message else None

    def get_pending_messages(self) -> List[Message]:
        """Get all pending messages."""
        with self.message_lock:
            return list(self.pending_messages.values())

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive producer statistics."""
        stats = dict(self.stats)

        # Calculate derived metrics
        elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
        stats["elapsed_seconds"] = elapsed

        if elapsed > 0:
            stats["messages_per_second"] = self.stats["messages_sent"] / elapsed
            stats["bytes_per_second"] = self.stats["total_bytes_sent"] / elapsed

        # Latency statistics
        if self.stats["latency_history"]:
            stats["latency_stats"] = {
                "avg_ms": statistics.mean(self.stats["latency_history"]),
                "median_ms": statistics.median(self.stats["latency_history"]),
                "p95_ms": self._percentile(self.stats["latency_history"], 95),
                "p99_ms": self._percentile(self.stats["latency_history"], 99),
                "min_ms": min(self.stats["latency_history"]),
                "max_ms": max(self.stats["latency_history"]),
            }

        # Reliability component stats
        stats["dlq_stats"] = self.dlq.get_stats()
        stats["retry_stats"] = self.retry_manager.get_stats()

        if self.deduplicator:
            stats["deduplication_stats"] = self.deduplicator.get_stats()

        # Pending messages
        with self.message_lock:
            stats["pending_messages"] = len(self.pending_messages)

        # Error rate
        total_attempts = self.stats["messages_sent"] + self.stats["messages_failed"]
        stats["error_rate"] = (
            self.stats["messages_failed"] / total_attempts if total_attempts > 0 else 0
        )

        return stats

    def _percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile of data."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int((percentile / 100.0) * len(sorted_data))
        return sorted_data[min(index, len(sorted_data) - 1)]

    def _get_health_data(self) -> Dict[str, Any]:
        """Get health data for monitoring."""
        stats = self.get_stats()

        return {
            "timestamp": datetime.now().isoformat(),
            "producer_connected": self._check_connection(),
            "pending_messages": stats["pending_messages"],
            "error_rate": stats["error_rate"],
            "dlq_size": stats["dlq_stats"]["total_messages"],
            "avg_latency_ms": stats.get("latency_stats", {}).get("avg_ms", 0),
            "messages_per_second": stats.get("messages_per_second", 0),
            "bytes_per_second": stats.get("bytes_per_second", 0),
        }

    def _check_connection(self) -> bool:
        """Check if producer is connected to Kafka."""
        try:
            metadata = self.producer.list_topics(timeout=5)
            return len(metadata.topics) > 0
        except Exception:
            return False

    def add_health_alert_callback(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """Add callback for health alerts."""
        self.health_monitor.add_alert_callback(callback)

    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status."""
        return self.health_monitor.get_health_summary()

    def get_dlq_messages(self, topic: Optional[str] = None) -> List[Message]:
        """Get dead letter queue messages."""
        if topic:
            return self.dlq.get_messages_by_topic(topic)
        return list(self.dlq.messages)

    def clear_dlq(self, topic: Optional[str] = None) -> int:
        """Clear dead letter queue messages."""
        if topic:
            return self.dlq.clear_topic(topic)
        else:
            with self.dlq._lock:
                count = len(self.dlq.messages)
                self.dlq.messages.clear()
                self.dlq.topic_stats.clear()
                return count

    def replay_dlq_messages(
        self, topic: str, max_messages: Optional[int] = None
    ) -> int:
        """Replay failed messages from DLQ."""
        messages = self.dlq.get_messages_by_topic(topic)

        if max_messages:
            messages = messages[:max_messages]

        replayed = 0
        for message in messages:
            # Reset retry count for replay
            message.retry_count = 0
            message.status = MessageStatus.PENDING

            # Resend message
            self._send_message_internal(message)
            replayed += 1

        # Remove replayed messages from DLQ
        self.dlq.clear_topic(topic)

        logger.info(f"Replayed {replayed} messages from DLQ for topic {topic}")
        return replayed

    def start_streaming(
        self,
        topics: List[str] = None,
        duration_hours: float = None,
        rate_multiplier: float = 1.0,
    ) -> None:
        """Start streaming data to Kafka topics."""
        # This method is for compatibility with KafkaDataProducer interface
        # In practice, ReliableKafkaProducer sends individual messages via send_message()
        logger.info(f"Streaming interface activated for topics: {topics}")
        logger.info(
            f"Duration: {duration_hours} hours, Rate multiplier: {rate_multiplier}"
        )

    def stop_streaming(self) -> None:
        """Stop streaming data."""
        # For compatibility with KafkaDataProducer interface
        logger.info("Streaming stopped")

    def get_basic_stats(self) -> Dict[str, Any]:
        """Get basic producer statistics for compatibility."""
        return {
            "messages_sent": self.stats.get("messages_sent", 0),
            "messages_failed": self.stats.get("messages_failed", 0),
            "bytes_sent": self.stats.get("total_bytes_sent", 0),
            "messages_per_second": 0.0,  # Would need timing logic to calculate
        }

    def close(self) -> None:
        """Close producer and cleanup resources."""
        logger.info("Closing reliable Kafka producer...")

        # Stop reliability components
        self.retry_manager.stop()
        self.health_monitor.stop()

        # Flush any pending messages
        try:
            self.flush(timeout=30)
        except Exception as e:
            logger.error(f"Error flushing messages during close: {e}")

        # Close producer
        if self.producer:
            self.producer.close()

        logger.info("Reliable Kafka producer closed")


# Convenience function for creating default alert handler
def default_alert_handler(alert: Dict[str, Any]) -> None:
    """Default alert handler that logs alerts."""
    severity = alert.get("severity", "unknown")
    alert_type = alert.get("type", "unknown")
    value = alert.get("value", "unknown")
    threshold = alert.get("threshold", "unknown")

    logger.warning(
        f"ALERT [{severity.upper()}] {alert_type}: {value} exceeds threshold {threshold}"
    )

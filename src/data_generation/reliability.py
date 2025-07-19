"""
Enhanced reliability features for Kafka data producers.

This module provides advanced error handling, retry mechanisms, dead letter queues,
message deduplication, and health monitoring for robust data production.
"""

import hashlib
import json
import logging
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import redis
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NotLeaderForPartitionError

logger = logging.getLogger(__name__)


class MessageStatus(Enum):
    """Message processing status."""

    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    RETRYING = "retrying"
    DLQ = "dlq"
    DUPLICATE = "duplicate"


@dataclass
class RetryConfig:
    """Configuration for retry mechanisms."""

    max_retries: int = 3
    initial_delay_ms: int = 100
    max_delay_ms: int = 30000
    backoff_multiplier: float = 2.0
    jitter: bool = True
    retriable_errors: List[str] = field(
        default_factory=lambda: [
            "KafkaTimeoutError",
            "NotLeaderForPartitionError",
            "RequestTimedOutError",
            "BrokerNotAvailableError",
        ]
    )


@dataclass
class Message:
    """Message wrapper with metadata for tracking."""

    id: str
    topic: str
    key: Optional[str]
    value: Dict[str, Any]
    timestamp: datetime
    status: MessageStatus = MessageStatus.PENDING
    retry_count: int = 0
    last_error: Optional[str] = None
    checksum: Optional[str] = None


class DeadLetterQueue:
    """Dead letter queue for failed messages."""

    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.messages: deque = deque(maxlen=max_size)
        self.topic_stats: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def add_message(self, message: Message, error: str) -> None:
        """Add a failed message to the DLQ."""
        with self._lock:
            message.status = MessageStatus.DLQ
            message.last_error = error
            self.messages.append(message)
            self.topic_stats[message.topic] += 1

            logger.warning(
                f"Message {message.id} added to DLQ for topic {message.topic}: {error}"
            )

    def get_messages_by_topic(self, topic: str) -> List[Message]:
        """Get all DLQ messages for a specific topic."""
        with self._lock:
            return [msg for msg in self.messages if msg.topic == topic]

    def get_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        with self._lock:
            return {
                "total_messages": len(self.messages),
                "topic_breakdown": dict(self.topic_stats),
                "oldest_message": self.messages[0].timestamp.isoformat()
                if self.messages
                else None,
                "newest_message": self.messages[-1].timestamp.isoformat()
                if self.messages
                else None,
            }

    def clear_topic(self, topic: str) -> int:
        """Clear all messages for a specific topic."""
        with self._lock:
            original_size = len(self.messages)
            self.messages = deque(
                [msg for msg in self.messages if msg.topic != topic],
                maxlen=self.max_size,
            )
            cleared_count = original_size - len(self.messages)
            self.topic_stats[topic] = 0

            logger.info(f"Cleared {cleared_count} messages from DLQ for topic {topic}")
            return cleared_count


class MessageDeduplicator:
    """Message deduplication using Redis or in-memory cache."""

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        ttl_seconds: int = 3600,
        max_memory_size: int = 100000,
    ):
        self.redis_client = redis_client
        self.ttl_seconds = ttl_seconds
        self.max_memory_size = max_memory_size

        # In-memory fallback
        self._memory_cache: Dict[str, datetime] = {}
        self._lock = threading.Lock()

        # Statistics
        self.stats = {
            "total_checked": 0,
            "duplicates_found": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }

    def _calculate_checksum(self, message: Dict[str, Any]) -> str:
        """Calculate message checksum for deduplication."""
        # Remove timestamp and other volatile fields for checksum calculation
        stable_data = {
            k: v
            for k, v in message.items()
            if k not in ["timestamp", "message_id", "processing_time"]
        }

        message_str = json.dumps(stable_data, sort_keys=True, default=str)
        return hashlib.sha256(message_str.encode()).hexdigest()

    def is_duplicate(self, message: Dict[str, Any], topic: str) -> bool:
        """Check if message is a duplicate."""
        self.stats["total_checked"] += 1

        checksum = self._calculate_checksum(message)
        key = f"msg_dedup:{topic}:{checksum}"

        # Try Redis first
        if self.redis_client:
            try:
                if self.redis_client.exists(key):
                    self.stats["duplicates_found"] += 1
                    self.stats["cache_hits"] += 1
                    return True
                else:
                    # Mark as seen
                    self.redis_client.setex(key, self.ttl_seconds, "1")
                    self.stats["cache_misses"] += 1
                    return False
            except Exception as e:
                logger.warning(
                    f"Redis deduplication failed: {e}, falling back to memory"
                )

        # Memory fallback
        with self._lock:
            now = datetime.now()

            # Clean expired entries
            expired_keys = [
                k
                for k, timestamp in self._memory_cache.items()
                if (now - timestamp).total_seconds() > self.ttl_seconds
            ]
            for k in expired_keys:
                del self._memory_cache[k]

            # Check if key exists
            if key in self._memory_cache:
                self.stats["duplicates_found"] += 1
                self.stats["cache_hits"] += 1
                return True

            # Add to cache if under limit
            if len(self._memory_cache) < self.max_memory_size:
                self._memory_cache[key] = now

            self.stats["cache_misses"] += 1
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics."""
        with self._lock:
            stats = dict(self.stats)
            stats["memory_cache_size"] = len(self._memory_cache)
            if self.stats["total_checked"] > 0:
                stats["duplicate_rate"] = (
                    self.stats["duplicates_found"] / self.stats["total_checked"]
                )
                stats["cache_hit_rate"] = (
                    self.stats["cache_hits"] / self.stats["total_checked"]
                )
            else:
                stats["duplicate_rate"] = 0.0
                stats["cache_hit_rate"] = 0.0
            return stats


class RetryManager:
    """Manages retry logic with exponential backoff."""

    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
        self.retry_queues: Dict[str, deque] = defaultdict(lambda: deque())
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._retry_thread = None
        self.stats = {
            "total_retries": 0,
            "successful_retries": 0,
            "failed_retries": 0,
            "dlq_messages": 0,
        }

    def start(self) -> None:
        """Start the retry processing thread."""
        if self._retry_thread is None or not self._retry_thread.is_alive():
            self._stop_event.clear()
            self._retry_thread = threading.Thread(
                target=self._process_retries, daemon=True
            )
            self._retry_thread.start()
            logger.info("Retry manager started")

    def stop(self) -> None:
        """Stop the retry processing thread."""
        self._stop_event.set()
        if self._retry_thread:
            self._retry_thread.join(timeout=5)
            logger.info("Retry manager stopped")

    def should_retry(self, error: Exception, retry_count: int) -> bool:
        """Determine if an error should be retried."""
        if retry_count >= self.config.max_retries:
            return False

        error_type = type(error).__name__
        return error_type in self.config.retriable_errors

    def calculate_delay(self, retry_count: int) -> float:
        """Calculate retry delay with exponential backoff."""
        delay_ms = min(
            self.config.initial_delay_ms
            * (self.config.backoff_multiplier**retry_count),
            self.config.max_delay_ms,
        )

        # Add jitter to avoid thundering herd
        if self.config.jitter:
            import random

            delay_ms *= 0.5 + random.random() * 0.5

        return delay_ms / 1000.0  # Convert to seconds

    def schedule_retry(self, message: Message, error: Exception) -> None:
        """Schedule a message for retry."""
        if self.should_retry(error, message.retry_count):
            message.retry_count += 1
            message.status = MessageStatus.RETRYING
            message.last_error = str(error)

            delay = self.calculate_delay(message.retry_count)
            retry_time = datetime.now() + timedelta(seconds=delay)

            with self._lock:
                self.retry_queues[message.topic].append((retry_time, message))
                self.stats["total_retries"] += 1

            logger.info(
                f"Scheduled retry {message.retry_count}/{self.config.max_retries} "
                f"for message {message.id} in {delay:.2f}s"
            )
        else:
            self.stats["failed_retries"] += 1
            self.stats["dlq_messages"] += 1
            logger.error(
                f"Message {message.id} exhausted retries ({message.retry_count}), "
                f"moving to DLQ: {error}"
            )

    def _process_retries(self) -> None:
        """Process retry queues in background thread."""
        while not self._stop_event.is_set():
            try:
                ready_messages = []
                now = datetime.now()

                with self._lock:
                    for topic, retry_queue in self.retry_queues.items():
                        while retry_queue and retry_queue[0][0] <= now:
                            retry_time, message = retry_queue.popleft()
                            ready_messages.append(message)

                # Process ready messages (implement callback mechanism)
                for message in ready_messages:
                    logger.debug(f"Processing retry for message {message.id}")
                    # This would call back to the producer to retry sending
                    # Implementation depends on integration with ReliableKafkaProducer

                time.sleep(0.1)  # Check every 100ms

            except Exception as e:
                logger.error(f"Error in retry processing: {e}")
                time.sleep(1)

    def get_stats(self) -> Dict[str, Any]:
        """Get retry statistics."""
        with self._lock:
            pending_count = sum(len(queue) for queue in self.retry_queues.values())
            stats = dict(self.stats)
            stats["pending_retries"] = pending_count
            stats["retry_queues"] = {
                topic: len(queue) for topic, queue in self.retry_queues.items()
            }
            return stats


class HealthMonitor:
    """Health monitoring for producers."""

    def __init__(self, check_interval_seconds: int = 30):
        self.check_interval = check_interval_seconds
        self.health_history: deque = deque(maxlen=100)  # Last 100 health checks
        self.alert_callbacks: List[Callable] = []
        self._stop_event = threading.Event()
        self._monitor_thread = None

        # Health thresholds
        self.thresholds = {
            "error_rate_threshold": 0.05,  # 5% error rate
            "latency_threshold_ms": 1000,  # 1 second
            "dlq_size_threshold": 100,  # 100 messages in DLQ
            "memory_usage_threshold": 0.8,  # 80% memory usage
        }

    def start(self, producer_health_callback: Callable) -> None:
        """Start health monitoring."""
        self.producer_health_callback = producer_health_callback

        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop, daemon=True
            )
            self._monitor_thread.start()
            logger.info("Health monitor started")

    def stop(self) -> None:
        """Stop health monitoring."""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
            logger.info("Health monitor stopped")

    def add_alert_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Add callback for health alerts."""
        self.alert_callbacks.append(callback)

    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while not self._stop_event.is_set():
            try:
                health_data = self._collect_health_data()
                self._evaluate_health(health_data)
                self.health_history.append(health_data)

                time.sleep(self.check_interval)

            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                time.sleep(self.check_interval)

    def _collect_health_data(self) -> Dict[str, Any]:
        """Collect health data from producer."""
        if self.producer_health_callback:
            return self.producer_health_callback()
        return {}

    def _evaluate_health(self, health_data: Dict[str, Any]) -> None:
        """Evaluate health data and trigger alerts if needed."""
        alerts = []

        # Check error rate
        if "error_rate" in health_data:
            if health_data["error_rate"] > self.thresholds["error_rate_threshold"]:
                alerts.append(
                    {
                        "type": "high_error_rate",
                        "value": health_data["error_rate"],
                        "threshold": self.thresholds["error_rate_threshold"],
                        "severity": "high",
                    }
                )

        # Check DLQ size
        if "dlq_size" in health_data:
            if health_data["dlq_size"] > self.thresholds["dlq_size_threshold"]:
                alerts.append(
                    {
                        "type": "high_dlq_size",
                        "value": health_data["dlq_size"],
                        "threshold": self.thresholds["dlq_size_threshold"],
                        "severity": "medium",
                    }
                )

        # Check latency
        if "avg_latency_ms" in health_data:
            if health_data["avg_latency_ms"] > self.thresholds["latency_threshold_ms"]:
                alerts.append(
                    {
                        "type": "high_latency",
                        "value": health_data["avg_latency_ms"],
                        "threshold": self.thresholds["latency_threshold_ms"],
                        "severity": "medium",
                    }
                )

        # Trigger alerts
        for alert in alerts:
            for callback in self.alert_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    logger.error(f"Error in alert callback: {e}")

    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary."""
        if not self.health_history:
            return {"status": "unknown", "message": "No health data available"}

        recent_health = list(self.health_history)[-10:]  # Last 10 checks

        # Calculate averages
        avg_error_rate = sum(h.get("error_rate", 0) for h in recent_health) / len(
            recent_health
        )
        avg_latency = sum(h.get("avg_latency_ms", 0) for h in recent_health) / len(
            recent_health
        )
        max_dlq_size = max(h.get("dlq_size", 0) for h in recent_health)

        # Determine overall status
        status = "healthy"
        issues = []

        if avg_error_rate > self.thresholds["error_rate_threshold"]:
            status = "unhealthy"
            issues.append(f"High error rate: {avg_error_rate:.2%}")

        if avg_latency > self.thresholds["latency_threshold_ms"]:
            status = "degraded" if status == "healthy" else status
            issues.append(f"High latency: {avg_latency:.0f}ms")

        if max_dlq_size > self.thresholds["dlq_size_threshold"]:
            status = "degraded" if status == "healthy" else status
            issues.append(f"High DLQ size: {max_dlq_size}")

        return {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "metrics": {
                "avg_error_rate": avg_error_rate,
                "avg_latency_ms": avg_latency,
                "max_dlq_size": max_dlq_size,
            },
            "issues": issues,
            "thresholds": self.thresholds,
            "checks_performed": len(self.health_history),
        }

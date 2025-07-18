"""Monitoring utilities for data ingestion producers."""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ProducerMetrics:
    """Metrics for a data producer."""

    name: str
    messages_sent: int = 0
    messages_failed: int = 0
    bytes_sent: int = 0
    start_time: float = field(default_factory=time.time)
    last_send_time: Optional[float] = None
    error_count: int = 0
    last_error: Optional[str] = None

    def add_success(self, bytes_sent: int):
        """Record a successful message send."""
        self.messages_sent += 1
        self.bytes_sent += bytes_sent
        self.last_send_time = time.time()

    def add_failure(self, error: str):
        """Record a failed message send."""
        self.messages_failed += 1
        self.error_count += 1
        self.last_error = error

    def get_rate_per_second(self) -> float:
        """Calculate messages per second."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.messages_sent / elapsed
        return 0.0

    def get_success_rate(self) -> float:
        """Calculate success rate as percentage."""
        total = self.messages_sent + self.messages_failed
        if total > 0:
            return (self.messages_sent / total) * 100
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "name": self.name,
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "bytes_sent": self.bytes_sent,
            "uptime_seconds": time.time() - self.start_time,
            "rate_per_second": self.get_rate_per_second(),
            "success_rate_percent": self.get_success_rate(),
            "last_send_time": self.last_send_time,
            "error_count": self.error_count,
            "last_error": self.last_error,
        }


class ProducerMonitor:
    """Monitor for tracking multiple producer instances."""

    def __init__(self):
        self.producers: Dict[str, ProducerMetrics] = {}
        self.logger = logging.getLogger(__name__)

    def register_producer(self, name: str) -> ProducerMetrics:
        """Register a new producer for monitoring."""
        metrics = ProducerMetrics(name)
        self.producers[name] = metrics
        self.logger.info(f"Registered producer: {name}")
        return metrics

    def get_producer_metrics(self, name: str) -> Optional[ProducerMetrics]:
        """Get metrics for a specific producer."""
        return self.producers.get(name)

    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for all registered producers."""
        return {name: metrics.to_dict() for name, metrics in self.producers.items()}

    def log_metrics(self, interval_seconds: int = 60):
        """Log metrics for all producers."""
        for name, metrics in self.producers.items():
            self.logger.info(
                f"Producer {name}: "
                f"sent={metrics.messages_sent}, "
                f"failed={metrics.messages_failed}, "
                f"rate={metrics.get_rate_per_second():.2f}/s, "
                f"success={metrics.get_success_rate():.1f}%"
            )

    def get_aggregate_metrics(self) -> Dict[str, Any]:
        """Get aggregated metrics across all producers."""
        total_sent = sum(m.messages_sent for m in self.producers.values())
        total_failed = sum(m.messages_failed for m in self.producers.values())
        total_bytes = sum(m.bytes_sent for m in self.producers.values())

        total_messages = total_sent + total_failed
        success_rate = (total_sent / total_messages * 100) if total_messages > 0 else 0

        # Calculate aggregate rate
        current_time = time.time()
        total_rate = 0.0
        for metrics in self.producers.values():
            elapsed = current_time - metrics.start_time
            if elapsed > 0:
                total_rate += metrics.messages_sent / elapsed

        return {
            "total_producers": len(self.producers),
            "total_messages_sent": total_sent,
            "total_messages_failed": total_failed,
            "total_bytes_sent": total_bytes,
            "aggregate_rate_per_second": total_rate,
            "aggregate_success_rate_percent": success_rate,
            "active_producers": len(
                [m for m in self.producers.values() if m.last_send_time]
            ),
        }


# Global monitor instance
monitor = ProducerMonitor()

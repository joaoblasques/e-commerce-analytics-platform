"""Base class for Kafka producers with common functionality."""

import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError


class BaseProducer(ABC):
    """Base class for Kafka producers with error handling and monitoring."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "",
        producer_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the base producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            producer_config: Additional producer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.logger = logging.getLogger(self.__class__.__name__)

        # Default producer configuration
        default_config = {
            "bootstrap_servers": bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
            "acks": "all",
            "retries": 3,
            "batch_size": 16384,
            "linger_ms": 10,
            "buffer_memory": 33554432,
            "compression_type": "snappy",
        }

        if producer_config:
            default_config.update(producer_config)

        self.producer = KafkaProducer(**default_config)
        self.metrics = {
            "messages_sent": 0,
            "messages_failed": 0,
            "bytes_sent": 0,
            "last_send_time": None,
        }

    def send_message(self, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Send a message to the Kafka topic.

        Args:
            message: Message to send
            key: Optional message key for partitioning

        Returns:
            True if message sent successfully, False otherwise
        """
        try:
            headers = []
            correlation_id = get_correlation_id()
            if correlation_id:
                headers.append(("X-Correlation-ID", correlation_id.encode("utf-8")))

            trace_context = get_trace_context()
            if trace_context:
                headers.append(("X-Trace-ID", trace_context.trace_id.encode("utf-8")))
                headers.append(("X-Span-ID", trace_context.span_id.encode("utf-8")))
                if trace_context.parent_span_id:
                    headers.append(
                        (
                            "X-Parent-Span-ID",
                            trace_context.parent_span_id.encode("utf-8"),
                        )
                    )

            future = self.producer.send(
                self.topic, value=message, key=key, headers=headers
            )
            record_metadata = future.get(timeout=10)

            self.metrics["messages_sent"] += 1
            self.metrics["bytes_sent"] += record_metadata.serialized_value_size
            self.metrics["last_send_time"] = time.time()

            self.logger.debug(
                f"Message sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            self.metrics["messages_failed"] += 1
            self.logger.error(f"Failed to send message: {e}")
            return False
        except Exception as e:
            self.metrics["messages_failed"] += 1
            self.logger.error(f"Unexpected error sending message: {e}")
            return False

    def flush(self) -> None:
        """Flush all pending messages."""
        self.producer.flush()

    def close(self) -> None:
        """Close the producer and clean up resources."""
        self.producer.close()

    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics.

        Returns:
            Dictionary of producer metrics
        """
        return self.metrics.copy()

    @abstractmethod
    def generate_message(self) -> Dict[str, Any]:
        """Generate a message to send.

        Returns:
            Message dictionary
        """
        pass

    @abstractmethod
    def get_message_key(self, message: Dict[str, Any]) -> Optional[str]:
        """Get the message key for partitioning.

        Args:
            message: Message to get key for

        Returns:
            Message key or None
        """
        pass

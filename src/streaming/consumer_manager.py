"""
Consumer Manager for Kafka Structured Streaming.

This module provides orchestration and management capabilities for multiple
streaming consumers with health monitoring, fault tolerance, and coordination.
"""

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import SparkSession

from src.utils.logger import get_logger
from src.utils.spark_utils import get_secure_temp_dir

from .consumers import (
    BaseStreamingConsumer,
    TransactionStreamConsumer,
    UserBehaviorStreamConsumer,
)


class ConsumerHealthMonitor:
    """Health monitoring for streaming consumers."""

    def __init__(self, check_interval: int = 30):
        """Initialize health monitor.

        Args:
            check_interval: Health check interval in seconds
        """
        self.check_interval = check_interval
        self.logger = get_logger(self.__class__.__name__)
        self.consumers: Dict[str, BaseStreamingConsumer] = {}
        self.health_status: Dict[str, Dict[str, Any]] = {}
        self.monitoring_active = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.health_callbacks: List[Callable[[str, Dict[str, Any]], None]] = []

    def register_consumer(self, name: str, consumer: BaseStreamingConsumer) -> None:
        """Register a consumer for health monitoring."""
        self.consumers[name] = consumer
        self.health_status[name] = {
            "status": "registered",
            "last_check": None,
            "consecutive_failures": 0,
            "total_failures": 0,
            "uptime_start": time.time(),
        }
        self.logger.info(f"Registered consumer for monitoring: {name}")

    def unregister_consumer(self, name: str) -> None:
        """Unregister a consumer from health monitoring."""
        if name in self.consumers:
            del self.consumers[name]
            del self.health_status[name]
            self.logger.info(f"Unregistered consumer from monitoring: {name}")

    def add_health_callback(
        self, callback: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        """Add callback for health status changes."""
        self.health_callbacks.append(callback)

    def start_monitoring(self) -> None:
        """Start health monitoring in a separate thread."""
        if self.monitoring_active:
            self.logger.warning("Health monitoring already active")
            return

        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        self.logger.info("Started health monitoring")

    def stop_monitoring(self) -> None:
        """Stop health monitoring."""
        self.monitoring_active = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        self.logger.info("Stopped health monitoring")

    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self.monitoring_active:
            try:
                self._check_all_consumers()
                time.sleep(self.check_interval)
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.check_interval)

    def _check_all_consumers(self) -> None:
        """Check health of all registered consumers."""
        for name, consumer in self.consumers.items():
            try:
                health_data = self._check_consumer_health(name, consumer)
                self._update_health_status(name, health_data)

                # Trigger callbacks if status changed
                for callback in self.health_callbacks:
                    try:
                        callback(name, health_data)
                    except Exception as e:
                        self.logger.error(f"Error in health callback: {e}")

            except Exception as e:
                self.logger.error(f"Error checking health for consumer {name}: {e}")
                self._record_health_failure(name, str(e))

    def _check_consumer_health(
        self, name: str, consumer: BaseStreamingConsumer
    ) -> Dict[str, Any]:
        """Check health of a single consumer."""
        health_data = {
            "timestamp": time.time(),
            "consumer_name": name,
            "is_healthy": True,
            "errors": [],
            "metrics": {},
        }

        try:
            # Get stream status
            stream_status = consumer.get_stream_status()
            health_data["stream_status"] = stream_status

            # Check if stream is active
            if stream_status.get("status") != "active":
                health_data["is_healthy"] = False
                health_data["errors"].append(
                    f"Stream not active: {stream_status.get('status')}"
                )

            # Check for processing issues
            if stream_status.get("input_rows_per_second") is not None:
                input_rate = stream_status.get("input_rows_per_second", 0)
                processed_rate = stream_status.get("processed_rows_per_second", 0)

                # Check if processing is keeping up
                if input_rate > 0 and processed_rate < input_rate * 0.8:
                    health_data["errors"].append(
                        f"Processing lag detected: input={input_rate:.2f}/s, "
                        f"processed={processed_rate:.2f}/s"
                    )

                health_data["metrics"] = {
                    "input_rows_per_second": input_rate,
                    "processed_rows_per_second": processed_rate,
                    "processing_ratio": processed_rate / input_rate
                    if input_rate > 0
                    else 1.0,
                }

            # Check batch duration
            batch_duration = stream_status.get("batch_duration")
            if batch_duration and batch_duration > 10000:  # 10 seconds
                health_data["errors"].append(f"High batch duration: {batch_duration}ms")

        except Exception as e:
            health_data["is_healthy"] = False
            health_data["errors"].append(f"Health check failed: {str(e)}")

        return health_data

    def _update_health_status(self, name: str, health_data: Dict[str, Any]) -> None:
        """Update health status for a consumer."""
        if name not in self.health_status:
            return

        status = self.health_status[name]
        status["last_check"] = health_data["timestamp"]

        if health_data["is_healthy"]:
            status["consecutive_failures"] = 0
            if status.get("status") != "healthy":
                status["status"] = "healthy"
                self.logger.info(f"Consumer {name} is now healthy")
        else:
            status["consecutive_failures"] += 1
            status["total_failures"] += 1
            status["status"] = "unhealthy"
            status["last_error"] = health_data["errors"]

            self.logger.warning(
                f"Consumer {name} is unhealthy (consecutive failures: {status['consecutive_failures']}): "
                f"{health_data['errors']}"
            )

    def _record_health_failure(self, name: str, error: str) -> None:
        """Record a health check failure."""
        if name in self.health_status:
            status = self.health_status[name]
            status["consecutive_failures"] += 1
            status["total_failures"] += 1
            status["status"] = "check_failed"
            status["last_error"] = [error]
            status["last_check"] = time.time()

    def get_health_summary(self) -> Dict[str, Any]:
        """Get overall health summary."""
        total_consumers = len(self.consumers)
        healthy_consumers = sum(
            1
            for status in self.health_status.values()
            if status.get("status") == "healthy"
        )

        return {
            "total_consumers": total_consumers,
            "healthy_consumers": healthy_consumers,
            "unhealthy_consumers": total_consumers - healthy_consumers,
            "overall_status": "healthy"
            if healthy_consumers == total_consumers
            else "degraded",
            "consumer_details": self.health_status.copy(),
            "monitoring_active": self.monitoring_active,
        }


class StreamingConsumerManager:
    """Manager for orchestrating multiple streaming consumers."""

    def __init__(
        self,
        spark: SparkSession,
        kafka_bootstrap_servers: str = "localhost:9092",
        base_checkpoint_location: Optional[str] = None,
        max_concurrent_consumers: int = 4,
    ):
        """Initialize consumer manager.

        Args:
            spark: SparkSession instance
            kafka_bootstrap_servers: Kafka bootstrap servers
            base_checkpoint_location: Base directory for checkpoints
            max_concurrent_consumers: Maximum number of concurrent consumers
        """
        self.spark = spark
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        # Use secure temp directory if not provided
        self.base_checkpoint_location = base_checkpoint_location or get_secure_temp_dir(
            "streaming_checkpoints"
        )
        self.max_concurrent_consumers = max_concurrent_consumers

        self.logger = get_logger(self.__class__.__name__)

        # Consumer registry
        self.consumers: Dict[str, BaseStreamingConsumer] = {}
        self.consumer_futures: Dict[str, Future] = {}

        # Thread pool for managing consumers
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_consumers)

        # Health monitoring
        self.health_monitor = ConsumerHealthMonitor()
        self.health_monitor.add_health_callback(self._on_health_change)

        # Shutdown coordination
        self.shutdown_event = threading.Event()

    def register_consumer(
        self,
        name: str,
        consumer_class: type,
        topic: str,
        consumer_group: str,
        **consumer_kwargs,
    ) -> None:
        """Register a new streaming consumer.

        Args:
            name: Unique name for the consumer
            consumer_class: Consumer class to instantiate
            topic: Kafka topic to consume from
            consumer_group: Consumer group ID
            **consumer_kwargs: Additional arguments for consumer
        """
        if name in self.consumers:
            raise ValueError(f"Consumer with name '{name}' already registered")

        # Create consumer instance
        consumer = consumer_class(
            spark=self.spark,
            kafka_bootstrap_servers=self.kafka_bootstrap_servers,
            topic=topic,
            consumer_group=consumer_group,
            checkpoint_location=f"{self.base_checkpoint_location}/{name}",
            **consumer_kwargs,
        )

        # Set error callback
        consumer.set_error_callback(lambda e: self._on_consumer_error(name, e))

        self.consumers[name] = consumer
        self.health_monitor.register_consumer(name, consumer)

        self.logger.info(
            f"Registered consumer: {name} (topic: {topic}, group: {consumer_group})"
        )

    def start_consumer(
        self,
        name: str,
        output_mode: str = "append",
        trigger_interval: str = "5 seconds",
        **stream_kwargs,
    ) -> None:
        """Start a specific consumer.

        Args:
            name: Consumer name
            output_mode: Streaming output mode
            trigger_interval: Trigger processing interval
            **stream_kwargs: Additional stream configuration
        """
        if name not in self.consumers:
            raise ValueError(f"Consumer '{name}' not registered")

        if name in self.consumer_futures:
            raise ValueError(f"Consumer '{name}' already started")

        consumer = self.consumers[name]

        # Submit consumer start task to executor
        future = self.executor.submit(
            self._start_consumer_task,
            name,
            consumer,
            output_mode,
            trigger_interval,
            **stream_kwargs,
        )

        self.consumer_futures[name] = future
        self.logger.info(f"Starting consumer: {name}")

    def start_all_consumers(self, **stream_kwargs) -> None:
        """Start all registered consumers."""
        for name in self.consumers:
            if name not in self.consumer_futures:
                try:
                    self.start_consumer(name, **stream_kwargs)
                except Exception as e:
                    self.logger.error(f"Failed to start consumer {name}: {e}")

        # Start health monitoring
        self.health_monitor.start_monitoring()
        self.logger.info("Started all consumers and health monitoring")

    def stop_consumer(self, name: str) -> None:
        """Stop a specific consumer."""
        if name not in self.consumers:
            self.logger.warning(f"Consumer '{name}' not registered")
            return

        consumer = self.consumers[name]

        try:
            consumer.stop_stream()

            # Cancel future if it exists
            if name in self.consumer_futures:
                future = self.consumer_futures[name]
                future.cancel()
                del self.consumer_futures[name]

            self.logger.info(f"Stopped consumer: {name}")

        except Exception as e:
            self.logger.error(f"Error stopping consumer {name}: {e}")

    def stop_all_consumers(self) -> None:
        """Stop all consumers gracefully."""
        self.logger.info("Stopping all consumers...")

        # Signal shutdown
        self.shutdown_event.set()

        # Stop health monitoring
        self.health_monitor.stop_monitoring()

        # Stop all consumers
        for name in list(self.consumers.keys()):
            self.stop_consumer(name)

        # Shutdown executor
        self.executor.shutdown(wait=True, timeout=30)

        self.logger.info("All consumers stopped")

    def _start_consumer_task(
        self,
        name: str,
        consumer: BaseStreamingConsumer,
        output_mode: str,
        trigger_interval: str,
        **stream_kwargs,
    ) -> None:
        """Task to start a consumer (runs in thread pool)."""
        try:
            # Start the streaming query
            streaming_query = consumer.start_stream(
                output_mode=output_mode,
                trigger_interval=trigger_interval,
                **stream_kwargs,
            )

            self.logger.info(f"Consumer {name} started successfully")

            # Wait for termination or shutdown signal
            while not self.shutdown_event.is_set() and streaming_query.isActive:
                try:
                    streaming_query.awaitTermination(timeout=5)
                except Exception as e:
                    # Timeout is expected for monitoring, log only unexpected errors
                    if "timeout" not in str(e).lower():
                        self.logger.debug(f"Stream monitoring exception: {e}")
                    continue

            # Stop if shutdown requested
            if self.shutdown_event.is_set():
                consumer.stop_stream()

        except Exception as e:
            self.logger.error(f"Error in consumer task {name}: {e}")
            raise

    def _on_consumer_error(self, name: str, error: Exception) -> None:
        """Handle consumer errors."""
        self.logger.error(f"Consumer {name} error: {error}")

        # Implement error recovery logic here
        # For example: restart consumer, alert administrators, etc.

    def _on_health_change(self, name: str, health_data: Dict[str, Any]) -> None:
        """Handle health status changes."""
        if not health_data["is_healthy"]:
            self.logger.warning(
                f"Consumer {name} health degraded: {health_data['errors']}"
            )

            # Implement automatic recovery logic here
            # For example: restart unhealthy consumers

    def get_consumer_status(self, name: str) -> Dict[str, Any]:
        """Get status of a specific consumer."""
        if name not in self.consumers:
            return {"error": f"Consumer '{name}' not registered"}

        consumer = self.consumers[name]
        stream_status = consumer.get_stream_status()

        # Get future status
        future_status = "not_started"
        if name in self.consumer_futures:
            future = self.consumer_futures[name]
            if future.running():
                future_status = "running"
            elif future.done():
                if future.exception():
                    future_status = f"failed: {future.exception()}"
                else:
                    future_status = "completed"
            else:
                future_status = "pending"

        return {
            "name": name,
            "future_status": future_status,
            "stream_status": stream_status,
            "topic": consumer.topic,
            "consumer_group": consumer.consumer_group,
        }

    def get_all_consumer_status(self) -> Dict[str, Any]:
        """Get status of all consumers."""
        status = {
            "consumers": {},
            "health_summary": self.health_monitor.get_health_summary(),
            "manager_status": {
                "active_consumers": len(self.consumer_futures),
                "registered_consumers": len(self.consumers),
                "shutdown_requested": self.shutdown_event.is_set(),
            },
        }

        for name in self.consumers:
            status["consumers"][name] = self.get_consumer_status(name)

        return status

    def wait_for_all_consumers(self, timeout: Optional[int] = None) -> None:
        """Wait for all consumers to complete."""
        try:
            for name, future in self.consumer_futures.items():
                try:
                    future.result(timeout=timeout)
                    self.logger.info(f"Consumer {name} completed")
                except Exception as e:
                    self.logger.error(f"Consumer {name} failed: {e}")
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, stopping all consumers...")
            self.stop_all_consumers()


def create_default_consumers(manager: StreamingConsumerManager) -> None:
    """Create and register default consumers for the e-commerce platform."""
    # Register transaction consumer
    manager.register_consumer(
        name="transaction_consumer",
        consumer_class=TransactionStreamConsumer,
        topic="transactions",
        consumer_group="transaction-analytics-group",
        max_offsets_per_trigger=1000,
        enable_backpressure=True,
    )

    # Register user behavior consumer
    manager.register_consumer(
        name="user_behavior_consumer",
        consumer_class=UserBehaviorStreamConsumer,
        topic="user-events",
        consumer_group="user-behavior-analytics-group",
        max_offsets_per_trigger=2000,  # Higher throughput for user events
        enable_backpressure=True,
    )

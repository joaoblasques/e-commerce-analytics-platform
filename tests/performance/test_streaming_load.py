"""
Performance load testing for streaming pipelines.

This module provides comprehensive load testing for Kafka producers, consumers,
and Spark Structured Streaming pipelines to validate system performance under high load.
"""

import json
import logging
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import psutil
import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics container."""

    throughput_events_per_second: float
    average_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_rate_percent: float
    cpu_usage_percent: float
    memory_usage_mb: float
    total_events_processed: int
    test_duration_seconds: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for reporting."""
        return {
            "throughput_events_per_second": self.throughput_events_per_second,
            "average_latency_ms": self.average_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
            "error_rate_percent": self.error_rate_percent,
            "cpu_usage_percent": self.cpu_usage_percent,
            "memory_usage_mb": self.memory_usage_mb,
            "total_events_processed": self.total_events_processed,
            "test_duration_seconds": self.test_duration_seconds,
        }


class KafkaLoadTester:
    """Kafka streaming load tester with comprehensive metrics collection."""

    def __init__(self, kafka_bootstrap_servers: str):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.consumers = []
        self.metrics = []
        self.errors = []
        self.start_time = None
        self.end_time = None

    def create_producer(self, **kwargs) -> KafkaProducer:
        """Create high-performance Kafka producer."""
        producer_config = {
            "bootstrap_servers": self.kafka_bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "key_serializer": lambda k: str(k).encode("utf-8") if k else None,
            "batch_size": 16384,  # 16KB batches for high throughput
            "linger_ms": 10,  # Small delay for batching
            "compression_type": "snappy",  # Fast compression
            "acks": 1,  # Leader acknowledgment for balance of speed/durability
            "retries": 3,
            "max_in_flight_requests_per_connection": 5,
            **kwargs,
        }
        return KafkaProducer(**producer_config)

    def create_consumer(self, topic: str, group_id: str, **kwargs) -> KafkaConsumer:
        """Create high-performance Kafka consumer."""
        consumer_config = {
            "bootstrap_servers": self.kafka_bootstrap_servers,
            "group_id": group_id,
            "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
            "key_deserializer": lambda k: k.decode("utf-8") if k else None,
            "auto_offset_reset": "latest",
            "enable_auto_commit": True,
            "auto_commit_interval_ms": 1000,
            "fetch_max_wait_ms": 500,
            "fetch_min_bytes": 1024,
            "max_poll_records": 500,
            **kwargs,
        }
        return KafkaConsumer(topic, **consumer_config)

    def generate_transaction_data(self, event_id: int) -> Dict[str, Any]:
        """Generate realistic transaction data for load testing."""
        return {
            "transaction_id": f"load_test_txn_{event_id}",
            "user_id": f"user_{event_id % 10000}",
            "product_id": f"prod_{event_id % 1000}",
            "amount": round((event_id % 1000) * 0.99 + 10.00, 2),
            "timestamp": datetime.now().isoformat(),
            "event_id": event_id,
            "test_metadata": {"generated_at": time.time(), "load_test": True},
        }

    @contextmanager
    def resource_monitor(self):
        """Context manager for monitoring system resources during load test."""
        process = psutil.Process()
        cpu_samples = []
        memory_samples = []
        monitoring = True

        def monitor_resources():
            while monitoring:
                try:
                    cpu_samples.append(process.cpu_percent())
                    memory_samples.append(process.memory_info().rss / 1024 / 1024)  # MB
                    time.sleep(0.1)  # Sample every 100ms
                except:
                    pass

        monitor_thread = threading.Thread(target=monitor_resources)
        monitor_thread.start()

        try:
            yield cpu_samples, memory_samples
        finally:
            monitoring = False
            monitor_thread.join(timeout=1)

    def producer_load_test(
        self, topic: str, target_rps: int, duration_seconds: int
    ) -> PerformanceMetrics:
        """
        Run producer load test with specified target RPS.

        Args:
            topic: Kafka topic to produce to
            target_rps: Target requests per second
            duration_seconds: Test duration in seconds

        Returns:
            PerformanceMetrics with test results
        """
        logger.info(
            f"Starting producer load test: {target_rps} RPS for {duration_seconds}s"
        )

        producer = self.create_producer()
        sent_messages = []
        errors = []

        with self.resource_monitor() as (cpu_samples, memory_samples):
            start_time = time.time()
            event_id = 0

            while time.time() - start_time < duration_seconds:
                batch_start = time.time()

                # Send messages to reach target RPS
                for _ in range(max(1, target_rps // 10)):  # Batch sends
                    try:
                        message = self.generate_transaction_data(event_id)
                        send_time = time.time()

                        future = producer.send(
                            topic, value=message, key=message["user_id"]
                        )
                        sent_messages.append((event_id, send_time))
                        event_id += 1

                    except Exception as e:
                        errors.append((event_id, str(e)))
                        logger.error(f"Producer error: {e}")

                # Rate limiting
                batch_duration = time.time() - batch_start
                expected_batch_duration = 0.1  # 100ms batches
                if batch_duration < expected_batch_duration:
                    time.sleep(expected_batch_duration - batch_duration)

            # Flush remaining messages
            producer.flush(timeout=30)
            end_time = time.time()

        producer.close()

        # Calculate metrics
        total_duration = end_time - start_time
        total_sent = len(sent_messages)
        throughput = total_sent / total_duration
        error_rate = (len(errors) / max(total_sent, 1)) * 100

        # Resource usage
        avg_cpu = statistics.mean(cpu_samples) if cpu_samples else 0
        avg_memory = statistics.mean(memory_samples) if memory_samples else 0

        return PerformanceMetrics(
            throughput_events_per_second=throughput,
            average_latency_ms=0,  # Producer latency measured differently
            p95_latency_ms=0,
            p99_latency_ms=0,
            error_rate_percent=error_rate,
            cpu_usage_percent=avg_cpu,
            memory_usage_mb=avg_memory,
            total_events_processed=total_sent,
            test_duration_seconds=total_duration,
        )

    def consumer_load_test(
        self, topic: str, group_id: str, duration_seconds: int
    ) -> PerformanceMetrics:
        """
        Run consumer load test measuring consumption performance.

        Args:
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            duration_seconds: Test duration in seconds

        Returns:
            PerformanceMetrics with test results
        """
        logger.info(f"Starting consumer load test for {duration_seconds}s")

        consumer = self.create_consumer(topic, group_id)
        consumed_messages = []
        latencies = []
        errors = []

        with self.resource_monitor() as (cpu_samples, memory_samples):
            start_time = time.time()

            while time.time() - start_time < duration_seconds:
                try:
                    message_batch = consumer.poll(timeout_ms=1000)

                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                consume_time = time.time()
                                data = message.value

                                # Calculate latency if message has timestamp
                                if (
                                    "test_metadata" in data
                                    and "generated_at" in data["test_metadata"]
                                ):
                                    latency_ms = (
                                        consume_time
                                        - data["test_metadata"]["generated_at"]
                                    ) * 1000
                                    latencies.append(latency_ms)

                                consumed_messages.append((message.offset, consume_time))

                            except Exception as e:
                                errors.append(str(e))
                                logger.error(f"Message processing error: {e}")

                except Exception as e:
                    errors.append(str(e))
                    logger.error(f"Consumer poll error: {e}")

            end_time = time.time()

        consumer.close()

        # Calculate metrics
        total_duration = end_time - start_time
        total_consumed = len(consumed_messages)
        throughput = total_consumed / total_duration
        error_rate = (len(errors) / max(total_consumed, 1)) * 100

        # Latency metrics
        avg_latency = statistics.mean(latencies) if latencies else 0
        p95_latency = (
            statistics.quantiles(latencies, n=20)[18] if latencies else 0
        )  # 95th percentile
        p99_latency = (
            statistics.quantiles(latencies, n=100)[98] if latencies else 0
        )  # 99th percentile

        # Resource usage
        avg_cpu = statistics.mean(cpu_samples) if cpu_samples else 0
        avg_memory = statistics.mean(memory_samples) if memory_samples else 0

        return PerformanceMetrics(
            throughput_events_per_second=throughput,
            average_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            error_rate_percent=error_rate,
            cpu_usage_percent=avg_cpu,
            memory_usage_mb=avg_memory,
            total_events_processed=total_consumed,
            test_duration_seconds=total_duration,
        )

    def end_to_end_load_test(
        self, topic: str, target_rps: int, duration_seconds: int
    ) -> Dict[str, PerformanceMetrics]:
        """
        Run end-to-end load test with concurrent producer and consumer.

        Args:
            topic: Kafka topic for the test
            target_rps: Target requests per second for producer
            duration_seconds: Test duration in seconds

        Returns:
            Dictionary with producer and consumer metrics
        """
        logger.info(
            f"Starting end-to-end load test: {target_rps} RPS for {duration_seconds}s"
        )

        results = {}

        with ThreadPoolExecutor(max_workers=2) as executor:
            # Start consumer first
            consumer_future = executor.submit(
                self.consumer_load_test,
                topic,
                f"load_test_group_{int(time.time())}",
                duration_seconds + 5,
            )

            # Wait a moment for consumer to be ready
            time.sleep(2)

            # Start producer
            producer_future = executor.submit(
                self.producer_load_test, topic, target_rps, duration_seconds
            )

            # Collect results
            results["producer"] = producer_future.result()
            results["consumer"] = consumer_future.result()

        return results


class StreamingPipelineLoadTester:
    """Load tester for complete streaming pipelines including Spark processing."""

    def __init__(self, kafka_bootstrap_servers: str, postgres_url: str):
        self.kafka_tester = KafkaLoadTester(kafka_bootstrap_servers)
        self.postgres_url = postgres_url

    def pipeline_throughput_test(
        self, scenarios: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Test streaming pipeline throughput under various scenarios.

        Args:
            scenarios: List of test scenarios with RPS and duration

        Returns:
            Dictionary with test results for each scenario
        """
        results = {}

        for i, scenario in enumerate(scenarios):
            scenario_name = f"scenario_{i+1}_{scenario['rps']}rps"
            logger.info(f"Running pipeline throughput test: {scenario_name}")

            try:
                # Run end-to-end test
                test_results = self.kafka_tester.end_to_end_load_test(
                    topic="transactions",
                    target_rps=scenario["rps"],
                    duration_seconds=scenario["duration"],
                )

                results[scenario_name] = {
                    "scenario": scenario,
                    "kafka_results": test_results,
                    "success": True,
                    "timestamp": datetime.now().isoformat(),
                }

                logger.info(f"Scenario {scenario_name} completed successfully")

                # Cool down between scenarios
                time.sleep(5)

            except Exception as e:
                logger.error(f"Scenario {scenario_name} failed: {e}")
                results[scenario_name] = {
                    "scenario": scenario,
                    "error": str(e),
                    "success": False,
                    "timestamp": datetime.now().isoformat(),
                }

        return results


# Test fixtures and test cases
@pytest.fixture(scope="session")
def kafka_container():
    """Kafka container for load testing."""
    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture(scope="session")
def postgres_container():
    """PostgreSQL container for load testing."""
    with PostgresContainer("postgres:13") as postgres:
        yield postgres


@pytest.fixture
def load_tester(kafka_container):
    """Load tester instance."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    return KafkaLoadTester(bootstrap_servers)


@pytest.fixture
def pipeline_tester(kafka_container, postgres_container):
    """Pipeline load tester instance."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    postgres_url = postgres_container.get_connection_url()
    return StreamingPipelineLoadTester(bootstrap_servers, postgres_url)


class TestStreamingLoadPerformance:
    """Performance load tests for streaming pipelines."""

    def test_kafka_producer_performance(self, load_tester):
        """Test Kafka producer performance under load."""
        # Test scenarios: low, medium, high load
        scenarios = [
            {"rps": 100, "duration": 10},
            {"rps": 1000, "duration": 10},
            {"rps": 5000, "duration": 10},
        ]

        results = {}

        for scenario in scenarios:
            logger.info(f"Testing producer at {scenario['rps']} RPS")

            metrics = load_tester.producer_load_test(
                topic="load_test_transactions",
                target_rps=scenario["rps"],
                duration_seconds=scenario["duration"],
            )

            results[f"{scenario['rps']}_rps"] = metrics

            # Performance assertions
            assert (
                metrics.error_rate_percent < 1.0
            ), f"Error rate too high: {metrics.error_rate_percent}%"
            assert (
                metrics.throughput_events_per_second > scenario["rps"] * 0.8
            ), "Throughput below 80% of target"

            logger.info(
                f"Producer {scenario['rps']} RPS: {metrics.throughput_events_per_second:.1f} events/sec"
            )

        # Log comprehensive results
        logger.info("Producer Performance Results:")
        for scenario, metrics in results.items():
            logger.info(
                f"  {scenario}: {metrics.throughput_events_per_second:.1f} events/sec, "
                f"{metrics.error_rate_percent:.2f}% errors"
            )

    def test_kafka_consumer_performance(self, load_tester, kafka_container):
        """Test Kafka consumer performance under load."""
        # First, produce messages for consumption
        producer = load_tester.create_producer()
        topic = "consumer_load_test"

        # Produce 10,000 messages
        logger.info("Pre-populating topic with messages for consumer test")
        for i in range(10000):
            message = load_tester.generate_transaction_data(i)
            producer.send(topic, value=message, key=message["user_id"])
        producer.flush()
        producer.close()

        # Test consumer performance
        metrics = load_tester.consumer_load_test(
            topic=topic, group_id="load_test_consumer_group", duration_seconds=30
        )

        # Performance assertions
        assert (
            metrics.error_rate_percent < 1.0
        ), f"Consumer error rate too high: {metrics.error_rate_percent}%"
        assert metrics.throughput_events_per_second > 100, "Consumer throughput too low"
        assert (
            metrics.average_latency_ms < 100
        ), f"Average latency too high: {metrics.average_latency_ms}ms"

        logger.info(
            f"Consumer Performance: {metrics.throughput_events_per_second:.1f} events/sec, "
            f"{metrics.average_latency_ms:.1f}ms avg latency"
        )

    def test_end_to_end_streaming_performance(self, load_tester):
        """Test end-to-end streaming performance with concurrent producer/consumer."""
        # Test scenarios with increasing load
        scenarios = [
            {"rps": 500, "duration": 20},
            {"rps": 1000, "duration": 20},
            {"rps": 2000, "duration": 20},
        ]

        for scenario in scenarios:
            logger.info(f"Testing end-to-end performance at {scenario['rps']} RPS")

            results = load_tester.end_to_end_load_test(
                topic="e2e_load_test",
                target_rps=scenario["rps"],
                duration_seconds=scenario["duration"],
            )

            producer_metrics = results["producer"]
            consumer_metrics = results["consumer"]

            # Performance assertions
            assert (
                producer_metrics.error_rate_percent < 1.0
            ), "Producer error rate too high"
            assert (
                consumer_metrics.error_rate_percent < 1.0
            ), "Consumer error rate too high"
            assert (
                consumer_metrics.average_latency_ms < 1000
            ), "End-to-end latency too high"

            # Throughput should be balanced
            throughput_ratio = (
                consumer_metrics.throughput_events_per_second
                / producer_metrics.throughput_events_per_second
            )
            assert (
                0.8 <= throughput_ratio <= 1.2
            ), f"Throughput imbalance: {throughput_ratio:.2f}"

            logger.info(f"E2E Performance {scenario['rps']} RPS:")
            logger.info(
                f"  Producer: {producer_metrics.throughput_events_per_second:.1f} events/sec"
            )
            logger.info(
                f"  Consumer: {consumer_metrics.throughput_events_per_second:.1f} events/sec"
            )
            logger.info(f"  Latency: {consumer_metrics.average_latency_ms:.1f}ms avg")

    def test_pipeline_scalability(self, pipeline_tester):
        """Test streaming pipeline scalability under increasing load."""
        # Scalability test scenarios
        scenarios = [
            {"rps": 100, "duration": 15},
            {"rps": 500, "duration": 15},
            {"rps": 1000, "duration": 15},
            {"rps": 2500, "duration": 15},
            {"rps": 5000, "duration": 15},
        ]

        results = pipeline_tester.pipeline_throughput_test(scenarios)

        # Analyze scalability
        successful_scenarios = [r for r in results.values() if r["success"]]
        assert (
            len(successful_scenarios) >= 3
        ), "Not enough scenarios completed successfully"

        # Check that performance scales reasonably
        throughputs = []
        for scenario_result in successful_scenarios:
            if "kafka_results" in scenario_result:
                producer_throughput = scenario_result["kafka_results"][
                    "producer"
                ].throughput_events_per_second
                throughputs.append(producer_throughput)

        # Should show general upward trend in throughput
        if len(throughputs) >= 3:
            # Simple trend check: later throughputs should generally be higher
            trend_positive = throughputs[-1] > throughputs[0]
            assert trend_positive, "Pipeline does not show scalability improvement"

        logger.info("Pipeline Scalability Results:")
        for scenario_name, result in results.items():
            if result["success"]:
                producer_metrics = result["kafka_results"]["producer"]
                logger.info(
                    f"  {scenario_name}: {producer_metrics.throughput_events_per_second:.1f} events/sec"
                )

    def test_performance_under_system_stress(self, load_tester):
        """Test performance under system resource stress."""
        # This test simulates high system load while running streaming tests
        import subprocess
        import threading

        def cpu_stress():
            # Light CPU stress (don't overwhelm the system)
            end_time = time.time() + 15
            while time.time() < end_time:
                # Busy work
                sum(range(1000))

        # Start background stress
        stress_threads = [threading.Thread(target=cpu_stress) for _ in range(2)]
        for thread in stress_threads:
            thread.start()

        try:
            # Run performance test under stress
            metrics = load_tester.end_to_end_load_test(
                topic="stress_test", target_rps=1000, duration_seconds=15
            )

            producer_metrics = metrics["producer"]
            consumer_metrics = metrics["consumer"]

            # Under stress, allow higher error rates but still functional
            assert (
                producer_metrics.error_rate_percent < 5.0
            ), "Producer error rate too high under stress"
            assert (
                consumer_metrics.error_rate_percent < 5.0
            ), "Consumer error rate too high under stress"
            assert (
                producer_metrics.throughput_events_per_second > 500
            ), "Throughput too low under stress"

            logger.info(f"Performance under stress:")
            logger.info(
                f"  Producer: {producer_metrics.throughput_events_per_second:.1f} events/sec, "
                f"CPU: {producer_metrics.cpu_usage_percent:.1f}%"
            )
            logger.info(
                f"  Consumer: {consumer_metrics.throughput_events_per_second:.1f} events/sec, "
                f"Latency: {consumer_metrics.average_latency_ms:.1f}ms"
            )

        finally:
            # Wait for stress threads to complete
            for thread in stress_threads:
                thread.join()


if __name__ == "__main__":
    # Example usage for standalone testing
    logging.basicConfig(level=logging.INFO)

    # This would typically use actual Kafka cluster
    # For demo purposes, using testcontainers
    print("Streaming Load Testing Framework")
    print("Run with pytest for full test suite:")
    print("pytest tests/performance/test_streaming_load.py -v --tb=short")

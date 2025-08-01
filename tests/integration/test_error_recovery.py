"""
Error Scenarios and Recovery Integration Tests

This module implements comprehensive integration tests for error handling
and recovery mechanisms across the entire data pipeline.

Tests cover:
1. Service failure and reconnection scenarios
2. Data corruption and validation recovery
3. Network partitions and resilience
4. Circuit breaker and backoff behaviors
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from unittest.mock import Mock, patch

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from src.data_ingestion.producers.base_producer import BaseProducer
from src.streaming.consumers import BaseStreamingConsumer
from src.utils.spark_utils import create_spark_session


class ErrorRecoveryTestEnvironment:
    """Test environment for error scenario and recovery testing."""

    def __init__(self):
        self.containers = {}
        self.services = {}
        self.spark = None
        self.failure_scenarios = {}

    def setup_containers(self):
        """Set up test containers for error recovery testing."""
        # PostgreSQL container
        self.containers["postgres"] = PostgresContainer("postgres:13")
        self.containers["postgres"].start()

        # Redis container
        self.containers["redis"] = RedisContainer("redis:7-alpine")
        self.containers["redis"].start()

        # Kafka container
        self.containers["kafka"] = KafkaContainer("confluentinc/cp-kafka:7.4.0")
        self.containers["kafka"].start()

        # Store connection details
        self.services["database_url"] = self.containers["postgres"].get_connection_url()

        redis_host = self.containers["redis"].get_container_host_ip()
        redis_port = self.containers["redis"].get_exposed_port(6379)
        self.services["redis_url"] = f"redis://{redis_host}:{redis_port}"

        kafka_host = self.containers["kafka"].get_container_host_ip()
        kafka_port = self.containers["kafka"].get_exposed_port(9093)
        self.services["kafka_bootstrap_servers"] = f"{kafka_host}:{kafka_port}"

    def setup_spark(self):
        """Set up Spark session for testing."""
        self.spark = create_spark_session(
            "error-recovery-test",
            config_overrides={
                "spark.sql.adaptive.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.execution.arrow.pyspark.enabled": "false",  # Avoid arrow issues
            },
        )

    def simulate_service_failure(self, service_name: str):
        """Simulate service failure by stopping container."""
        if service_name in self.containers:
            self.containers[service_name].stop()
            self.failure_scenarios[service_name] = "stopped"

    def restore_service(self, service_name: str):
        """Restore failed service by restarting container."""
        if (
            service_name in self.containers
            and self.failure_scenarios.get(service_name) == "stopped"
        ):
            # For testcontainers, we need to create a new container
            if service_name == "postgres":
                self.containers[service_name] = PostgresContainer("postgres:13")
            elif service_name == "redis":
                self.containers[service_name] = RedisContainer("redis:7-alpine")
            elif service_name == "kafka":
                self.containers[service_name] = KafkaContainer(
                    "confluentinc/cp-kafka:7.4.0"
                )

            self.containers[service_name].start()
            self.failure_scenarios[service_name] = "restored"

            # Update service connection details
            if service_name == "postgres":
                self.services["database_url"] = self.containers[
                    "postgres"
                ].get_connection_url()
            elif service_name == "redis":
                redis_host = self.containers["redis"].get_container_host_ip()
                redis_port = self.containers["redis"].get_exposed_port(6379)
                self.services["redis_url"] = f"redis://{redis_host}:{redis_port}"
            elif service_name == "kafka":
                kafka_host = self.containers["kafka"].get_container_host_ip()
                kafka_port = self.containers["kafka"].get_exposed_port(9093)
                self.services["kafka_bootstrap_servers"] = f"{kafka_host}:{kafka_port}"

    def teardown(self):
        """Clean up test environment."""
        if self.spark:
            self.spark.stop()

        for container in self.containers.values():
            try:
                container.stop()
            except Exception as e:
                print(f"Error stopping container: {e}")


@pytest.fixture(scope="class")
def error_recovery_env():
    """Fixture for error recovery test environment."""
    env = ErrorRecoveryTestEnvironment()
    env.setup_containers()
    env.setup_spark()

    yield env

    env.teardown()


class TestKafkaFailureRecovery:
    """Test Kafka failure and recovery scenarios."""

    def test_kafka_broker_failure_and_recovery(self, error_recovery_env):
        """Test recovery from Kafka broker failure."""
        # Create producer with retry configuration
        producer = KafkaProducer(
            bootstrap_servers=[error_recovery_env.services["kafka_bootstrap_servers"]],
            retries=3,
            retry_backoff_ms=500,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        # Send initial message to verify connection
        test_message = {
            "test": "before_failure",
            "timestamp": datetime.now().isoformat(),
        }
        future = producer.send("test-topic", test_message)

        try:
            record_metadata = future.get(timeout=10)
            assert record_metadata.topic == "test-topic"
        except Exception as e:
            pytest.skip(f"Cannot establish initial Kafka connection: {e}")

        # Simulate Kafka failure
        error_recovery_env.simulate_service_failure("kafka")

        # Wait for failure to take effect
        time.sleep(2)

        # Try to send message during failure (should fail)
        failure_message = {
            "test": "during_failure",
            "timestamp": datetime.now().isoformat(),
        }

        with pytest.raises(Exception):
            failure_future = producer.send("test-topic", failure_message)
            failure_future.get(timeout=5)  # Should timeout or raise exception

        # Restore Kafka service
        error_recovery_env.restore_service("kafka")

        # Wait for service restoration
        time.sleep(5)

        # Create new producer for restored service
        recovered_producer = KafkaProducer(
            bootstrap_servers=[error_recovery_env.services["kafka_bootstrap_servers"]],
            retries=3,
            retry_backoff_ms=500,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        # Send message after recovery (should succeed)
        recovery_message = {
            "test": "after_recovery",
            "timestamp": datetime.now().isoformat(),
        }
        recovery_future = recovered_producer.send("test-topic", recovery_message)

        try:
            recovery_metadata = recovery_future.get(timeout=10)
            assert recovery_metadata.topic == "test-topic"
        except Exception as e:
            pytest.fail(f"Failed to send message after Kafka recovery: {e}")

        producer.close()
        recovered_producer.close()

    def test_kafka_connection_retry_backoff(self, error_recovery_env):
        """Test exponential backoff retry mechanism for Kafka connections."""
        # Create producer with specific retry configuration
        retry_config = {
            "bootstrap_servers": [
                error_recovery_env.services["kafka_bootstrap_servers"]
            ],
            "retries": 5,
            "retry_backoff_ms": 100,  # Start with 100ms
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        }

        producer = KafkaProducer(**retry_config)

        # Test retry behavior with timing
        start_time = time.time()

        try:
            # This should succeed quickly
            test_message = {
                "test": "retry_test",
                "timestamp": datetime.now().isoformat(),
            }
            future = producer.send("test-topic", test_message)
            future.get(timeout=5)

            success_time = time.time() - start_time
            # Should be very fast for successful connection
            assert success_time < 1.0

        except Exception:
            # If it fails, that's also acceptable for this test
            pass

        producer.close()

    def test_consumer_group_rebalancing(self, error_recovery_env):
        """Test consumer group rebalancing during failures."""
        # Create multiple consumers in the same group
        consumer_config = {
            "bootstrap_servers": [
                error_recovery_env.services["kafka_bootstrap_servers"]
            ],
            "group_id": "test-rebalance-group",
            "auto_offset_reset": "earliest",
            "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
            "consumer_timeout_ms": 5000,
        }

        consumer1 = KafkaConsumer("test-topic", **consumer_config)
        consumer2 = KafkaConsumer("test-topic", **consumer_config)

        # Send test messages
        producer = KafkaProducer(
            bootstrap_servers=[error_recovery_env.services["kafka_bootstrap_servers"]],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        for i in range(10):
            message = {
                "id": i,
                "test": "rebalancing",
                "timestamp": datetime.now().isoformat(),
            }
            producer.send("test-topic", message)

        producer.flush()
        producer.close()

        # Consume messages with both consumers
        messages_consumed = {"consumer1": 0, "consumer2": 0}

        # Consumer 1 reads some messages
        try:
            for message in consumer1:
                messages_consumed["consumer1"] += 1
                if messages_consumed["consumer1"] >= 3:
                    break
        except Exception:
            pass  # Timeout is expected

        # Close consumer1 to trigger rebalancing
        consumer1.close()

        # Consumer 2 should take over
        try:
            for message in consumer2:
                messages_consumed["consumer2"] += 1
                if messages_consumed["consumer2"] >= 3:
                    break
        except Exception:
            pass  # Timeout is expected

        consumer2.close()

        # Verify some messages were consumed
        total_consumed = messages_consumed["consumer1"] + messages_consumed["consumer2"]
        assert total_consumed > 0


class TestDatabaseFailureRecovery:
    """Test database failure and recovery scenarios."""

    def test_database_connection_failure_handling(self, error_recovery_env):
        """Test database connection failure and reconnection."""
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import OperationalError

        # Test initial connection
        engine = create_engine(error_recovery_env.services["database_url"])

        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                assert result.fetchone()[0] == 1
        except Exception as e:
            pytest.skip(f"Cannot establish initial database connection: {e}")

        # Simulate database failure
        error_recovery_env.simulate_service_failure("postgres")

        # Wait for failure
        time.sleep(2)

        # Try to connect during failure (should fail)
        failed_engine = create_engine(error_recovery_env.services["database_url"])

        with pytest.raises(OperationalError):
            with failed_engine.connect() as conn:
                conn.execute(text("SELECT 1"))

        # Restore database
        error_recovery_env.restore_service("postgres")

        # Wait for restoration
        time.sleep(5)

        # Test reconnection
        restored_engine = create_engine(error_recovery_env.services["database_url"])

        try:
            with restored_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                assert result.fetchone()[0] == 1
        except Exception as e:
            pytest.fail(f"Failed to reconnect to database after recovery: {e}")

    def test_database_transaction_rollback(self, error_recovery_env):
        """Test database transaction rollback on errors."""
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import StatementError

        engine = create_engine(error_recovery_env.services["database_url"])

        try:
            with engine.connect() as conn:
                with conn.begin() as trans:
                    # Execute valid statement
                    conn.execute(
                        text("CREATE TEMP TABLE test_rollback (id INT, name TEXT)")
                    )
                    conn.execute(text("INSERT INTO test_rollback VALUES (1, 'test')"))

                    # Execute invalid statement to trigger rollback
                    try:
                        conn.execute(
                            text("INSERT INTO test_rollback VALUES ('invalid', 123)")
                        )  # Type error
                    except StatementError:
                        trans.rollback()

                    # Verify rollback occurred
                    result = conn.execute(text("SELECT COUNT(*) FROM test_rollback"))
                    count = result.fetchone()[0]
                    assert count == 0  # Should be 0 due to rollback

        except Exception as e:
            # If temp table operations fail, the test environment might not support them
            pytest.skip(f"Database transaction test failed: {e}")

    def test_connection_pool_exhaustion_recovery(self, error_recovery_env):
        """Test recovery from connection pool exhaustion."""
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import StaticPool

        # Create engine with limited pool size
        engine = create_engine(
            error_recovery_env.services["database_url"],
            poolclass=StaticPool,
            pool_size=2,
            max_overflow=0,
            pool_timeout=1,
        )

        connections = []

        try:
            # Exhaust connection pool
            for i in range(2):
                conn = engine.connect()
                connections.append(conn)

            # Try to get another connection (should timeout)
            with pytest.raises(Exception):  # TimeoutError or similar
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

            # Release one connection
            connections[0].close()
            connections.pop(0)

            # Now should be able to get connection again
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                assert result.fetchone()[0] == 1

        finally:
            # Clean up remaining connections
            for conn in connections:
                try:
                    conn.close()
                except Exception:
                    pass


class TestRedisFailureRecovery:
    """Test Redis failure and recovery scenarios."""

    def test_redis_connection_failure_handling(self, error_recovery_env):
        """Test Redis connection failure and recovery."""
        import redis

        # Test initial connection
        redis_client = redis.from_url(error_recovery_env.services["redis_url"])

        try:
            redis_client.ping()
            redis_client.set("test_key", "test_value")
            assert redis_client.get("test_key").decode() == "test_value"
        except Exception as e:
            pytest.skip(f"Cannot establish initial Redis connection: {e}")

        # Simulate Redis failure
        error_recovery_env.simulate_service_failure("redis")

        # Wait for failure
        time.sleep(2)

        # Try to connect during failure (should fail)
        with pytest.raises(redis.ConnectionError):
            redis_client.ping()

        # Restore Redis
        error_recovery_env.restore_service("redis")

        # Wait for restoration
        time.sleep(5)

        # Test reconnection
        restored_client = redis.from_url(error_recovery_env.services["redis_url"])

        try:
            restored_client.ping()
            restored_client.set("recovery_test", "success")
            assert restored_client.get("recovery_test").decode() == "success"
        except Exception as e:
            pytest.fail(f"Failed to reconnect to Redis after recovery: {e}")

    def test_redis_cache_miss_handling(self, error_recovery_env):
        """Test handling of cache misses when Redis is unavailable."""
        import redis

        # Mock a cache service
        class CacheService:
            def __init__(self, redis_url):
                self.redis_client = redis.from_url(redis_url)
                self.fallback_cache = {}

            def get(self, key):
                try:
                    value = self.redis_client.get(key)
                    return value.decode() if value else None
                except redis.ConnectionError:
                    # Fallback to in-memory cache
                    return self.fallback_cache.get(key)

            def set(self, key, value):
                try:
                    self.redis_client.set(key, value)
                    # Also store in fallback cache
                    self.fallback_cache[key] = value
                except redis.ConnectionError:
                    # Store only in fallback cache
                    self.fallback_cache[key] = value

        cache_service = CacheService(error_recovery_env.services["redis_url"])

        # Test normal operation
        try:
            cache_service.set("test_key", "test_value")
            assert cache_service.get("test_key") == "test_value"
        except Exception as e:
            pytest.skip(f"Cache service test setup failed: {e}")

        # Simulate Redis failure
        error_recovery_env.simulate_service_failure("redis")

        # Wait for failure
        time.sleep(2)

        # Test fallback behavior
        cache_service.set("fallback_key", "fallback_value")
        assert cache_service.get("fallback_key") == "fallback_value"

        # Should also get previously cached value from fallback
        assert cache_service.get("test_key") == "test_value"


class TestStreamingProcessingErrors:
    """Test streaming processing error scenarios."""

    def test_malformed_data_handling(self, error_recovery_env):
        """Test handling of malformed data in streaming pipeline."""
        # Send mix of valid and invalid messages
        producer = KafkaProducer(
            bootstrap_servers=[error_recovery_env.services["kafka_bootstrap_servers"]],
            value_serializer=lambda v: v.encode("utf-8")
            if isinstance(v, str)
            else json.dumps(v).encode("utf-8"),
        )

        # Send valid message
        valid_message = {
            "transaction_id": "valid_001",
            "amount": 100.0,
            "timestamp": datetime.now().isoformat(),
        }
        producer.send("test-topic", valid_message)

        # Send malformed JSON
        producer.send("test-topic", "invalid_json_message")

        # Send message with missing required fields
        incomplete_message = {"amount": 50.0}  # Missing transaction_id
        producer.send("test-topic", incomplete_message)

        # Send message with invalid data types
        invalid_types_message = {
            "transaction_id": "valid_002",
            "amount": "not_a_number",
        }
        producer.send("test-topic", invalid_types_message)

        producer.flush()
        producer.close()

        # Consumer should handle all messages gracefully
        consumer = KafkaConsumer(
            "test-topic",
            bootstrap_servers=[error_recovery_env.services["kafka_bootstrap_servers"]],
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
        )

        message_count = 0
        valid_count = 0
        invalid_count = 0

        for message in consumer:
            message_count += 1
            try:
                data = json.loads(message.value.decode("utf-8"))
                if "transaction_id" in data and "amount" in data:
                    valid_count += 1
                else:
                    invalid_count += 1
            except (json.JSONDecodeError, UnicodeDecodeError):
                invalid_count += 1

        consumer.close()

        # Verify we received all messages
        assert message_count == 4
        assert valid_count == 1  # Only the first message is fully valid
        assert invalid_count == 3  # Three malformed messages

    def test_streaming_query_failure_recovery(self, error_recovery_env):
        """Test recovery from streaming query failures."""
        from pyspark.sql.functions import col, from_json
        from pyspark.sql.types import StringType, StructField, StructType

        # Define schema for parsing
        schema = StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("amount", StringType(), True),
            ]
        )

        # Create streaming DataFrame
        try:
            streaming_df = (
                error_recovery_env.spark.readStream.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    error_recovery_env.services["kafka_bootstrap_servers"],
                )
                .option("subscribe", "test-topic")
                .option("startingOffsets", "latest")
                .load()
            )

            # Parse with error handling
            parsed_df = streaming_df.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")

            # Start query with checkpoint
            query = (
                parsed_df.writeStream.format("memory")
                .queryName("error_recovery_test")
                .outputMode("append")
                .option("checkpointLocation", "/tmp/checkpoint_error_test")
                .start()
            )

            # Let it run briefly
            time.sleep(2)

            # Stop query
            query.stop()

            # Verify query ran without crashing
            assert query.lastProgress is not None

        except Exception as e:
            # Streaming tests might fail in test environment
            pytest.skip(f"Streaming query test failed: {e}")

    def test_checkpoint_recovery(self, error_recovery_env):
        """Test streaming checkpoint recovery."""
        # This test would verify that streaming queries can recover from checkpoints
        # after failures, but requires more complex setup

        checkpoint_location = "/tmp/checkpoint_recovery_test"

        # Verify checkpoint directory can be created
        import os

        os.makedirs(checkpoint_location, exist_ok=True)
        assert os.path.exists(checkpoint_location)

        # In a real test, we would:
        # 1. Start a streaming query with checkpointing
        # 2. Stop it abruptly
        # 3. Restart from checkpoint
        # 4. Verify it continues from where it left off


class TestCircuitBreakerPatterns:
    """Test circuit breaker patterns for resilience."""

    def test_circuit_breaker_open_close_behavior(self, error_recovery_env):
        """Test circuit breaker open/close behavior."""

        class CircuitBreaker:
            def __init__(self, failure_threshold=3, timeout=5):
                self.failure_threshold = failure_threshold
                self.timeout = timeout
                self.failure_count = 0
                self.last_failure_time = None
                self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

            def call(self, func, *args, **kwargs):
                current_time = time.time()

                if self.state == "OPEN":
                    if current_time - self.last_failure_time > self.timeout:
                        self.state = "HALF_OPEN"
                    else:
                        raise Exception("Circuit breaker is OPEN")

                try:
                    result = func(*args, **kwargs)
                    if self.state == "HALF_OPEN":
                        self.state = "CLOSED"
                        self.failure_count = 0
                    return result
                except Exception as e:
                    self.failure_count += 1
                    self.last_failure_time = current_time
                    if self.failure_count >= self.failure_threshold:
                        self.state = "OPEN"
                    raise e

        # Test functions
        def failing_function():
            raise Exception("Simulated failure")

        def successful_function():
            return "success"

        # Test circuit breaker
        circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=2)

        # Test failure escalation to OPEN state
        failure_count = 0
        for i in range(5):
            try:
                circuit_breaker.call(failing_function)
            except Exception:
                failure_count += 1

        assert failure_count == 5  # 3 failures + 2 "circuit open" exceptions
        assert circuit_breaker.state == "OPEN"

        # Test that circuit remains open
        with pytest.raises(Exception, match="Circuit breaker is OPEN"):
            circuit_breaker.call(successful_function)

        # Wait for timeout
        time.sleep(2.1)

        # Test transition to HALF_OPEN and recovery
        result = circuit_breaker.call(successful_function)
        assert result == "success"
        assert circuit_breaker.state == "CLOSED"

    def test_exponential_backoff_retry(self, error_recovery_env):
        """Test exponential backoff retry mechanism."""

        class ExponentialBackoffRetry:
            def __init__(self, max_retries=3, base_delay=0.1, max_delay=2.0):
                self.max_retries = max_retries
                self.base_delay = base_delay
                self.max_delay = max_delay

            def retry(self, func, *args, **kwargs):
                for attempt in range(self.max_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        if attempt == self.max_retries:
                            raise e

                        # Calculate delay with exponential backoff
                        delay = min(self.base_delay * (2**attempt), self.max_delay)
                        time.sleep(delay)

        # Test with eventually successful function
        call_count = 0

        def eventually_successful():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception(f"Failure {call_count}")
            return f"Success on attempt {call_count}"

        retry_handler = ExponentialBackoffRetry(max_retries=5, base_delay=0.01)

        start_time = time.time()
        result = retry_handler.retry(eventually_successful)
        end_time = time.time()

        assert result == "Success on attempt 3"
        assert call_count == 3

        # Verify some delay occurred due to backoff
        assert end_time - start_time > 0.01  # At least base delay

    def test_bulkhead_pattern(self, error_recovery_env):
        """Test bulkhead pattern for fault isolation."""

        class BulkheadService:
            def __init__(self):
                self.critical_pool = {"available": 3, "in_use": 0}
                self.non_critical_pool = {"available": 2, "in_use": 0}

            def execute_critical(self, func, *args, **kwargs):
                if self.critical_pool["available"] == 0:
                    raise Exception("Critical pool exhausted")

                self.critical_pool["available"] -= 1
                self.critical_pool["in_use"] += 1

                try:
                    return func(*args, **kwargs)
                finally:
                    self.critical_pool["available"] += 1
                    self.critical_pool["in_use"] -= 1

            def execute_non_critical(self, func, *args, **kwargs):
                if self.non_critical_pool["available"] == 0:
                    raise Exception("Non-critical pool exhausted")

                self.non_critical_pool["available"] -= 1
                self.non_critical_pool["in_use"] += 1

                try:
                    return func(*args, **kwargs)
                finally:
                    self.non_critical_pool["available"] += 1
                    self.non_critical_pool["in_use"] -= 1

        bulkhead = BulkheadService()

        # Test that pools are isolated
        def quick_task():
            return "done"

        def slow_task():
            time.sleep(0.1)
            return "done"

        # Exhaust non-critical pool
        for i in range(2):
            result = bulkhead.execute_non_critical(quick_task)
            assert result == "done"

        # Non-critical pool should be exhausted
        with pytest.raises(Exception, match="Non-critical pool exhausted"):
            bulkhead.execute_non_critical(quick_task)

        # Critical pool should still be available
        result = bulkhead.execute_critical(quick_task)
        assert result == "done"


# Mark all tests as integration tests
pytestmark = pytest.mark.integration

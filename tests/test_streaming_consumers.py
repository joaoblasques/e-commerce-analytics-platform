"""
Tests for Kafka Structured Streaming consumers.

This module tests the streaming consumer functionality including
schema validation, error handling, and stream processing.
"""

import json
import time
import unittest
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.consumer_manager import (
    ConsumerHealthMonitor,
    StreamingConsumerManager,
)
from src.streaming.consumers import (
    BaseStreamingConsumer,
    DataQualityChecker,
    SchemaValidator,
    StreamingConsumerError,
    TransactionStreamConsumer,
    UserBehaviorStreamConsumer,
)
from src.utils.spark_utils import create_spark_session


class TestSchemaValidator(unittest.TestCase):
    """Test schema validation functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.schema_validator = SchemaValidator()

        self.expected_schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("amount", DoubleType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        )

        self.matching_schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("amount", DoubleType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        )

        self.different_schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("price", DoubleType(), False),  # Different field name
                StructField("timestamp", TimestampType(), False),
            ]
        )

    def test_validate_schema_success(self):
        """Test successful schema validation."""
        result = self.schema_validator.validate_schema(
            Mock(schema=self.matching_schema), self.expected_schema
        )
        self.assertTrue(result)

    def test_validate_schema_failure(self):
        """Test schema validation failure."""
        result = self.schema_validator.validate_schema(
            Mock(schema=self.different_schema), self.expected_schema
        )
        self.assertFalse(result)

    def test_get_schema_diff(self):
        """Test schema difference detection."""
        actual_schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("price", DoubleType(), False),  # Different field
                StructField("extra_field", StringType(), False),  # Extra field
                # Missing timestamp field
            ]
        )

        diff = self.schema_validator.get_schema_diff(
            actual_schema, self.expected_schema
        )

        self.assertIn("amount", diff["missing_fields"])
        self.assertIn("timestamp", diff["missing_fields"])
        self.assertIn("price", diff["extra_fields"])
        self.assertIn("extra_field", diff["extra_fields"])


class TestDataQualityChecker(unittest.TestCase):
    """Test data quality checking functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.logger = Mock()
        self.quality_checker = DataQualityChecker(self.logger)

    def test_check_nulls(self):
        """Test null value checking."""
        # Mock DataFrame with null checks
        mock_df = Mock()
        mock_df.columns = ["user_id", "amount"]
        mock_df.select.return_value.alias.return_value.collect.return_value = [
            {"null_count": 5}
        ]

        result = self.quality_checker.check_nulls(mock_df, ["user_id"])

        # Should log warning for null values
        self.logger.warning.assert_called()
        self.assertEqual(result, mock_df)

    def test_check_data_freshness(self):
        """Test data freshness checking."""
        mock_df = Mock()
        mock_df.columns = ["timestamp", "user_id"]
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.count.return_value = 3  # 3 stale records
        mock_df.drop.return_value = mock_df

        result = self.quality_checker.check_data_freshness(mock_df, "timestamp", 300)

        # Should log warning for stale data
        self.logger.warning.assert_called()
        self.assertEqual(result, mock_df)


class TestBaseStreamingConsumer(unittest.TestCase):
    """Test base streaming consumer functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock(spec=SparkSession)
        self.spark.conf = Mock()

        # Create a concrete implementation for testing
        class TestConsumer(BaseStreamingConsumer):
            def get_schema(self):
                return StructType(
                    [
                        StructField("user_id", StringType(), False),
                        StructField("amount", DoubleType(), False),
                    ]
                )

            def transform_stream(self, df):
                return df

        self.consumer = TestConsumer(
            spark=self.spark, topic="test-topic", consumer_group="test-group"
        )

    def test_init(self):
        """Test consumer initialization."""
        self.assertEqual(self.consumer.topic, "test-topic")
        self.assertEqual(self.consumer.consumer_group, "test-group")
        self.assertEqual(self.consumer.kafka_bootstrap_servers, "localhost:9092")
        self.assertTrue(self.consumer.enable_backpressure)

    def test_configure_spark_settings(self):
        """Test Spark configuration."""
        # Verify Spark configuration was called
        self.spark.conf.set.assert_called()

        # Check if backpressure settings were configured
        calls = self.spark.conf.set.call_args_list
        call_args = [call[0] for call in calls]

        # Should have configuration calls
        self.assertTrue(len(call_args) > 0)

    @patch("src.streaming.consumers.get_logger")
    def test_error_callback(self, mock_logger):
        """Test error callback functionality."""
        callback = Mock()
        self.consumer.set_error_callback(callback)

        error = Exception("Test error")
        self.consumer.error_callback(error)

        callback.assert_called_once_with(error)

    def test_get_critical_columns(self):
        """Test critical columns identification."""
        critical_columns = self.consumer._get_critical_columns()
        self.assertIn("timestamp", critical_columns)


class TestTransactionStreamConsumer(unittest.TestCase):
    """Test transaction stream consumer."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock(spec=SparkSession)
        self.spark.conf = Mock()
        self.consumer = TransactionStreamConsumer(spark=self.spark)

    def test_init_defaults(self):
        """Test consumer initialization with defaults."""
        self.assertEqual(self.consumer.topic, "transactions")
        self.assertEqual(self.consumer.consumer_group, "transaction-analytics-group")

    def test_schema(self):
        """Test transaction schema definition."""
        schema = self.consumer.get_schema()
        field_names = [field.name for field in schema.fields]

        required_fields = [
            "transaction_id",
            "user_id",
            "product_id",
            "price",
            "quantity",
            "timestamp",
            "user_location",
            "device_info",
        ]

        for field in required_fields:
            self.assertIn(field, field_names)

    def test_transform_stream(self):
        """Test stream transformation logic."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df

        result = self.consumer.transform_stream(mock_df)

        # Should apply transformations
        mock_df.withColumn.assert_called()
        mock_df.filter.assert_called()
        self.assertEqual(result, mock_df)

    def test_critical_columns(self):
        """Test critical columns for transactions."""
        critical_columns = self.consumer._get_critical_columns()
        expected_critical = [
            "transaction_id",
            "user_id",
            "product_id",
            "price",
            "quantity",
            "timestamp",
        ]

        for field in expected_critical:
            self.assertIn(field, critical_columns)


class TestUserBehaviorStreamConsumer(unittest.TestCase):
    """Test user behavior stream consumer."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock(spec=SparkSession)
        self.spark.conf = Mock()
        self.consumer = UserBehaviorStreamConsumer(spark=self.spark)

    def test_init_defaults(self):
        """Test consumer initialization with defaults."""
        self.assertEqual(self.consumer.topic, "user-events")
        self.assertEqual(self.consumer.consumer_group, "user-behavior-analytics-group")

    def test_schema(self):
        """Test user behavior schema definition."""
        schema = self.consumer.get_schema()
        field_names = [field.name for field in schema.fields]

        required_fields = [
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "timestamp",
            "user_location",
            "device_info",
        ]

        for field in required_fields:
            self.assertIn(field, field_names)

    def test_transform_stream(self):
        """Test stream transformation logic."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df

        result = self.consumer.transform_stream(mock_df)

        # Should apply transformations
        mock_df.withColumn.assert_called()
        mock_df.filter.assert_called()
        self.assertEqual(result, mock_df)

    def test_critical_columns(self):
        """Test critical columns for user behavior."""
        critical_columns = self.consumer._get_critical_columns()
        expected_critical = [
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "timestamp",
        ]

        for field in expected_critical:
            self.assertIn(field, critical_columns)


class TestConsumerHealthMonitor(unittest.TestCase):
    """Test consumer health monitoring."""

    def setUp(self):
        """Set up test fixtures."""
        self.monitor = ConsumerHealthMonitor(check_interval=1)
        self.mock_consumer = Mock(spec=BaseStreamingConsumer)

    def test_register_consumer(self):
        """Test consumer registration."""
        self.monitor.register_consumer("test_consumer", self.mock_consumer)

        self.assertIn("test_consumer", self.monitor.consumers)
        self.assertIn("test_consumer", self.monitor.health_status)
        self.assertEqual(
            self.monitor.health_status["test_consumer"]["status"], "registered"
        )

    def test_unregister_consumer(self):
        """Test consumer unregistration."""
        self.monitor.register_consumer("test_consumer", self.mock_consumer)
        self.monitor.unregister_consumer("test_consumer")

        self.assertNotIn("test_consumer", self.monitor.consumers)
        self.assertNotIn("test_consumer", self.monitor.health_status)

    def test_health_callback(self):
        """Test health status callbacks."""
        callback = Mock()
        self.monitor.add_health_callback(callback)

        self.assertIn(callback, self.monitor.health_callbacks)

    def test_check_consumer_health_healthy(self):
        """Test health check for healthy consumer."""
        self.mock_consumer.get_stream_status.return_value = {
            "status": "active",
            "input_rows_per_second": 100.0,
            "processed_rows_per_second": 95.0,
            "batch_duration": 1000,
        }

        health_data = self.monitor._check_consumer_health(
            "test_consumer", self.mock_consumer
        )

        self.assertTrue(health_data["is_healthy"])
        self.assertEqual(len(health_data["errors"]), 0)
        self.assertIn("metrics", health_data)

    def test_check_consumer_health_unhealthy(self):
        """Test health check for unhealthy consumer."""
        self.mock_consumer.get_stream_status.return_value = {
            "status": "stopped",
            "input_rows_per_second": 100.0,
            "processed_rows_per_second": 10.0,  # Processing lag
            "batch_duration": 15000,  # High batch duration
        }

        health_data = self.monitor._check_consumer_health(
            "test_consumer", self.mock_consumer
        )

        self.assertFalse(health_data["is_healthy"])
        self.assertTrue(len(health_data["errors"]) > 0)

    def test_get_health_summary(self):
        """Test health summary generation."""
        # Register consumers with different health states
        self.monitor.register_consumer("healthy_consumer", Mock())
        self.monitor.register_consumer("unhealthy_consumer", Mock())

        self.monitor.health_status["healthy_consumer"]["status"] = "healthy"
        self.monitor.health_status["unhealthy_consumer"]["status"] = "unhealthy"

        summary = self.monitor.get_health_summary()

        self.assertEqual(summary["total_consumers"], 2)
        self.assertEqual(summary["healthy_consumers"], 1)
        self.assertEqual(summary["unhealthy_consumers"], 1)
        self.assertEqual(summary["overall_status"], "degraded")


class TestStreamingConsumerManager(unittest.TestCase):
    """Test streaming consumer manager."""

    def setUp(self):
        """Set up test fixtures."""
        self.spark = Mock(spec=SparkSession)
        self.manager = StreamingConsumerManager(
            spark=self.spark, max_concurrent_consumers=2
        )

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self.manager, "executor"):
            self.manager.executor.shutdown(wait=False)

    def test_init(self):
        """Test manager initialization."""
        self.assertEqual(self.manager.spark, self.spark)
        self.assertEqual(self.manager.max_concurrent_consumers, 2)
        self.assertEqual(len(self.manager.consumers), 0)

    def test_register_consumer(self):
        """Test consumer registration."""
        self.manager.register_consumer(
            name="test_consumer",
            consumer_class=TransactionStreamConsumer,
            topic="test-topic",
            consumer_group="test-group",
        )

        self.assertIn("test_consumer", self.manager.consumers)
        consumer = self.manager.consumers["test_consumer"]
        self.assertEqual(consumer.topic, "test-topic")
        self.assertEqual(consumer.consumer_group, "test-group")

    def test_register_duplicate_consumer(self):
        """Test registering duplicate consumer names."""
        self.manager.register_consumer(
            name="test_consumer",
            consumer_class=TransactionStreamConsumer,
            topic="test-topic",
            consumer_group="test-group",
        )

        with self.assertRaises(ValueError):
            self.manager.register_consumer(
                name="test_consumer",  # Duplicate name
                consumer_class=UserBehaviorStreamConsumer,
                topic="another-topic",
                consumer_group="another-group",
            )

    def test_get_consumer_status_not_registered(self):
        """Test getting status of non-existent consumer."""
        status = self.manager.get_consumer_status("non_existent")
        self.assertIn("error", status)

    def test_get_all_consumer_status(self):
        """Test getting status of all consumers."""
        # Register a consumer
        self.manager.register_consumer(
            name="test_consumer",
            consumer_class=TransactionStreamConsumer,
            topic="test-topic",
            consumer_group="test-group",
        )

        status = self.manager.get_all_consumer_status()

        self.assertIn("consumers", status)
        self.assertIn("health_summary", status)
        self.assertIn("manager_status", status)
        self.assertIn("test_consumer", status["consumers"])


class TestStreamingConsumerIntegration(unittest.TestCase):
    """Integration tests for streaming consumers."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for integration tests."""
        try:
            cls.spark = create_spark_session(
                app_name="StreamingConsumerTests",
                master="local[2]",
                enable_hive_support=False,
                additional_configs={
                    "spark.sql.shuffle.partitions": "2",
                    "spark.sql.streaming.checkpointLocation": "/tmp/test_checkpoints",
                },
            )
        except Exception:
            # If Spark is not available, skip integration tests
            cls.spark = None

    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session."""
        if cls.spark:
            cls.spark.stop()

    def setUp(self):
        """Set up test fixtures."""
        if not self.spark:
            self.skipTest("Spark not available for integration tests")

    def test_transaction_consumer_creation(self):
        """Test creating a transaction consumer with real Spark session."""
        consumer = TransactionStreamConsumer(
            spark=self.spark,
            kafka_bootstrap_servers="localhost:9092",
            topic="test-transactions",
            consumer_group="test-group",
        )

        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.topic, "test-transactions")
        self.assertEqual(consumer.consumer_group, "test-group")

        # Test schema
        schema = consumer.get_schema()
        self.assertIsInstance(schema, StructType)
        self.assertTrue(len(schema.fields) > 0)

    def test_user_behavior_consumer_creation(self):
        """Test creating a user behavior consumer with real Spark session."""
        consumer = UserBehaviorStreamConsumer(
            spark=self.spark,
            kafka_bootstrap_servers="localhost:9092",
            topic="test-user-events",
            consumer_group="test-group",
        )

        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.topic, "test-user-events")
        self.assertEqual(consumer.consumer_group, "test-group")

        # Test schema
        schema = consumer.get_schema()
        self.assertIsInstance(schema, StructType)
        self.assertTrue(len(schema.fields) > 0)

    def test_consumer_manager_with_real_spark(self):
        """Test consumer manager with real Spark session."""
        manager = StreamingConsumerManager(
            spark=self.spark,
            kafka_bootstrap_servers="localhost:9092",
            max_concurrent_consumers=1,
        )

        # Register a consumer
        manager.register_consumer(
            name="test_transaction_consumer",
            consumer_class=TransactionStreamConsumer,
            topic="test-transactions",
            consumer_group="test-transaction-group",
        )

        # Check registration
        self.assertEqual(len(manager.consumers), 1)
        self.assertIn("test_transaction_consumer", manager.consumers)

        # Get status
        status = manager.get_all_consumer_status()
        self.assertEqual(status["manager_status"]["registered_consumers"], 1)

        # Clean up
        manager.stop_all_consumers()


if __name__ == "__main__":
    # Run tests
    unittest.main(verbosity=2)

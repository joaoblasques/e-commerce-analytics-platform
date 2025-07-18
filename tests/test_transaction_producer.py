"""Tests for the transaction data producer."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from src.data_ingestion.monitoring import ProducerMetrics
from src.data_ingestion.producers.transaction_producer import TransactionProducer


class TestTransactionProducer:
    """Test cases for TransactionProducer."""

    @pytest.fixture
    def producer(self):
        """Create a test producer instance."""
        with patch("src.data_ingestion.producers.base_producer.KafkaProducer"):
            producer = TransactionProducer(
                bootstrap_servers="localhost:9092",
                topic="test_transactions",
                generation_rate=3600.0,  # 1 message per second
            )
            return producer

    def test_producer_initialization(self, producer):
        """Test producer initialization."""
        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic == "test_transactions"
        assert producer.generation_rate == 3600.0
        assert producer.fake is not None
        assert len(producer.categories) > 0
        assert len(producer.payment_methods) > 0

    def test_generate_message_structure(self, producer):
        """Test that generated messages have the correct structure."""
        message = producer.generate_message()

        # Check required fields
        required_fields = [
            "transaction_id",
            "user_id",
            "session_id",
            "product_id",
            "product_name",
            "category_id",
            "category_name",
            "brand",
            "price",
            "quantity",
            "discount_applied",
            "payment_method",
            "payment_provider",
            "currency",
            "timestamp",
        ]

        for field in required_fields:
            assert field in message, f"Missing required field: {field}"

        # Check nested structures
        assert "user_location" in message
        assert "country" in message["user_location"]
        assert "device_info" in message
        assert "type" in message["device_info"]
        assert "marketing_attribution" in message
        assert "campaign_id" in message["marketing_attribution"]

    def test_message_data_types(self, producer):
        """Test that message fields have correct data types."""
        message = producer.generate_message()

        # String fields
        assert isinstance(message["transaction_id"], str)
        assert isinstance(message["user_id"], str)
        assert isinstance(message["product_name"], str)
        assert isinstance(message["currency"], str)

        # Numeric fields
        assert isinstance(message["price"], float)
        assert isinstance(message["quantity"], int)
        assert isinstance(message["discount_applied"], float)

        # Check price is positive
        assert message["price"] > 0
        assert message["quantity"] > 0
        assert message["discount_applied"] >= 0

        # Check timestamp format
        timestamp = message["timestamp"]
        assert timestamp.endswith("Z")
        # Should be parseable as ISO format
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

    def test_message_key_generation(self, producer):
        """Test message key generation for partitioning."""
        message = producer.generate_message()
        key = producer.get_message_key(message)

        assert key == message["user_id"]
        assert isinstance(key, str)
        assert len(key) > 0

    def test_realistic_transaction_patterns(self, producer):
        """Test that generated data follows realistic patterns."""
        messages = [producer.generate_message() for _ in range(100)]

        # Test category distribution
        categories = [msg["category_id"] for msg in messages]
        unique_categories = set(categories)
        assert len(unique_categories) > 1, "Should have multiple categories"

        # Test price ranges are reasonable
        prices = [msg["price"] for msg in messages]
        assert min(prices) >= 1.0, "Minimum price should be reasonable"
        assert max(prices) <= 5000.0, "Maximum price should be reasonable"

        # Test discount patterns
        discounts = [msg["discount_applied"] for msg in messages]
        no_discount_count = sum(1 for d in discounts if d == 0.0)
        assert no_discount_count > 50, "Most transactions should have no discount"

        # Test quantity patterns
        quantities = [msg["quantity"] for msg in messages]
        assert all(q >= 1 for q in quantities), "All quantities should be >= 1"
        assert all(q <= 5 for q in quantities), "All quantities should be <= 5"

        # Test payment method distribution
        payment_methods = [msg["payment_method"] for msg in messages]
        unique_payment_methods = set(payment_methods)
        assert len(unique_payment_methods) > 1, "Should have multiple payment methods"

    def test_time_multiplier_calculation(self, producer):
        """Test time-based generation rate multipliers."""
        # Test business hours
        business_hour = datetime(2024, 1, 15, 14, 30, 0)  # Monday 2:30 PM
        multiplier = producer._get_time_multiplier(business_hour)
        assert multiplier > 1.0, "Business hours should increase rate"

        # Test late night
        late_night = datetime(2024, 1, 15, 2, 30, 0)  # Monday 2:30 AM
        multiplier = producer._get_time_multiplier(late_night)
        assert multiplier < 1.0, "Late night should decrease rate"

        # Test weekend
        weekend = datetime(2024, 1, 13, 14, 30, 0)  # Saturday 2:30 PM
        multiplier = producer._get_time_multiplier(weekend)
        assert multiplier > 1.0, "Weekend should increase rate"

    def test_delay_calculation(self, producer):
        """Test delay calculation for generation rate."""
        # Test with different rates
        producer.generation_rate = 3600.0  # 1 per second
        delay = producer.get_delay_seconds()
        assert 0.5 < delay < 1.5, "Delay should be approximately 1 second"

        producer.generation_rate = 7200.0  # 2 per second
        delay = producer.get_delay_seconds()
        assert 0.25 < delay < 0.75, "Delay should be approximately 0.5 seconds"

    def test_weighted_choice(self, producer):
        """Test weighted random choice functionality."""
        choices = [
            {"value": "A", "weight": 0.8},
            {"value": "B", "weight": 0.2},
        ]

        # Test multiple choices to verify weighting
        results = [producer._weighted_choice(choices)["value"] for _ in range(100)]
        a_count = results.count("A")
        b_count = results.count("B")

        # A should appear more often than B
        assert a_count > b_count, "Higher weighted choice should appear more often"

    @patch("src.data_ingestion.producers.base_producer.KafkaProducer")
    def test_send_message_success(self, mock_kafka_producer):
        """Test successful message sending."""
        # Mock the producer
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance

        # Mock successful send
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_record_metadata.topic = "test_transactions"
        mock_record_metadata.partition = 0
        mock_record_metadata.offset = 123
        mock_record_metadata.serialized_value_size = 1024
        mock_future.get.return_value = mock_record_metadata
        mock_producer_instance.send.return_value = mock_future

        # Create producer and send message
        producer = TransactionProducer(topic="test_transactions")
        message = {"test": "data"}
        result = producer.send_message(message, "test_key")

        assert result is True
        mock_producer_instance.send.assert_called_once_with(
            "test_transactions", value=message, key="test_key"
        )

        # Check metrics were updated
        metrics = producer.get_metrics()
        assert metrics["messages_sent"] == 1
        assert metrics["messages_failed"] == 0
        assert metrics["bytes_sent"] == 1024


class TestProducerMetrics:
    """Test cases for ProducerMetrics."""

    def test_metrics_initialization(self):
        """Test metrics initialization."""
        metrics = ProducerMetrics("test_producer")

        assert metrics.name == "test_producer"
        assert metrics.messages_sent == 0
        assert metrics.messages_failed == 0
        assert metrics.bytes_sent == 0
        assert metrics.error_count == 0
        assert metrics.last_error is None
        assert metrics.start_time > 0

    def test_add_success(self):
        """Test adding successful message metrics."""
        metrics = ProducerMetrics("test_producer")

        metrics.add_success(1024)

        assert metrics.messages_sent == 1
        assert metrics.bytes_sent == 1024
        assert metrics.last_send_time is not None

    def test_add_failure(self):
        """Test adding failure metrics."""
        metrics = ProducerMetrics("test_producer")

        metrics.add_failure("Connection error")

        assert metrics.messages_failed == 1
        assert metrics.error_count == 1
        assert metrics.last_error == "Connection error"

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        metrics = ProducerMetrics("test_producer")

        # No messages yet
        assert metrics.get_success_rate() == 0.0

        # Add some successful and failed messages
        metrics.add_success(100)
        metrics.add_success(200)
        metrics.add_failure("Error")

        # 2 successful out of 3 total = 66.67%
        success_rate = metrics.get_success_rate()
        assert abs(success_rate - 66.67) < 0.01

    def test_metrics_to_dict(self):
        """Test metrics dictionary conversion."""
        metrics = ProducerMetrics("test_producer")
        metrics.add_success(1024)

        metrics_dict = metrics.to_dict()

        required_keys = [
            "name",
            "messages_sent",
            "messages_failed",
            "bytes_sent",
            "uptime_seconds",
            "rate_per_second",
            "success_rate_percent",
            "last_send_time",
            "error_count",
            "last_error",
        ]

        for key in required_keys:
            assert key in metrics_dict

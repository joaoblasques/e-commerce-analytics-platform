"""Tests for user behavior event producer."""

import json
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from src.data_ingestion.producers.user_behavior_producer import UserBehaviorProducer


class TestUserBehaviorProducer:
    """Test suite for UserBehaviorProducer."""

    @pytest.fixture
    def producer(self):
        """Create a user behavior producer instance for testing."""
        with patch(
            "src.data_ingestion.producers.base_producer.KafkaProducer"
        ) as mock_kafka:
            # Mock the Kafka producer
            mock_kafka.return_value = Mock()
            return UserBehaviorProducer(
                bootstrap_servers="localhost:9092",
                topic="test-user-events",
                generation_rate=1000.0,
            )

    def test_initialization(self, producer):
        """Test producer initialization."""
        assert producer.topic == "test-user-events"
        assert producer.generation_rate == 1000.0
        assert producer.session_duration_range == (60, 1800)
        assert len(producer.active_sessions) == 0
        assert producer.business_hours == (9, 17)
        assert producer.peak_hours == (19, 22)
        assert producer.weekend_multiplier == 1.8

    def test_data_pools_initialization(self, producer):
        """Test that data pools are properly initialized."""
        assert len(producer.event_types) == 14
        assert len(producer.page_categories) == 7
        assert len(producer.user_segments) == 4
        assert len(producer.devices) == 5
        assert len(producer.locations) == 7
        assert len(producer.referrers) == 6
        assert len(producer.product_categories) == 7
        assert len(producer.journey_transitions) == 5

    def test_event_types_weights(self, producer):
        """Test that event types have appropriate weights."""
        total_weight = sum(event["weight"] for event in producer.event_types)
        assert abs(total_weight - 1.0) < 0.1  # Should sum to approximately 1.0

        # Check most common event types
        event_weights = {
            event["type"]: event["weight"] for event in producer.event_types
        }
        assert event_weights["page_view"] == 0.40  # Most common
        assert event_weights["product_view"] == 0.25  # Second most common
        assert event_weights["complete_checkout"] == 0.02  # Least common

    def test_weighted_choice(self, producer):
        """Test weighted random choice functionality."""
        choices = [
            {"item": "A", "weight": 0.8},
            {"item": "B", "weight": 0.2},
        ]

        results = []
        for _ in range(100):
            choice = producer._weighted_choice(choices)
            results.append(choice["item"])

        # A should be chosen more often than B
        count_a = results.count("A")
        count_b = results.count("B")
        assert count_a > count_b

    def test_time_multiplier_business_hours(self, producer):
        """Test time multiplier during business hours."""
        # Business hours (9-17)
        business_time = datetime(2024, 1, 15, 14, 0, 0)  # Monday 2 PM
        multiplier = producer._get_time_multiplier(business_time)
        assert multiplier >= 1.0  # Should be boosted

        # Non-business hours
        night_time = datetime(2024, 1, 15, 2, 0, 0)  # Monday 2 AM
        multiplier = producer._get_time_multiplier(night_time)
        assert multiplier < 1.0  # Should be reduced

    def test_time_multiplier_peak_hours(self, producer):
        """Test time multiplier during peak hours."""
        # Peak hours (19-22)
        peak_time = datetime(2024, 1, 15, 20, 0, 0)  # Monday 8 PM
        multiplier = producer._get_time_multiplier(peak_time)
        assert multiplier >= 2.0  # Should have peak boost

    def test_time_multiplier_weekend(self, producer):
        """Test time multiplier during weekend."""
        # Weekend (Saturday)
        weekend_time = datetime(2024, 1, 13, 14, 0, 0)  # Saturday 2 PM
        multiplier = producer._get_time_multiplier(weekend_time)
        assert multiplier >= 1.5  # Should have weekend boost

    def test_session_creation(self, producer):
        """Test session creation functionality."""
        timestamp = datetime.utcnow()
        user_id = "test_user_123"

        session = producer._create_session(user_id, timestamp)

        assert session["user_id"] == user_id
        assert session["session_id"].startswith("sess_")
        assert session["user_segment"] in [
            "new_visitor",
            "returning_visitor",
            "loyal_customer",
            "power_user",
        ]
        assert session["session_start"] == timestamp
        assert session["session_end"] > timestamp
        assert session["events_count"] == 0
        assert session["last_event_type"] is None
        assert isinstance(session["cart_items"], list)
        assert isinstance(session["page_views"], list)
        assert "device_info" in session
        assert "location" in session
        assert "referrer" in session

    def test_session_cleanup(self, producer):
        """Test expired session cleanup."""
        timestamp = datetime.utcnow()

        # Create a session that should expire
        expired_session = producer._create_session(
            "user_1", timestamp - timedelta(hours=1)
        )
        expired_session["session_end"] = timestamp - timedelta(minutes=30)
        producer.active_sessions["expired_session"] = expired_session

        # Create a session that should not expire
        active_session = producer._create_session("user_2", timestamp)
        active_session["session_end"] = timestamp + timedelta(minutes=30)
        producer.active_sessions["active_session"] = active_session

        # Run cleanup
        producer._cleanup_expired_sessions()

        # Only active session should remain
        assert len(producer.active_sessions) == 1
        assert "active_session" in producer.active_sessions
        assert "expired_session" not in producer.active_sessions

    def test_journey_transitions(self, producer):
        """Test user journey transition patterns."""
        # Test transition from page_view
        next_event = producer._get_next_event_type("page_view")
        assert next_event in [
            "page_view",
            "product_view",
            "search",
            "add_to_cart",
            "login",
            "logout",
        ]

        # Test transition from product_view
        next_event = producer._get_next_event_type("product_view")
        assert next_event in [
            "product_view",
            "add_to_cart",
            "wishlist_add",
            "page_view",
            "begin_checkout",
            "logout",
        ]

        # Test transition from unknown event type
        next_event = producer._get_next_event_type("unknown_event")
        assert next_event in [event["type"] for event in producer.event_types]

    def test_page_info_generation(self, producer):
        """Test page information generation for different event types."""
        timestamp = datetime.utcnow()
        session = producer._create_session("test_user", timestamp)

        # Test product view
        page_info = producer._generate_page_info("product_view", session)
        assert page_info["page_category"] == "product"
        assert "product_id" in page_info
        assert "product_category" in page_info
        assert page_info["page_url"].startswith("/product/")

        # Test search
        page_info = producer._generate_page_info("search", session)
        assert page_info["page_category"] == "search"
        assert "search_term" in page_info
        assert "search_results_count" in page_info
        assert page_info["page_url"].startswith("/search?q=")

        # Test add to cart
        page_info = producer._generate_page_info("add_to_cart", session)
        assert page_info["page_category"] == "product"
        assert "product_id" in page_info
        assert "quantity" in page_info
        assert "price" in page_info

        # Test checkout
        page_info = producer._generate_page_info("begin_checkout", session)
        assert page_info["page_category"] == "checkout"
        assert "cart_value" in page_info
        assert "cart_items_count" in page_info

        # Test complete checkout
        page_info = producer._generate_page_info("complete_checkout", session)
        assert page_info["page_category"] == "checkout"
        assert "order_id" in page_info
        assert "order_value" in page_info
        assert "payment_method" in page_info

    def test_message_generation(self, producer):
        """Test user behavior event message generation."""
        message = producer.generate_message()

        # Check required fields
        assert "event_id" in message
        assert "event_type" in message
        assert "timestamp" in message
        assert "session_id" in message
        assert "user_id" in message
        assert "user_segment" in message
        assert "page_info" in message
        assert "event_data" in message
        assert "device_info" in message
        assert "location" in message
        assert "referrer" in message
        assert "session_info" in message
        assert "technical_info" in message

    def test_message_structure_validation(self, producer):
        """Test that generated messages have correct structure."""
        message = producer.generate_message()

        # Validate event_id format
        assert message["event_id"].startswith("evt_")

        # Validate timestamp format
        timestamp = datetime.fromisoformat(message["timestamp"].replace("Z", "+00:00"))
        assert isinstance(timestamp, datetime)

        # Validate session_id format
        assert message["session_id"].startswith("sess_")

        # Validate user_id format
        assert message["user_id"].startswith("user_")

        # Validate user_segment
        assert message["user_segment"] in [
            "new_visitor",
            "returning_visitor",
            "loyal_customer",
            "power_user",
        ]

        # Validate page_info structure
        page_info = message["page_info"]
        assert "page_url" in page_info
        assert "page_title" in page_info
        assert "page_category" in page_info

        # Validate device_info structure
        device_info = message["device_info"]
        assert "type" in device_info
        assert "os" in device_info
        assert "browser" in device_info
        assert "user_agent" in device_info
        assert "screen_resolution" in device_info
        assert "viewport_size" in device_info

        # Validate location structure
        location = message["location"]
        assert "country" in location
        assert "state" in location
        assert "city" in location
        assert "timezone" in location
        assert "latitude" in location
        assert "longitude" in location

        # Validate session_info structure
        session_info = message["session_info"]
        assert "session_start" in session_info
        assert "events_in_session" in session_info
        assert "session_duration_so_far" in session_info
        assert "is_new_session" in session_info

    def test_message_key_generation(self, producer):
        """Test message key generation for partitioning."""
        message = producer.generate_message()
        key = producer.get_message_key(message)

        assert key == message["session_id"]
        assert key.startswith("sess_")

    def test_delay_calculation(self, producer):
        """Test delay calculation based on generation rate."""
        # Test with different generation rates
        producer.generation_rate = 3600.0  # 1 event per second
        delay = producer.get_delay_seconds()
        assert 0.5 < delay < 2.0  # Should be around 1 second with randomness

        producer.generation_rate = 1800.0  # 0.5 events per second
        delay = producer.get_delay_seconds()
        assert 1.0 < delay < 4.0  # Should be around 2 seconds with randomness

    def test_session_correlation(self, producer):
        """Test that events from the same session are correlated."""
        # Generate multiple messages and check session correlation
        messages = []
        for _ in range(5):
            message = producer.generate_message()
            messages.append(message)

        # Check that we have sessions
        session_ids = [msg["session_id"] for msg in messages]
        assert len(set(session_ids)) >= 1  # Should have at least one session

        # Check that messages from same session have same user_id
        sessions = {}
        for msg in messages:
            session_id = msg["session_id"]
            if session_id not in sessions:
                sessions[session_id] = {"user_id": msg["user_id"], "messages": []}
            sessions[session_id]["messages"].append(msg)

            # All messages from same session should have same user_id
            assert msg["user_id"] == sessions[session_id]["user_id"]

    def test_event_data_specificity(self, producer):
        """Test that event-specific data is included appropriately."""
        # Test search event
        with patch.object(producer, "_get_next_event_type", return_value="search"):
            message = producer.generate_message()
            assert message["event_type"] == "search"
            assert "search_term" in message["event_data"]
            assert "search_results_count" in message["event_data"]
            assert "search_duration_ms" in message["event_data"]

        # Test add_to_cart event
        with patch.object(producer, "_get_next_event_type", return_value="add_to_cart"):
            message = producer.generate_message()
            assert message["event_type"] == "add_to_cart"
            assert "product_id" in message["event_data"]
            assert "quantity" in message["event_data"]
            assert "price" in message["event_data"]

        # Test complete_checkout event
        with patch.object(
            producer, "_get_next_event_type", return_value="complete_checkout"
        ):
            message = producer.generate_message()
            assert message["event_type"] == "complete_checkout"
            assert "order_id" in message["event_data"]
            assert "order_value" in message["event_data"]
            assert "payment_method" in message["event_data"]

    def test_json_serialization(self, producer):
        """Test that generated messages can be serialized to JSON."""
        message = producer.generate_message()

        # Should be able to serialize to JSON without errors
        json_str = json.dumps(message)
        assert len(json_str) > 0

        # Should be able to deserialize back
        deserialized = json.loads(json_str)
        assert deserialized["event_id"] == message["event_id"]
        assert deserialized["event_type"] == message["event_type"]

    @patch("src.data_ingestion.producers.base_producer.KafkaProducer")
    @patch(
        "src.data_ingestion.producers.user_behavior_producer.UserBehaviorProducer.send_message"
    )
    def test_continuous_run_duration(self, mock_send, mock_kafka, producer):
        """Test continuous run with duration limit."""
        mock_send.return_value = True

        # Run for 1 second
        start_time = time.time()
        producer.run_continuous(duration_seconds=1)
        end_time = time.time()

        # Should run for approximately 1 second
        assert end_time - start_time >= 1.0
        assert end_time - start_time < 5.0  # Allow more tolerance for CI environment

        # Should have sent some messages
        assert mock_send.call_count > 0

    def test_realistic_user_segments(self, producer):
        """Test that user segments have realistic characteristics."""
        # Generate multiple sessions and check segment distribution
        sessions = []
        for _ in range(100):
            timestamp = datetime.utcnow()
            user_id = f"user_{_}"
            session = producer._create_session(user_id, timestamp)
            sessions.append(session)

        # Check segment distribution
        segments = [s["user_segment"] for s in sessions]
        segment_counts = {
            "new_visitor": segments.count("new_visitor"),
            "returning_visitor": segments.count("returning_visitor"),
            "loyal_customer": segments.count("loyal_customer"),
            "power_user": segments.count("power_user"),
        }

        # new_visitor and returning_visitor should be most common
        assert segment_counts["new_visitor"] > segment_counts["power_user"]
        assert segment_counts["returning_visitor"] > segment_counts["power_user"]

    def test_device_and_location_simulation(self, producer):
        """Test device and location simulation accuracy."""
        message = producer.generate_message()

        # Device info validation
        device_info = message["device_info"]
        assert device_info["type"] in ["mobile", "desktop", "tablet"]
        assert device_info["os"] in ["iOS", "Android", "Windows", "macOS", "iPadOS"]
        assert device_info["browser"] in ["Safari", "Chrome"]
        assert "user_agent" in device_info
        assert "screen_resolution" in device_info
        assert "viewport_size" in device_info

        # Location validation
        location = message["location"]
        assert location["country"] in ["US", "CA", "GB", "DE", "AU"]
        assert isinstance(location["latitude"], float)
        assert isinstance(location["longitude"], float)
        assert "timezone" in location

    def test_producer_metrics_integration(self, producer):
        """Test integration with base producer metrics."""
        # Generate some messages
        for _ in range(5):
            message = producer.generate_message()
            # Simulate successful send
            producer.metrics["messages_sent"] += 1
            producer.metrics["bytes_sent"] += len(json.dumps(message))

        metrics = producer.get_metrics()
        assert metrics["messages_sent"] == 5
        assert metrics["bytes_sent"] > 0

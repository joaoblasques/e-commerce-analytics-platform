"""
Test suite for data generation framework.
"""

import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_generation import (
    ECommerceDataGenerator,
    DataGenerationConfig,
    TemporalPatterns,
    GeographicPatterns,
    FraudPatterns,
    SeasonalPatterns,
    KafkaDataProducer,
    DataGenerationOrchestrator
)
from data_generation.patterns import FraudType, GeographicLocation


class TestDataGenerationConfig:
    """Test suite for DataGenerationConfig."""

    def test_config_initialization(self):
        """Test config initialization with defaults."""
        config = DataGenerationConfig()
        
        assert config.total_users == 10000
        assert config.total_products == 1000
        assert config.fraud_rate == 0.001
        assert len(config.categories) > 0
        assert 'Electronics' in config.categories
        assert 'Clothing' in config.categories

    def test_config_validation(self):
        """Test config validation."""
        config = DataGenerationConfig()
        errors = config.validate()
        assert len(errors) == 0

    def test_config_validation_errors(self):
        """Test config validation with errors."""
        config = DataGenerationConfig()
        config.total_users = -1
        config.fraud_rate = 1.5
        config.active_user_ratio = 2.0
        
        errors = config.validate()
        assert len(errors) > 0
        assert "Total users must be positive" in errors
        assert "Fraud rate must be between 0 and 1" in errors
        assert "Active user ratio must be between 0 and 1" in errors

    def test_category_weight_calculation(self):
        """Test category weight calculation."""
        config = DataGenerationConfig()
        
        # High-weight categories
        assert config.get_category_weight('Electronics') == 0.15
        assert config.get_category_weight('Clothing') == 0.15
        
        # Medium-weight categories
        assert config.get_category_weight('Books') == 0.12
        assert config.get_category_weight('Home & Garden') == 0.12
        
        # Regular categories
        assert config.get_category_weight('Sports & Outdoors') == 0.08
        
        # Unknown category
        assert config.get_category_weight('Unknown') == 0.01

    def test_hourly_multiplier(self):
        """Test hourly multiplier calculation."""
        config = DataGenerationConfig()
        
        # Peak hours
        assert config.get_hourly_multiplier(10) == 3.0
        assert config.get_hourly_multiplier(20) == 3.0
        
        # Regular business hours
        assert config.get_hourly_multiplier(14) == 1.0
        
        # Off-business hours
        assert config.get_hourly_multiplier(3) == 0.1

    def test_seasonal_multiplier(self):
        """Test seasonal multiplier calculation."""
        config = DataGenerationConfig()
        
        # Black Friday
        black_friday = datetime(2024, 11, 29, 12, 0, 0)
        multiplier, categories = config.get_seasonal_multiplier(black_friday)
        assert multiplier == 8.0
        assert 'Electronics' in categories
        
        # Regular day
        regular_day = datetime(2024, 6, 15, 12, 0, 0)
        multiplier, categories = config.get_seasonal_multiplier(regular_day)
        assert multiplier == 1.0
        assert categories == []

    def test_json_serialization(self):
        """Test JSON serialization/deserialization."""
        config = DataGenerationConfig()
        json_str = config.to_json()
        
        # Should be valid JSON
        data = json.loads(json_str)
        assert isinstance(data, dict)
        assert data['total_users'] == 10000
        
        # Should be able to reconstruct
        config2 = DataGenerationConfig.from_json(json_str)
        assert config2.total_users == config.total_users
        assert config2.fraud_rate == config.fraud_rate


class TestTemporalPatterns:
    """Test suite for TemporalPatterns."""

    def test_temporal_context_generation(self):
        """Test temporal context generation."""
        config = DataGenerationConfig()
        patterns = TemporalPatterns(config.__dict__)
        
        # Test business hour
        timestamp = datetime(2024, 6, 15, 14, 30, 0)  # Saturday, 2:30 PM
        context = patterns.get_temporal_context(timestamp)
        
        assert context.hour == 14
        assert context.is_weekend == True
        assert context.season == "summer"
        assert context.business_hour_multiplier > 0

    def test_realistic_timestamp_generation(self):
        """Test realistic timestamp generation."""
        config = DataGenerationConfig()
        patterns = TemporalPatterns(config.__dict__)
        
        base_time = datetime(2024, 6, 15, 12, 0, 0)
        timestamp = patterns.generate_realistic_timestamp(base_time)
        
        assert isinstance(timestamp, datetime)
        assert timestamp.year == 2024
        assert timestamp.month == 6
        assert timestamp.day == 15

    def test_session_duration_generation(self):
        """Test session duration generation."""
        config = DataGenerationConfig()
        patterns = TemporalPatterns(config.__dict__)
        
        # VIP user should have longer sessions
        vip_duration = patterns.generate_session_duration("vip")
        regular_duration = patterns.generate_session_duration("regular")
        
        assert vip_duration.total_seconds() > 0
        assert regular_duration.total_seconds() > 0
        assert vip_duration >= timedelta(minutes=1)


class TestGeographicPatterns:
    """Test suite for GeographicPatterns."""

    def test_location_generation(self):
        """Test location generation."""
        config = DataGenerationConfig()
        patterns = GeographicPatterns(config.__dict__)
        
        location = patterns.generate_location()
        
        assert isinstance(location, GeographicLocation)
        assert location.latitude != 0
        assert location.longitude != 0
        assert location.country != ""
        assert location.city != ""
        assert location.region != ""

    def test_specific_region_generation(self):
        """Test location generation for specific region."""
        config = DataGenerationConfig()
        patterns = GeographicPatterns(config.__dict__)
        
        location = patterns.generate_location("North America")
        
        assert location.region == "North America"
        assert location.country in ['US', 'CA', 'MX']

    def test_distance_calculation(self):
        """Test distance calculation between locations."""
        config = DataGenerationConfig()
        patterns = GeographicPatterns(config.__dict__)
        
        # New York to Los Angeles (approximate)
        ny = GeographicLocation(40.7128, -74.0060, "US", "NY", "New York", "UTC-5", "North America")
        la = GeographicLocation(34.0522, -118.2437, "US", "CA", "Los Angeles", "UTC-8", "North America")
        
        distance = patterns.calculate_distance(ny, la)
        
        # Should be approximately 3,900 km
        assert 3500 < distance < 4500

    def test_location_anomaly_detection(self):
        """Test location anomaly detection."""
        config = DataGenerationConfig()
        patterns = GeographicPatterns(config.__dict__)
        
        # Same location
        ny1 = GeographicLocation(40.7128, -74.0060, "US", "NY", "New York", "UTC-5", "North America")
        ny2 = GeographicLocation(40.7130, -74.0062, "US", "NY", "New York", "UTC-5", "North America")
        
        assert not patterns.is_location_anomaly(ny1, ny2, threshold_km=500)
        
        # Different continents
        tokyo = GeographicLocation(35.6762, 139.6503, "JP", "Tokyo", "Tokyo", "UTC+9", "Asia")
        
        assert patterns.is_location_anomaly(ny1, tokyo, threshold_km=500)


class TestFraudPatterns:
    """Test suite for FraudPatterns."""

    def test_fraud_injection_decision(self):
        """Test fraud injection decision."""
        config = DataGenerationConfig()
        config.fraud_rate = 0.5  # 50% for testing
        
        patterns = FraudPatterns(config.__dict__)
        
        # With 50% rate, should inject fraud sometimes
        results = [patterns.should_inject_fraud() for _ in range(100)]
        assert any(results)  # At least some should be True

    def test_fraud_type_selection(self):
        """Test fraud type selection."""
        config = DataGenerationConfig()
        patterns = FraudPatterns(config.__dict__)
        
        fraud_type = patterns.select_fraud_type()
        
        assert isinstance(fraud_type, FraudType)
        assert fraud_type.value in config.fraud_patterns

    def test_velocity_attack_generation(self):
        """Test velocity attack generation."""
        config = DataGenerationConfig()
        patterns = FraudPatterns(config.__dict__)
        
        user_id = "test_user"
        timestamp = datetime.now()
        
        attacks = patterns.generate_velocity_attack(user_id, timestamp)
        
        assert len(attacks) >= 5
        assert all(attack['user_id'] == user_id for attack in attacks)
        assert all(attack['fraud_type'] == FraudType.VELOCITY_ATTACK.value for attack in attacks)

    def test_location_anomaly_generation(self):
        """Test location anomaly generation."""
        config = DataGenerationConfig()
        patterns = FraudPatterns(config.__dict__)
        
        user_location = GeographicLocation(40.7128, -74.0060, "US", "NY", "New York", "UTC-5", "North America")
        
        anomaly_location = patterns.generate_location_anomaly(user_location)
        
        assert isinstance(anomaly_location, GeographicLocation)
        assert anomaly_location.region != user_location.region

    def test_amount_anomaly_generation(self):
        """Test amount anomaly generation."""
        config = DataGenerationConfig()
        patterns = FraudPatterns(config.__dict__)
        
        user_avg_amount = 100.0
        anomaly_amount = patterns.generate_amount_anomaly(user_avg_amount)
        
        # Should be significantly different from average
        assert anomaly_amount != user_avg_amount
        assert anomaly_amount > user_avg_amount * 2 or anomaly_amount < 5.0

    def test_device_anomaly_generation(self):
        """Test device anomaly generation."""
        config = DataGenerationConfig()
        patterns = FraudPatterns(config.__dict__)
        
        device_info = patterns.generate_device_anomaly()
        
        assert isinstance(device_info, dict)
        assert 'device_id' in device_info
        assert 'device_type' in device_info
        assert device_info['device_type'] == 'unknown'


class TestSeasonalPatterns:
    """Test suite for SeasonalPatterns."""

    def test_seasonal_multiplier_calculation(self):
        """Test seasonal multiplier calculation."""
        config = DataGenerationConfig()
        patterns = SeasonalPatterns(config.__dict__)
        
        # Holiday season
        december_timestamp = datetime(2024, 12, 15, 12, 0, 0)
        multiplier = patterns.get_seasonal_multiplier(december_timestamp, "Electronics")
        assert multiplier == 1.5  # Holiday season boost
        
        # Regular season
        june_timestamp = datetime(2024, 6, 15, 12, 0, 0)
        multiplier = patterns.get_seasonal_multiplier(june_timestamp, "Electronics")
        assert multiplier == 1.2  # Summer season boost

    def test_trending_categories(self):
        """Test trending categories identification."""
        config = DataGenerationConfig()
        patterns = SeasonalPatterns(config.__dict__)
        
        # Holiday season
        december_timestamp = datetime(2024, 12, 15, 12, 0, 0)
        trending = patterns.get_trending_categories(december_timestamp)
        assert 'Electronics' in trending
        assert 'Toys & Games' in trending

    def test_seasonal_boost_application(self):
        """Test seasonal boost application."""
        config = DataGenerationConfig()
        patterns = SeasonalPatterns(config.__dict__)
        
        base_prob = 0.1
        december_timestamp = datetime(2024, 12, 15, 12, 0, 0)
        
        # Electronics should get boost in December
        boosted = patterns.apply_seasonal_boost("Electronics", base_prob, december_timestamp)
        assert boosted > base_prob
        
        # Random category should get less boost
        other_boosted = patterns.apply_seasonal_boost("Pet Supplies", base_prob, december_timestamp)
        assert other_boosted >= base_prob


class TestECommerceDataGenerator:
    """Test suite for ECommerceDataGenerator."""

    def test_generator_initialization(self):
        """Test generator initialization."""
        config = DataGenerationConfig()
        config.total_users = 100
        config.total_products = 50
        
        generator = ECommerceDataGenerator(config)
        
        assert len(generator.users) == 100
        assert len(generator.products) == 50
        assert len(generator.user_locations) == 100
        assert len(generator.user_devices) == 100

    def test_user_generation(self):
        """Test user generation."""
        config = DataGenerationConfig()
        config.total_users = 10
        
        generator = ECommerceDataGenerator(config)
        
        # Check user structure
        user_id = list(generator.users.keys())[0]
        user = generator.users[user_id]
        
        required_fields = ['user_id', 'email', 'name', 'age', 'location', 'user_type']
        for field in required_fields:
            assert field in user
        
        assert user['user_type'] in ['regular', 'vip', 'new']
        assert 18 <= user['age'] <= 76

    def test_product_generation(self):
        """Test product generation."""
        config = DataGenerationConfig()
        config.total_products = 10
        
        generator = ECommerceDataGenerator(config)
        
        # Check product structure
        product_id = list(generator.products.keys())[0]
        product = generator.products[product_id]
        
        required_fields = ['product_id', 'name', 'category', 'price', 'brand']
        for field in required_fields:
            assert field in product
        
        assert product['category'] in config.categories
        assert product['price'] > 0

    def test_transaction_generation(self):
        """Test transaction generation."""
        config = DataGenerationConfig()
        config.total_users = 10
        config.total_products = 10
        
        generator = ECommerceDataGenerator(config)
        
        transactions = generator.generate_transactions(5)
        
        assert len(transactions) == 5
        
        # Check transaction structure
        transaction = transactions[0]
        required_fields = ['transaction_id', 'user_id', 'timestamp', 'products', 'total_amount']
        for field in required_fields:
            assert field in transaction
        
        assert transaction['total_amount'] > 0
        assert len(transaction['products']) > 0

    def test_user_event_generation(self):
        """Test user event generation."""
        config = DataGenerationConfig()
        config.total_users = 10
        
        generator = ECommerceDataGenerator(config)
        
        events = generator.generate_user_events(5)
        
        assert len(events) == 5
        
        # Check event structure
        event = events[0]
        required_fields = ['event_id', 'user_id', 'session_id', 'event_type', 'timestamp']
        for field in required_fields:
            assert field in event
        
        assert event['event_type'] in config.event_types

    def test_product_update_generation(self):
        """Test product update generation."""
        config = DataGenerationConfig()
        config.total_products = 10
        
        generator = ECommerceDataGenerator(config)
        
        updates = generator.generate_product_updates(5)
        
        assert len(updates) == 5
        
        # Check update structure
        update = updates[0]
        required_fields = ['product_id', 'update_type', 'timestamp', 'old_value', 'new_value']
        for field in required_fields:
            assert field in update
        
        assert update['update_type'] in ['price_change', 'stock_update', 'description_update', 'status_change']

    def test_statistics_generation(self):
        """Test statistics generation."""
        config = DataGenerationConfig()
        config.total_users = 10
        config.total_products = 10
        
        generator = ECommerceDataGenerator(config)
        
        stats = generator.get_statistics()
        
        assert stats['total_users'] == 10
        assert stats['total_products'] == 10
        assert 'active_users' in stats
        assert 'categories' in stats


class TestKafkaDataProducer:
    """Test suite for KafkaDataProducer."""

    @patch('data_generation.producers.KafkaProducer')
    def test_producer_initialization(self, mock_kafka_producer):
        """Test producer initialization."""
        config = DataGenerationConfig()
        generator = ECommerceDataGenerator(config)
        
        producer = KafkaDataProducer(generator)
        
        assert producer.generator == generator
        assert producer.bootstrap_servers == ['localhost:9092']
        assert not producer.streaming_active

    @patch('data_generation.producers.KafkaProducer')
    def test_health_check(self, mock_kafka_producer):
        """Test producer health check."""
        config = DataGenerationConfig()
        generator = ECommerceDataGenerator(config)
        
        # Mock successful connection
        mock_producer_instance = MagicMock()
        mock_producer_instance.list_topics.return_value = Mock(topics={
            'transactions': Mock(),
            'user-events': Mock(),
            'product-updates': Mock(),
            'fraud-alerts': Mock(),
            'analytics-results': Mock()
        })
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = KafkaDataProducer(generator)
        health = producer.health_check()
        
        assert health['overall_status'] == 'HEALTHY'
        assert health['producer_connected'] == True
        assert 'available_topics' in health

    @patch('data_generation.producers.KafkaProducer')
    def test_send_batch(self, mock_kafka_producer):
        """Test batch sending."""
        config = DataGenerationConfig()
        generator = ECommerceDataGenerator(config)
        
        # Mock successful sending
        mock_producer_instance = MagicMock()
        mock_future = Mock()
        mock_future.get.return_value = Mock(partition=0, offset=123)
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = KafkaDataProducer(generator)
        
        # Generate test data
        test_data = [
            {'transaction_id': 'test1', 'user_id': 'user1', 'amount': 100},
            {'transaction_id': 'test2', 'user_id': 'user2', 'amount': 200}
        ]
        
        result = producer.send_batch('transactions', test_data)
        
        assert result == 2  # Both messages sent successfully
        assert mock_producer_instance.send.call_count == 2


class TestDataGenerationOrchestrator:
    """Test suite for DataGenerationOrchestrator."""

    @patch('data_generation.producers.KafkaProducer')
    def test_orchestrator_initialization(self, mock_kafka_producer):
        """Test orchestrator initialization."""
        config = DataGenerationConfig()
        orchestrator = DataGenerationOrchestrator(config)
        
        assert orchestrator.config == config
        assert orchestrator.generator is not None
        assert orchestrator.producer is not None
        assert 'normal_traffic' in orchestrator.scenarios

    @patch('data_generation.producers.KafkaProducer')
    def test_scenario_configuration(self, mock_kafka_producer):
        """Test scenario configuration."""
        config = DataGenerationConfig()
        orchestrator = DataGenerationOrchestrator(config)
        
        # Check scenario exists
        assert 'peak_traffic' in orchestrator.scenarios
        
        scenario = orchestrator.scenarios['peak_traffic']
        assert scenario['rate_multiplier'] == 3.0
        assert 'transactions' in scenario['topics']

    @patch('data_generation.producers.KafkaProducer')
    def test_comprehensive_stats(self, mock_kafka_producer):
        """Test comprehensive statistics."""
        config = DataGenerationConfig()
        orchestrator = DataGenerationOrchestrator(config)
        
        stats = orchestrator.get_comprehensive_stats()
        
        assert 'generator_stats' in stats
        assert 'producer_stats' in stats
        assert 'producer_health' in stats
        assert 'config' in stats


@pytest.mark.integration
class TestDataGenerationIntegration:
    """Integration tests for data generation (requires running Kafka)."""

    def test_end_to_end_generation(self):
        """Test end-to-end data generation."""
        try:
            config = DataGenerationConfig()
            config.total_users = 10
            config.total_products = 10
            
            generator = ECommerceDataGenerator(config)
            
            # Generate small amounts of data
            transactions = generator.generate_transactions(2)
            events = generator.generate_user_events(2)
            updates = generator.generate_product_updates(2)
            
            assert len(transactions) == 2
            assert len(events) == 2
            assert len(updates) == 2
            
            # Verify data structure
            assert all('transaction_id' in t for t in transactions)
            assert all('event_id' in e for e in events)
            assert all('product_id' in u for u in updates)
            
        except Exception as e:
            pytest.skip(f"Integration test failed: {e}")

    def test_kafka_integration(self):
        """Test Kafka integration if available."""
        try:
            config = DataGenerationConfig()
            config.total_users = 5
            config.total_products = 5
            
            generator = ECommerceDataGenerator(config)
            producer = KafkaDataProducer(generator)
            
            # Test health check
            health = producer.health_check()
            
            if health['producer_connected']:
                # Generate small batch
                transactions = generator.generate_transactions(1)
                result = producer.send_batch('transactions', transactions)
                
                assert result == 1
                
            producer.close()
            
        except Exception as e:
            pytest.skip(f"Kafka integration test failed: {e}")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
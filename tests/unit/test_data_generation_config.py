"""
Comprehensive unit tests for data generation configuration.
"""
import json
from datetime import datetime

from src.data_generation.config import DataGenerationConfig


def test_config_initialization():
    """Test that configuration initializes with default values."""
    config = DataGenerationConfig()

    # Test basic attributes
    assert config.total_users == 10000
    assert config.active_user_ratio == 0.7
    assert config.vip_user_ratio == 0.05
    assert config.total_products == 1000
    assert config.base_transaction_rate == 10000
    assert config.peak_hour_multiplier == 3.0


def test_config_custom_initialization():
    """Test that configuration can be initialized with custom values."""
    config = DataGenerationConfig(
        total_users=5000, active_user_ratio=0.8, base_transaction_rate=20000
    )

    assert config.total_users == 5000
    assert config.active_user_ratio == 0.8
    assert config.base_transaction_rate == 20000


def test_categories_default():
    """Test that categories have expected default values."""
    config = DataGenerationConfig()

    expected_categories = [
        "Electronics",
        "Clothing",
        "Books",
        "Home & Garden",
        "Sports & Outdoors",
        "Beauty & Personal Care",
        "Automotive",
        "Health & Wellness",
        "Toys & Games",
        "Food & Beverages",
        "Jewelry & Accessories",
        "Pet Supplies",
    ]

    assert config.categories == expected_categories
    assert len(config.categories) == 12


def test_regions_default():
    """Test that regions have expected default structure."""
    config = DataGenerationConfig()

    assert "North America" in config.regions
    assert "Europe" in config.regions
    assert "Asia" in config.regions
    assert "Others" in config.regions

    # Test region structure
    na_region = config.regions["North America"]
    assert "weight" in na_region
    assert "time_zone_offset" in na_region
    assert na_region["weight"] == 0.45


def test_to_json():
    """Test configuration serialization to JSON."""
    config = DataGenerationConfig(total_users=1000)
    json_str = config.to_json()

    assert isinstance(json_str, str)
    parsed = json.loads(json_str)
    assert parsed["total_users"] == 1000
    assert "categories" in parsed
    assert "regions" in parsed


def test_from_json():
    """Test configuration deserialization from JSON."""
    original_config = DataGenerationConfig(total_users=2000, active_user_ratio=0.9)
    json_str = original_config.to_json()

    new_config = DataGenerationConfig.from_json(json_str)
    assert new_config.total_users == 2000
    assert new_config.active_user_ratio == 0.9


def test_get_category_weight():
    """Test category weight calculation."""
    config = DataGenerationConfig()

    # High-weight categories
    assert config.get_category_weight("Electronics") == 0.15
    assert config.get_category_weight("Clothing") == 0.15

    # Medium-weight categories
    assert config.get_category_weight("Books") == 0.12
    assert config.get_category_weight("Home & Garden") == 0.12

    # Standard-weight categories
    assert config.get_category_weight("Sports & Outdoors") == 0.08

    # Non-existent category
    assert config.get_category_weight("NonExistent") == 0.01


def test_get_hourly_multiplier_weekday():
    """Test hourly multiplier calculation for weekdays."""
    config = DataGenerationConfig()

    # Peak hours
    assert config.get_hourly_multiplier(10, False) == 3.0  # Peak hour
    assert config.get_hourly_multiplier(14, False) == 3.0  # Peak hour
    assert config.get_hourly_multiplier(19, False) == 3.0  # Peak hour

    # Regular business hours
    assert config.get_hourly_multiplier(9, False) == 1.0  # Business hour but not peak
    assert config.get_hourly_multiplier(16, False) == 1.0  # Business hour but not peak

    # Off-business hours
    assert config.get_hourly_multiplier(2, False) == 0.1  # Night time
    assert config.get_hourly_multiplier(5, False) == 0.1  # Early morning


def test_get_hourly_multiplier_weekend():
    """Test hourly multiplier calculation for weekends."""
    config = DataGenerationConfig()

    # Peak hours on weekend
    assert config.get_hourly_multiplier(10, True) == 3.0
    assert config.get_hourly_multiplier(20, True) == 3.0

    # Regular weekend hours
    assert config.get_hourly_multiplier(12, True) == 1.0

    # Off-hours on weekend
    assert config.get_hourly_multiplier(3, True) == 0.1


def test_get_seasonal_multiplier():
    """Test seasonal multiplier calculation."""
    config = DataGenerationConfig()

    # Test Black Friday
    black_friday = datetime(2024, 11, 29, 12, 0)
    multiplier, categories = config.get_seasonal_multiplier(black_friday)
    assert multiplier == 8.0
    assert "Electronics" in categories
    assert "Clothing" in categories

    # Test Christmas
    christmas = datetime(2024, 12, 25, 12, 0)
    multiplier, categories = config.get_seasonal_multiplier(christmas)
    assert multiplier == 6.0
    assert "Toys & Games" in categories

    # Test normal day
    normal_day = datetime(2024, 6, 15, 12, 0)
    multiplier, categories = config.get_seasonal_multiplier(normal_day)
    assert multiplier == 1.0
    assert categories == []


def test_validate_valid_config():
    """Test validation of a valid configuration."""
    config = DataGenerationConfig()
    errors = config.validate()
    assert len(errors) == 0


def test_validate_invalid_payment_methods():
    """Test validation with invalid payment method weights."""
    config = DataGenerationConfig()
    config.payment_methods = {
        "credit_card": 0.6,
        "debit_card": 0.5,  # This makes total > 1.0
    }

    errors = config.validate()
    assert len(errors) > 0
    assert any("Payment method weights" in error for error in errors)


def test_validate_invalid_device_types():
    """Test validation with invalid device type weights."""
    config = DataGenerationConfig()
    config.device_types = {"desktop": 0.7, "mobile": 0.6}  # This makes total > 1.0

    errors = config.validate()
    assert len(errors) > 0
    assert any("Device type weights" in error for error in errors)


def test_validate_invalid_user_counts():
    """Test validation with invalid user counts."""
    config = DataGenerationConfig()
    config.total_users = -100
    config.total_products = 0

    errors = config.validate()
    assert len(errors) >= 2
    assert any("Total users must be positive" in error for error in errors)
    assert any("Total products must be positive" in error for error in errors)


def test_validate_invalid_ratios():
    """Test validation with invalid ratios."""
    config = DataGenerationConfig()
    config.active_user_ratio = 1.5  # > 1
    config.vip_user_ratio = -0.1  # < 0
    config.fraud_rate = 2.0  # > 1

    errors = config.validate()
    assert len(errors) >= 3
    assert any("Active user ratio" in error for error in errors)
    assert any("VIP user ratio" in error for error in errors)
    assert any("Fraud rate" in error for error in errors)


def test_fraud_patterns_structure():
    """Test fraud patterns structure."""
    config = DataGenerationConfig()

    expected_patterns = [
        "velocity_attack",
        "location_anomaly",
        "amount_anomaly",
        "time_anomaly",
        "device_anomaly",
        "pattern_anomaly",
    ]

    for pattern in expected_patterns:
        assert pattern in config.fraud_patterns
        assert isinstance(config.fraud_patterns[pattern], float)
        assert 0 <= config.fraud_patterns[pattern] <= 1


def test_seasonal_events_structure():
    """Test seasonal events structure."""
    config = DataGenerationConfig()

    expected_events = ["Black Friday", "Christmas", "Back to School", "Valentine's Day"]

    for event in expected_events:
        assert event in config.seasonal_events
        event_data = config.seasonal_events[event]
        assert "date" in event_data
        assert "multiplier" in event_data
        assert "duration_hours" in event_data
        assert "categories" in event_data


def test_business_hours_structure():
    """Test business hours structure."""
    config = DataGenerationConfig()

    assert "weekday" in config.business_hours
    assert "weekend" in config.business_hours

    weekday_hours = config.business_hours["weekday"]
    weekend_hours = config.business_hours["weekend"]

    assert isinstance(weekday_hours, list)
    assert isinstance(weekend_hours, list)
    assert all(0 <= hour <= 23 for hour in weekday_hours)
    assert all(0 <= hour <= 23 for hour in weekend_hours)


def test_age_ranges_and_distribution():
    """Test age ranges and distribution consistency."""
    config = DataGenerationConfig()

    # Check that all age groups in distribution have corresponding ranges
    for _age_group in config.age_distribution.keys():
        assert _age_group in config.age_ranges

    # Check that all ranges are valid
    for age_group, (min_age, max_age) in config.age_ranges.items():
        assert min_age < max_age
        assert min_age >= 0
        assert max_age <= 120


def test_price_ranges_for_categories():
    """Test price ranges for all categories."""
    config = DataGenerationConfig()

    # Check that all categories have price ranges
    for category in config.categories:
        assert category in config.price_ranges
        min_price, max_price = config.price_ranges[category]
        assert min_price < max_price
        assert min_price > 0


def test_event_types_structure():
    """Test event types structure."""
    config = DataGenerationConfig()

    total_weight = sum(event["weight"] for event in config.event_types.values())
    assert abs(total_weight - 1.0) < 0.01  # Should sum to approximately 1.0

    for _event_type, event_data in config.event_types.items():
        assert "weight" in event_data
        assert "duration_range" in event_data
        assert len(event_data["duration_range"]) == 2
        assert event_data["duration_range"][0] < event_data["duration_range"][1]


def test_json_round_trip():
    """Test that configuration survives JSON round trip."""
    original = DataGenerationConfig(total_users=5000, active_user_ratio=0.8)

    # Convert to JSON and back
    json_str = original.to_json()
    restored = DataGenerationConfig.from_json(json_str)

    # Compare key attributes
    assert original.total_users == restored.total_users
    assert original.active_user_ratio == restored.active_user_ratio
    assert original.categories == restored.categories


def test_config_immutable_defaults():
    """Test that default factory functions work correctly."""
    config1 = DataGenerationConfig()
    config2 = DataGenerationConfig()

    # Modifying one config should not affect the other
    config1.categories.append("Test Category")
    assert "Test Category" not in config2.categories


def test_operating_systems_structure():
    """Test operating systems distribution structure."""
    config = DataGenerationConfig()

    for device_type in config.device_types.keys():
        assert device_type in config.operating_systems

        os_dist = config.operating_systems[device_type]
        total_weight = sum(os_dist.values())
        assert abs(total_weight - 1.0) < 0.01  # Should sum to approximately 1.0

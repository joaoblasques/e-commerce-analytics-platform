"""
Configuration for data generation framework.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class DataGenerationConfig:
    """Configuration for e-commerce data generation."""

    # User base configuration
    total_users: int = 10000
    active_user_ratio: float = 0.7
    vip_user_ratio: float = 0.05

    # Product catalog configuration
    total_products: int = 1000
    categories: List[str] = field(
        default_factory=lambda: [
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
    )

    # Transaction patterns
    base_transaction_rate: int = 10000  # events per hour
    peak_hour_multiplier: float = 3.0
    weekend_multiplier: float = 1.5
    holiday_multiplier: float = 5.0

    # User behavior patterns
    base_event_rate: int = 100000  # events per hour
    session_duration_minutes: Tuple[int, int] = (5, 120)  # min, max
    events_per_session: Tuple[int, int] = (3, 50)  # min, max

    # Geographic distribution
    regions: Dict[str, Dict[str, float]] = field(
        default_factory=lambda: {
            "North America": {"weight": 0.45, "time_zone_offset": -5},
            "Europe": {"weight": 0.30, "time_zone_offset": 1},
            "Asia": {"weight": 0.20, "time_zone_offset": 8},
            "Others": {"weight": 0.05, "time_zone_offset": 0},
        }
    )

    # Fraud patterns
    fraud_rate: float = 0.001  # 0.1% of transactions
    fraud_patterns: Dict[str, float] = field(
        default_factory=lambda: {
            "velocity_attack": 0.3,  # Multiple transactions in short time
            "location_anomaly": 0.2,  # Unusual location
            "amount_anomaly": 0.2,  # Unusual amount
            "time_anomaly": 0.15,  # Unusual time
            "device_anomaly": 0.1,  # New/unusual device
            "pattern_anomaly": 0.05,  # Unusual purchase pattern
        }
    )

    # Seasonal patterns
    seasonal_events: Dict[str, Dict[str, Any]] = field(
        default_factory=lambda: {
            "Black Friday": {
                "date": "2024-11-29",
                "multiplier": 8.0,
                "duration_hours": 24,
                "categories": ["Electronics", "Clothing", "Home & Garden"],
            },
            "Christmas": {
                "date": "2024-12-25",
                "multiplier": 6.0,
                "duration_hours": 72,
                "categories": ["Toys & Games", "Electronics", "Jewelry & Accessories"],
            },
            "Back to School": {
                "date": "2024-08-15",
                "multiplier": 3.0,
                "duration_hours": 168,  # 1 week
                "categories": ["Books", "Electronics", "Clothing"],
            },
            "Valentine's Day": {
                "date": "2024-02-14",
                "multiplier": 4.0,
                "duration_hours": 48,
                "categories": ["Jewelry & Accessories", "Beauty & Personal Care"],
            },
        }
    )

    # Business hours (24-hour format)
    business_hours: Dict[str, List[int]] = field(
        default_factory=lambda: {
            "weekday": [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
            "weekend": [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
        }
    )

    # Payment methods distribution
    payment_methods: Dict[str, float] = field(
        default_factory=lambda: {
            "credit_card": 0.45,
            "debit_card": 0.25,
            "paypal": 0.15,
            "apple_pay": 0.08,
            "google_pay": 0.05,
            "bank_transfer": 0.02,
        }
    )

    # Device types distribution
    device_types: Dict[str, float] = field(
        default_factory=lambda: {"desktop": 0.35, "mobile": 0.50, "tablet": 0.15}
    )

    # Operating systems distribution
    operating_systems: Dict[str, Dict[str, float]] = field(
        default_factory=lambda: {
            "desktop": {"Windows": 0.65, "macOS": 0.30, "Linux": 0.05},
            "mobile": {"Android": 0.70, "iOS": 0.30},
            "tablet": {"Android": 0.60, "iOS": 0.40},
        }
    )

    # Browsers distribution
    browsers: Dict[str, float] = field(
        default_factory=lambda: {
            "Chrome": 0.65,
            "Safari": 0.20,
            "Firefox": 0.08,
            "Edge": 0.05,
            "Opera": 0.02,
        }
    )

    # Price ranges by category
    price_ranges: Dict[str, Tuple[float, float]] = field(
        default_factory=lambda: {
            "Electronics": (50.0, 2000.0),
            "Clothing": (15.0, 300.0),
            "Books": (5.0, 50.0),
            "Home & Garden": (10.0, 500.0),
            "Sports & Outdoors": (20.0, 800.0),
            "Beauty & Personal Care": (8.0, 150.0),
            "Automotive": (25.0, 1500.0),
            "Health & Wellness": (15.0, 200.0),
            "Toys & Games": (10.0, 250.0),
            "Food & Beverages": (5.0, 100.0),
            "Jewelry & Accessories": (20.0, 5000.0),
            "Pet Supplies": (10.0, 150.0),
        }
    )

    # Customer demographics
    age_ranges: Dict[str, Tuple[int, int]] = field(
        default_factory=lambda: {
            "Gen Z": (18, 25),
            "Millennial": (26, 41),
            "Gen X": (42, 57),
            "Baby Boomer": (58, 76),
        }
    )

    age_distribution: Dict[str, float] = field(
        default_factory=lambda: {
            "Gen Z": 0.25,
            "Millennial": 0.35,
            "Gen X": 0.25,
            "Baby Boomer": 0.15,
        }
    )

    # Marketing channels
    marketing_channels: Dict[str, float] = field(
        default_factory=lambda: {
            "organic_search": 0.35,
            "paid_search": 0.20,
            "social_media": 0.15,
            "email_marketing": 0.12,
            "direct_traffic": 0.10,
            "referral": 0.05,
            "display_ads": 0.03,
        }
    )

    # Event types for user behavior
    event_types: Dict[str, Dict[str, Any]] = field(
        default_factory=lambda: {
            "page_view": {"weight": 0.40, "duration_range": (10, 300)},
            "product_view": {"weight": 0.25, "duration_range": (30, 600)},
            "search": {"weight": 0.15, "duration_range": (5, 60)},
            "add_to_cart": {"weight": 0.08, "duration_range": (5, 30)},
            "remove_from_cart": {"weight": 0.03, "duration_range": (5, 15)},
            "checkout_start": {"weight": 0.04, "duration_range": (30, 300)},
            "checkout_complete": {"weight": 0.02, "duration_range": (60, 180)},
            "login": {"weight": 0.02, "duration_range": (5, 30)},
            "logout": {"weight": 0.01, "duration_range": (1, 5)},
        }
    )

    @classmethod
    def from_json(cls, json_str: str) -> "DataGenerationConfig":
        """Create configuration from JSON string."""
        data = json.loads(json_str)
        return cls(**data)

    def to_json(self) -> str:
        """Convert configuration to JSON string."""
        return json.dumps(self.__dict__, indent=2, default=str)

    def get_category_weight(self, category: str) -> float:
        """Get normalized weight for a category."""
        total_categories = len(self.categories)
        if category in self.categories:
            # Electronics and Clothing get higher weights
            if category in ["Electronics", "Clothing"]:
                return 0.15
            elif category in ["Books", "Home & Garden"]:
                return 0.12
            else:
                return 0.08
        return 0.01

    def get_hourly_multiplier(self, hour: int, is_weekend: bool = False) -> float:
        """Get traffic multiplier for a specific hour."""
        business_hours = self.business_hours["weekend" if is_weekend else "weekday"]

        if hour in business_hours:
            # Peak hours: 10-11 AM, 2-3 PM, 7-9 PM
            if hour in [10, 11, 14, 15, 19, 20, 21]:
                return self.peak_hour_multiplier
            # Regular business hours
            elif hour in business_hours:
                return 1.0
            else:
                return 0.3
        else:
            # Off-business hours
            return 0.1

    def get_seasonal_multiplier(self, date: datetime) -> Tuple[float, List[str]]:
        """Get seasonal multiplier and affected categories for a date."""
        for event_name, event_data in self.seasonal_events.items():
            event_date = datetime.strptime(event_data["date"], "%Y-%m-%d")
            duration = timedelta(hours=event_data["duration_hours"])

            if event_date <= date <= event_date + duration:
                return event_data["multiplier"], event_data["categories"]

        return 1.0, []

    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []

        # Validate ratios sum to <= 1.0
        if sum(self.payment_methods.values()) > 1.01:
            errors.append("Payment method weights sum to more than 1.0")

        if sum(self.device_types.values()) > 1.01:
            errors.append("Device type weights sum to more than 1.0")

        if sum(self.age_distribution.values()) > 1.01:
            errors.append("Age distribution weights sum to more than 1.0")

        if sum(self.marketing_channels.values()) > 1.01:
            errors.append("Marketing channel weights sum to more than 1.0")

        # Validate fraud patterns
        if sum(self.fraud_patterns.values()) > 1.01:
            errors.append("Fraud pattern weights sum to more than 1.0")

        # Validate ranges
        if self.total_users <= 0:
            errors.append("Total users must be positive")

        if self.total_products <= 0:
            errors.append("Total products must be positive")

        if not (0 <= self.active_user_ratio <= 1):
            errors.append("Active user ratio must be between 0 and 1")

        if not (0 <= self.vip_user_ratio <= 1):
            errors.append("VIP user ratio must be between 0 and 1")

        if not (0 <= self.fraud_rate <= 1):
            errors.append("Fraud rate must be between 0 and 1")

        return errors

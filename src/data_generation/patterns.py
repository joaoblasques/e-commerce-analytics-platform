"""
Pattern generation classes for realistic e-commerce data simulation.
"""

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from faker import Faker


class FraudType(Enum):
    """Types of fraud patterns to inject."""

    VELOCITY_ATTACK = "velocity_attack"
    LOCATION_ANOMALY = "location_anomaly"
    AMOUNT_ANOMALY = "amount_anomaly"
    TIME_ANOMALY = "time_anomaly"
    DEVICE_ANOMALY = "device_anomaly"
    PATTERN_ANOMALY = "pattern_anomaly"


@dataclass
class GeographicLocation:
    """Geographic location data."""

    latitude: float
    longitude: float
    country: str
    state: str
    city: str
    timezone: str
    region: str


@dataclass
class TemporalContext:
    """Temporal context for data generation."""

    timestamp: datetime
    hour: int
    day_of_week: int
    is_weekend: bool
    is_holiday: bool
    season: str
    business_hour_multiplier: float
    seasonal_multiplier: float


class TemporalPatterns:
    """Generates realistic temporal patterns for e-commerce data."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.holidays = self._load_holidays()

    def _load_holidays(self) -> List[datetime]:
        """Load holiday dates."""
        holidays = []
        current_year = datetime.now().year

        # Major holidays affecting e-commerce
        holiday_dates = [
            f"{current_year}-01-01",  # New Year
            f"{current_year}-02-14",  # Valentine's Day
            f"{current_year}-05-12",  # Mother's Day (second Sunday in May)
            f"{current_year}-06-15",  # Father's Day (third Sunday in June)
            f"{current_year}-07-04",  # Independence Day
            f"{current_year}-10-31",  # Halloween
            f"{current_year}-11-28",  # Thanksgiving (fourth Thursday in November)
            f"{current_year}-11-29",  # Black Friday
            f"{current_year}-12-02",  # Cyber Monday
            f"{current_year}-12-25",  # Christmas
        ]

        for date_str in holiday_dates:
            holidays.append(datetime.strptime(date_str, "%Y-%m-%d"))

        return holidays

    def get_temporal_context(self, timestamp: datetime) -> TemporalContext:
        """Get comprehensive temporal context for a timestamp."""
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        is_weekend = day_of_week >= 5
        is_holiday = any(
            abs((holiday - timestamp).days) < 1 for holiday in self.holidays
        )

        # Determine season
        month = timestamp.month
        if month in [12, 1, 2]:
            season = "winter"
        elif month in [3, 4, 5]:
            season = "spring"
        elif month in [6, 7, 8]:
            season = "summer"
        else:
            season = "fall"

        # Calculate business hour multiplier
        business_hours = self.config.get("business_hours", {})
        if is_weekend:
            active_hours = business_hours.get("weekend", [])
        else:
            active_hours = business_hours.get("weekday", [])

        if hour in active_hours:
            # Peak hours get higher multiplier
            if hour in [10, 11, 14, 15, 19, 20, 21]:
                business_hour_multiplier = 3.0
            else:
                business_hour_multiplier = 1.0
        else:
            business_hour_multiplier = 0.2

        # Weekend multiplier
        if is_weekend:
            business_hour_multiplier *= 1.5

        # Holiday multiplier
        seasonal_multiplier = 1.0
        if is_holiday:
            seasonal_multiplier = 4.0

        return TemporalContext(
            timestamp=timestamp,
            hour=hour,
            day_of_week=day_of_week,
            is_weekend=is_weekend,
            is_holiday=is_holiday,
            season=season,
            business_hour_multiplier=business_hour_multiplier,
            seasonal_multiplier=seasonal_multiplier,
        )

    def generate_realistic_timestamp(
        self, base_time: datetime = None, hours_range: Tuple[int, int] = (0, 24)
    ) -> datetime:
        """Generate a realistic timestamp based on business patterns."""
        if base_time is None:
            base_time = datetime.now()

        # Weight hours by business activity
        hour_weights = []
        for hour in range(24):
            context = self.get_temporal_context(base_time.replace(hour=hour))
            hour_weights.append(context.business_hour_multiplier)

        # Normalize weights
        total_weight = sum(hour_weights)
        hour_probabilities = [w / total_weight for w in hour_weights]

        # Select hour based on weights
        selected_hour = np.random.choice(24, p=hour_probabilities)

        # Add some randomness to minutes and seconds
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)

        return base_time.replace(hour=selected_hour, minute=minutes, second=seconds)

    def generate_session_duration(self, user_type: str = "regular") -> timedelta:
        """Generate realistic session duration based on user type."""
        if user_type == "vip":
            # VIP users browse longer
            base_minutes = random.normalvariate(45, 15)
        elif user_type == "new":
            # New users have shorter sessions
            base_minutes = random.normalvariate(15, 8)
        else:
            # Regular users
            base_minutes = random.normalvariate(25, 12)

        # Ensure minimum 1 minute
        minutes = max(1, int(base_minutes))
        return timedelta(minutes=minutes)


class GeographicPatterns:
    """Generates realistic geographic patterns for e-commerce data."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.faker = Faker()
        self.regions = self._setup_regions()

    def _setup_regions(self) -> Dict[str, Dict[str, Any]]:
        """Setup geographic regions with realistic data."""
        return {
            "North America": {
                "countries": ["US", "CA", "MX"],
                "weight": 0.45,
                "timezone_offset": -5,
                "cities": [
                    {
                        "city": "New York",
                        "state": "NY",
                        "lat": 40.7128,
                        "lon": -74.0060,
                    },
                    {
                        "city": "Los Angeles",
                        "state": "CA",
                        "lat": 34.0522,
                        "lon": -118.2437,
                    },
                    {"city": "Chicago", "state": "IL", "lat": 41.8781, "lon": -87.6298},
                    {"city": "Houston", "state": "TX", "lat": 29.7604, "lon": -95.3698},
                    {"city": "Toronto", "state": "ON", "lat": 43.6532, "lon": -79.3832},
                    {
                        "city": "Mexico City",
                        "state": "CDMX",
                        "lat": 19.4326,
                        "lon": -99.1332,
                    },
                ],
            },
            "Europe": {
                "countries": ["GB", "DE", "FR", "IT", "ES", "NL"],
                "weight": 0.30,
                "timezone_offset": 1,
                "cities": [
                    {
                        "city": "London",
                        "state": "England",
                        "lat": 51.5074,
                        "lon": -0.1278,
                    },
                    {
                        "city": "Berlin",
                        "state": "Berlin",
                        "lat": 52.5200,
                        "lon": 13.4050,
                    },
                    {
                        "city": "Paris",
                        "state": "Île-de-France",
                        "lat": 48.8566,
                        "lon": 2.3522,
                    },
                    {"city": "Rome", "state": "Lazio", "lat": 41.9028, "lon": 12.4964},
                    {
                        "city": "Madrid",
                        "state": "Madrid",
                        "lat": 40.4168,
                        "lon": -3.7038,
                    },
                    {
                        "city": "Amsterdam",
                        "state": "North Holland",
                        "lat": 52.3676,
                        "lon": 4.9041,
                    },
                ],
            },
            "Asia": {
                "countries": ["JP", "KR", "SG", "IN", "CN", "AU"],
                "weight": 0.20,
                "timezone_offset": 8,
                "cities": [
                    {
                        "city": "Tokyo",
                        "state": "Tokyo",
                        "lat": 35.6762,
                        "lon": 139.6503,
                    },
                    {
                        "city": "Seoul",
                        "state": "Seoul",
                        "lat": 37.5665,
                        "lon": 126.9780,
                    },
                    {
                        "city": "Singapore",
                        "state": "Singapore",
                        "lat": 1.3521,
                        "lon": 103.8198,
                    },
                    {
                        "city": "Mumbai",
                        "state": "Maharashtra",
                        "lat": 19.0760,
                        "lon": 72.8777,
                    },
                    {
                        "city": "Shanghai",
                        "state": "Shanghai",
                        "lat": 31.2304,
                        "lon": 121.4737,
                    },
                    {
                        "city": "Sydney",
                        "state": "NSW",
                        "lat": -33.8688,
                        "lon": 151.2093,
                    },
                ],
            },
            "Others": {
                "countries": ["BR", "AR", "ZA", "NG", "EG"],
                "weight": 0.05,
                "timezone_offset": 0,
                "cities": [
                    {
                        "city": "São Paulo",
                        "state": "SP",
                        "lat": -23.5505,
                        "lon": -46.6333,
                    },
                    {
                        "city": "Buenos Aires",
                        "state": "CABA",
                        "lat": -34.6037,
                        "lon": -58.3816,
                    },
                    {
                        "city": "Cape Town",
                        "state": "Western Cape",
                        "lat": -33.9249,
                        "lon": 18.4241,
                    },
                    {"city": "Lagos", "state": "Lagos", "lat": 6.5244, "lon": 3.3792},
                    {"city": "Cairo", "state": "Cairo", "lat": 30.0444, "lon": 31.2357},
                ],
            },
        }

    def generate_location(self, region: str = None) -> GeographicLocation:
        """Generate a realistic geographic location."""
        if region is None:
            # Select region based on weights
            region_weights = [data["weight"] for data in self.regions.values()]
            region = np.random.choice(list(self.regions.keys()), p=region_weights)

        region_data = self.regions[region]
        city_data = random.choice(region_data["cities"])

        # Add some noise to coordinates for privacy
        lat_noise = random.uniform(-0.1, 0.1)
        lon_noise = random.uniform(-0.1, 0.1)

        return GeographicLocation(
            latitude=city_data["lat"] + lat_noise,
            longitude=city_data["lon"] + lon_noise,
            country=random.choice(region_data["countries"]),
            state=city_data["state"],
            city=city_data["city"],
            timezone=f"UTC{region_data['timezone_offset']:+d}",
            region=region,
        )

    def calculate_distance(
        self, loc1: GeographicLocation, loc2: GeographicLocation
    ) -> float:
        """Calculate distance between two locations in kilometers."""
        # Haversine formula
        R = 6371  # Earth's radius in kilometers

        lat1, lon1 = math.radians(loc1.latitude), math.radians(loc1.longitude)
        lat2, lon2 = math.radians(loc2.latitude), math.radians(loc2.longitude)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        return R * c

    def is_location_anomaly(
        self,
        user_location: GeographicLocation,
        new_location: GeographicLocation,
        threshold_km: float = 500,
    ) -> bool:
        """Check if a location represents an anomaly for fraud detection."""
        distance = self.calculate_distance(user_location, new_location)
        return distance > threshold_km


class FraudPatterns:
    """Generates fraud patterns for detection testing."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.fraud_rate = config.get("fraud_rate", 0.001)
        self.fraud_patterns = config.get("fraud_patterns", {})

    def should_inject_fraud(self) -> bool:
        """Determine if fraud should be injected based on configured rate."""
        return random.random() < self.fraud_rate

    def select_fraud_type(self) -> FraudType:
        """Select fraud type based on configured weights."""
        fraud_types = list(self.fraud_patterns.keys())
        weights = list(self.fraud_patterns.values())

        # Normalize weights
        total_weight = sum(weights)
        probabilities = [w / total_weight for w in weights]

        selected = np.random.choice(fraud_types, p=probabilities)
        return FraudType(selected)

    def generate_velocity_attack(
        self, user_id: str, base_timestamp: datetime
    ) -> List[Dict[str, Any]]:
        """Generate velocity attack pattern - multiple transactions in short time."""
        transactions = []
        num_transactions = random.randint(5, 15)

        for i in range(num_transactions):
            # Transactions within 5 minutes
            timestamp = base_timestamp + timedelta(seconds=random.randint(0, 300))

            transaction = {
                "user_id": user_id,
                "timestamp": timestamp,
                "amount": random.uniform(100, 500),
                "fraud_type": FraudType.VELOCITY_ATTACK.value,
                "fraud_indicator": f"velocity_{i+1}_of_{num_transactions}",
            }
            transactions.append(transaction)

        return transactions

    def generate_location_anomaly(
        self, user_location: GeographicLocation
    ) -> GeographicLocation:
        """Generate location anomaly - transaction from unusual location."""
        # Pick a location far from user's typical location
        regions = ["North America", "Europe", "Asia", "Others"]
        # Remove user's current region
        if user_location.region in regions:
            regions.remove(user_location.region)

        anomaly_region = random.choice(regions)
        geo_patterns = GeographicPatterns(self.config)

        return geo_patterns.generate_location(anomaly_region)

    def generate_amount_anomaly(self, user_avg_amount: float) -> float:
        """Generate amount anomaly - unusually high or low amount."""
        if random.random() < 0.7:
            # Unusually high amount
            return user_avg_amount * random.uniform(5, 20)
        else:
            # Unusually low amount (might be testing)
            return random.uniform(0.01, 1.0)

    def generate_time_anomaly(self, user_timezone: str) -> datetime:
        """Generate time anomaly - transaction at unusual time."""
        # Generate transaction at 2-4 AM in user's timezone
        now = datetime.now()
        anomaly_hour = random.randint(2, 4)

        return now.replace(hour=anomaly_hour, minute=random.randint(0, 59))

    def generate_device_anomaly(self) -> Dict[str, str]:
        """Generate device anomaly - new/unusual device."""
        return {
            "device_id": f"fraud_device_{random.randint(10000, 99999)}",
            "device_type": "unknown",
            "os": "unknown",
            "browser": "unknown",
            "first_seen": datetime.now().isoformat(),
        }

    def generate_pattern_anomaly(self, user_categories: List[str]) -> str:
        """Generate pattern anomaly - unusual category for user."""
        all_categories = self.config.get("categories", [])

        # Find categories user doesn't usually buy
        unusual_categories = [
            cat for cat in all_categories if cat not in user_categories
        ]

        if unusual_categories:
            return random.choice(unusual_categories)
        else:
            # If user buys everything, pick a random category
            return random.choice(all_categories)


class SeasonalPatterns:
    """Generates seasonal patterns and trends."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.seasonal_events = config.get("seasonal_events", {})

    def get_seasonal_multiplier(self, timestamp: datetime, category: str) -> float:
        """Get seasonal multiplier for a specific timestamp and category."""
        for event_name, event_data in self.seasonal_events.items():
            event_date = datetime.strptime(event_data["date"], "%Y-%m-%d")
            duration = timedelta(hours=event_data["duration_hours"])

            # Check if timestamp falls within event period
            if event_date <= timestamp <= event_date + duration:
                # Check if category is affected
                if category in event_data["categories"]:
                    return event_data["multiplier"]
                else:
                    # Other categories get moderate increase
                    return event_data["multiplier"] * 0.5

        # Check for general seasonal trends
        month = timestamp.month

        # Holiday shopping season
        if month in [11, 12]:
            return 1.5
        # Back to school season
        elif month in [8, 9]:
            return 1.3
        # Summer season
        elif month in [6, 7]:
            return 1.2
        # Regular season
        else:
            return 1.0

    def get_trending_categories(self, timestamp: datetime) -> List[str]:
        """Get categories that are trending at a specific time."""
        month = timestamp.month

        if month in [11, 12]:  # Holiday season
            return ["Electronics", "Toys & Games", "Jewelry & Accessories"]
        elif month in [8, 9]:  # Back to school
            return ["Books", "Electronics", "Clothing"]
        elif month in [6, 7]:  # Summer
            return ["Sports & Outdoors", "Clothing", "Travel"]
        elif month in [2]:  # Valentine's Day
            return ["Jewelry & Accessories", "Beauty & Personal Care"]
        else:
            return []

    def apply_seasonal_boost(
        self, category: str, base_probability: float, timestamp: datetime
    ) -> float:
        """Apply seasonal boost to category probability."""
        multiplier = self.get_seasonal_multiplier(timestamp, category)
        trending_categories = self.get_trending_categories(timestamp)

        boosted_probability = base_probability * multiplier

        # Additional boost for trending categories
        if category in trending_categories:
            boosted_probability *= 1.3

        return min(boosted_probability, 1.0)  # Cap at 1.0

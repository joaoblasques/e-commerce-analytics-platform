"""Transaction data producer for e-commerce transactions."""

import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from faker import Faker

from .base_producer import BaseProducer


class TransactionProducer(BaseProducer):
    """Producer for e-commerce transaction data."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions",
        producer_config: Optional[Dict[str, Any]] = None,
        generation_rate: float = 1000.0,  # transactions per hour
    ):
        """Initialize transaction producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            producer_config: Additional producer configuration
            generation_rate: Target transactions per hour
        """
        super().__init__(bootstrap_servers, topic, producer_config)
        self.generation_rate = generation_rate
        self.fake = Faker()

        # Initialize realistic data pools
        self._init_data_pools()

        # Transaction patterns
        self.business_hours = (9, 17)  # 9 AM to 5 PM
        self.peak_hours = (12, 14)  # 12 PM to 2 PM
        self.weekend_multiplier = 1.5  # Higher activity on weekends

    def _init_data_pools(self) -> None:
        """Initialize data pools for realistic transaction generation."""
        self.categories = [
            {"id": "cat_electronics", "name": "Electronics", "weight": 0.25},
            {"id": "cat_clothing", "name": "Clothing", "weight": 0.20},
            {"id": "cat_books", "name": "Books", "weight": 0.15},
            {"id": "cat_home", "name": "Home & Garden", "weight": 0.15},
            {"id": "cat_sports", "name": "Sports & Outdoors", "weight": 0.10},
            {"id": "cat_beauty", "name": "Beauty & Health", "weight": 0.10},
            {"id": "cat_toys", "name": "Toys & Games", "weight": 0.05},
        ]

        self.products = {
            "cat_electronics": [
                {
                    "name": "MacBook Pro 14-inch",
                    "price_range": (1500, 2500),
                    "brand": "Apple",
                },
                {"name": "iPhone 15 Pro", "price_range": (800, 1200), "brand": "Apple"},
                {
                    "name": "Samsung Galaxy S24",
                    "price_range": (700, 1000),
                    "brand": "Samsung",
                },
                {"name": "Dell XPS 13", "price_range": (1000, 1800), "brand": "Dell"},
                {"name": "Sony WH-1000XM5", "price_range": (300, 400), "brand": "Sony"},
            ],
            "cat_clothing": [
                {
                    "name": "Nike Air Max Sneakers",
                    "price_range": (80, 150),
                    "brand": "Nike",
                },
                {
                    "name": "Levi's 501 Jeans",
                    "price_range": (50, 100),
                    "brand": "Levi's",
                },
                {
                    "name": "North Face Jacket",
                    "price_range": (150, 300),
                    "brand": "North Face",
                },
                {"name": "Adidas T-Shirt", "price_range": (20, 50), "brand": "Adidas"},
                {"name": "Zara Dress", "price_range": (40, 80), "brand": "Zara"},
            ],
            "cat_books": [
                {
                    "name": "The Great Gatsby",
                    "price_range": (10, 25),
                    "brand": "Penguin",
                },
                {
                    "name": "Data Science Handbook",
                    "price_range": (30, 60),
                    "brand": "O'Reilly",
                },
                {
                    "name": "Harry Potter Series",
                    "price_range": (50, 100),
                    "brand": "Scholastic",
                },
                {"name": "The Art of War", "price_range": (8, 20), "brand": "Dover"},
                {
                    "name": "Cooking Basics",
                    "price_range": (15, 35),
                    "brand": "Williams Sonoma",
                },
            ],
        }

        self.payment_methods = [
            {"method": "credit_card", "provider": "stripe", "weight": 0.60},
            {"method": "debit_card", "provider": "stripe", "weight": 0.25},
            {"method": "paypal", "provider": "paypal", "weight": 0.10},
            {"method": "apple_pay", "provider": "apple", "weight": 0.05},
        ]

        self.currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]

        self.locations = [
            {"country": "US", "state": "CA", "city": "San Francisco", "zip": "94105"},
            {"country": "US", "state": "NY", "city": "New York", "zip": "10001"},
            {"country": "US", "state": "TX", "city": "Austin", "zip": "73301"},
            {"country": "US", "state": "WA", "city": "Seattle", "zip": "98101"},
            {"country": "CA", "state": "ON", "city": "Toronto", "zip": "M5H 2N2"},
            {"country": "GB", "state": "England", "city": "London", "zip": "SW1A 1AA"},
        ]

        self.devices = [
            {"type": "desktop", "os": "macOS", "browser": "Chrome", "weight": 0.35},
            {"type": "mobile", "os": "iOS", "browser": "Safari", "weight": 0.30},
            {"type": "mobile", "os": "Android", "browser": "Chrome", "weight": 0.25},
            {"type": "tablet", "os": "iPadOS", "browser": "Safari", "weight": 0.10},
        ]

        self.campaigns = [
            {
                "id": "summer_sale_2024",
                "channel": "google_ads",
                "referrer": "google.com",
            },
            {
                "id": "winter_promo_2024",
                "channel": "facebook_ads",
                "referrer": "facebook.com",
            },
            {"id": "email_campaign_q1", "channel": "email", "referrer": "email"},
            {
                "id": "influencer_collab",
                "channel": "social",
                "referrer": "instagram.com",
            },
            {"id": "organic_search", "channel": "organic", "referrer": "google.com"},
        ]

    def _weighted_choice(self, choices: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Make a weighted random choice from a list of options."""
        weights = [choice.get("weight", 1.0) for choice in choices]
        return random.choices(choices, weights=weights, k=1)[0]

    def _get_time_multiplier(self, timestamp: datetime) -> float:
        """Get activity multiplier based on time patterns."""
        hour = timestamp.hour
        day_of_week = timestamp.weekday()

        # Base multiplier
        multiplier = 1.0

        # Business hours boost
        if self.business_hours[0] <= hour <= self.business_hours[1]:
            multiplier *= 1.5

        # Peak hours boost
        if self.peak_hours[0] <= hour <= self.peak_hours[1]:
            multiplier *= 2.0

        # Weekend boost
        if day_of_week >= 5:  # Saturday and Sunday
            multiplier *= self.weekend_multiplier

        # Late night reduction
        if hour < 6 or hour > 22:
            multiplier *= 0.3

        return multiplier

    def generate_message(self) -> Dict[str, Any]:
        """Generate a realistic transaction message."""
        timestamp = datetime.utcnow()

        # Generate IDs
        transaction_id = (
            f"txn_{timestamp.strftime('%Y%m%d')}_{random.randint(100000000, 999999999)}"
        )
        user_id = f"user_{random.randint(100000000, 999999999)}"
        session_id = f"sess_{int(timestamp.timestamp())}_{user_id}"

        # Select category and product
        category = self._weighted_choice(self.categories)
        if category["id"] in self.products:
            product = random.choice(self.products[category["id"]])
        else:
            product = {
                "name": self.fake.catch_phrase(),
                "price_range": (10, 100),
                "brand": self.fake.company(),
            }

        product_id = f"prod_{category['id']}_{random.randint(1000, 9999)}"

        # Generate price and quantity
        base_price = random.uniform(*product["price_range"])
        quantity = random.choices(
            [1, 2, 3, 4, 5], weights=[0.7, 0.15, 0.08, 0.05, 0.02], k=1
        )[0]

        # Apply discount (20% chance)
        discount = random.uniform(0.05, 0.30) if random.random() < 0.2 else 0.0
        final_price = base_price * (1 - discount)

        # Select payment method and location
        payment = self._weighted_choice(self.payment_methods)
        location = random.choice(self.locations)
        device = self._weighted_choice(self.devices)
        campaign = random.choice(self.campaigns)

        # Generate coordinates near the city
        lat_offset = random.uniform(-0.1, 0.1)
        lon_offset = random.uniform(-0.1, 0.1)

        return {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "session_id": session_id,
            "product_id": product_id,
            "product_name": product["name"],
            "category_id": category["id"],
            "category_name": category["name"],
            "subcategory": self.fake.word().title(),
            "brand": product["brand"],
            "price": round(final_price, 2),
            "quantity": quantity,
            "discount_applied": round(discount, 2),
            "payment_method": payment["method"],
            "payment_provider": payment["provider"],
            "currency": random.choice(self.currencies),
            "timestamp": timestamp.isoformat() + "Z",
            "user_location": {
                "country": location["country"],
                "state": location["state"],
                "city": location["city"],
                "zip_code": location["zip"],
                "latitude": round(37.7749 + lat_offset, 6),
                "longitude": round(-122.4194 + lon_offset, 6),
            },
            "device_info": {
                "type": device["type"],
                "os": device["os"],
                "browser": device["browser"],
                "user_agent": self.fake.user_agent(),
            },
            "marketing_attribution": {
                "campaign_id": campaign["id"],
                "channel": campaign["channel"],
                "referrer": campaign["referrer"],
            },
        }

    def get_message_key(self, message: Dict[str, Any]) -> Optional[str]:
        """Get message key for partitioning by user_id."""
        return message.get("user_id")

    def get_delay_seconds(self) -> float:
        """Calculate delay between messages based on generation rate and time patterns."""
        current_time = datetime.utcnow()
        time_multiplier = self._get_time_multiplier(current_time)

        # Adjust rate based on time patterns
        adjusted_rate = self.generation_rate * time_multiplier

        # Convert to messages per second
        messages_per_second = adjusted_rate / 3600.0

        # Add some randomness (±20%)
        base_delay = 1.0 / messages_per_second
        random_factor = random.uniform(0.8, 1.2)

        return base_delay * random_factor

    def run_continuous(self, duration_seconds: Optional[int] = None) -> None:
        """Run the producer continuously.

        Args:
            duration_seconds: How long to run (None for infinite)
        """
        start_time = time.time()

        self.logger.info(
            f"Starting transaction producer with rate: {self.generation_rate} transactions/hour"
        )

        try:
            while True:
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break

                message = self.generate_message()
                key = self.get_message_key(message)

                if self.send_message(message, key):
                    self.logger.debug(f"Sent transaction: {message['transaction_id']}")

                # Wait before next message
                delay = self.get_delay_seconds()
                time.sleep(delay)

        except KeyboardInterrupt:
            self.logger.info("Stopping transaction producer...")
        finally:
            self.flush()
            self.close()

            # Log final metrics
            metrics = self.get_metrics()
            self.logger.info(f"Final metrics: {metrics}")

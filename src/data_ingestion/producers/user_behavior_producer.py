"""User behavior event producer for website interaction tracking."""

import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from faker import Faker

from .base_producer import BaseProducer


class UserBehaviorProducer(BaseProducer):
    """Producer for user behavior events and website interactions."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "user-events",
        producer_config: Optional[Dict[str, Any]] = None,
        generation_rate: float = 5000.0,  # events per hour
        session_duration_range: Tuple[int, int] = (60, 1800),  # 1-30 minutes
    ):
        """Initialize user behavior producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            producer_config: Additional producer configuration
            generation_rate: Target events per hour
            session_duration_range: Min/max session duration in seconds
        """
        super().__init__(bootstrap_servers, topic, producer_config)
        self.generation_rate = generation_rate
        self.session_duration_range = session_duration_range
        self.fake = Faker()

        # Active sessions tracking
        self.active_sessions: Dict[str, Dict[str, Any]] = {}
        self.session_cleanup_interval = 300  # Clean up sessions every 5 minutes
        self.last_cleanup = time.time()

        # Initialize data pools
        self._init_data_pools()

        # Behavioral patterns
        self.business_hours = (9, 17)
        self.peak_hours = (19, 22)  # Evening peak for e-commerce
        self.weekend_multiplier = 1.8  # Higher weekend activity

    def _init_data_pools(self) -> None:
        """Initialize data pools for realistic user behavior generation."""
        self.event_types = [
            {"type": "page_view", "weight": 0.40},
            {"type": "product_view", "weight": 0.25},
            {"type": "search", "weight": 0.10},
            {"type": "add_to_cart", "weight": 0.08},
            {"type": "remove_from_cart", "weight": 0.02},
            {"type": "begin_checkout", "weight": 0.05},
            {"type": "complete_checkout", "weight": 0.02},
            {"type": "wishlist_add", "weight": 0.03},
            {"type": "review_write", "weight": 0.01},
            {"type": "contact_support", "weight": 0.01},
            {"type": "newsletter_signup", "weight": 0.01},
            {"type": "account_create", "weight": 0.01},
            {"type": "login", "weight": 0.04},
            {"type": "logout", "weight": 0.01},
        ]

        self.page_categories = [
            {
                "category": "home",
                "pages": ["home", "featured", "deals"],
                "weight": 0.25,
            },
            {
                "category": "product",
                "pages": ["product_detail", "product_list"],
                "weight": 0.30,
            },
            {
                "category": "category",
                "pages": ["electronics", "clothing", "books", "home"],
                "weight": 0.20,
            },
            {
                "category": "checkout",
                "pages": ["cart", "checkout", "payment"],
                "weight": 0.08,
            },
            {
                "category": "account",
                "pages": ["profile", "orders", "wishlist"],
                "weight": 0.10,
            },
            {
                "category": "support",
                "pages": ["help", "contact", "faq"],
                "weight": 0.05,
            },
            {
                "category": "about",
                "pages": ["about", "careers", "privacy"],
                "weight": 0.02,
            },
        ]

        self.user_segments = [
            {
                "segment": "new_visitor",
                "weight": 0.35,
                "session_length_multiplier": 0.6,
            },
            {
                "segment": "returning_visitor",
                "weight": 0.40,
                "session_length_multiplier": 1.0,
            },
            {
                "segment": "loyal_customer",
                "weight": 0.20,
                "session_length_multiplier": 1.5,
            },
            {"segment": "power_user", "weight": 0.05, "session_length_multiplier": 2.0},
        ]

        self.devices = [
            {"type": "mobile", "os": "iOS", "browser": "Safari", "weight": 0.35},
            {"type": "mobile", "os": "Android", "browser": "Chrome", "weight": 0.30},
            {"type": "desktop", "os": "Windows", "browser": "Chrome", "weight": 0.20},
            {"type": "desktop", "os": "macOS", "browser": "Safari", "weight": 0.10},
            {"type": "tablet", "os": "iPadOS", "browser": "Safari", "weight": 0.05},
        ]

        self.locations = [
            {
                "country": "US",
                "state": "CA",
                "city": "San Francisco",
                "timezone": "America/Los_Angeles",
            },
            {
                "country": "US",
                "state": "NY",
                "city": "New York",
                "timezone": "America/New_York",
            },
            {
                "country": "US",
                "state": "TX",
                "city": "Austin",
                "timezone": "America/Chicago",
            },
            {
                "country": "CA",
                "state": "ON",
                "city": "Toronto",
                "timezone": "America/Toronto",
            },
            {
                "country": "GB",
                "state": "England",
                "city": "London",
                "timezone": "Europe/London",
            },
            {
                "country": "DE",
                "state": "Berlin",
                "city": "Berlin",
                "timezone": "Europe/Berlin",
            },
            {
                "country": "AU",
                "state": "NSW",
                "city": "Sydney",
                "timezone": "Australia/Sydney",
            },
        ]

        self.referrers = [
            {"type": "organic", "source": "google.com", "weight": 0.40},
            {"type": "direct", "source": "direct", "weight": 0.25},
            {"type": "social", "source": "facebook.com", "weight": 0.15},
            {"type": "paid", "source": "google_ads", "weight": 0.10},
            {"type": "email", "source": "email_campaign", "weight": 0.05},
            {"type": "referral", "source": "affiliate_site", "weight": 0.05},
        ]

        # Product categories for product-related events
        self.product_categories = [
            "electronics",
            "clothing",
            "books",
            "home",
            "sports",
            "beauty",
            "toys",
        ]

        # Journey patterns (probability of next event type given current event)
        self.journey_transitions = {
            "page_view": {
                "page_view": 0.35,
                "product_view": 0.25,
                "search": 0.15,
                "add_to_cart": 0.05,
                "login": 0.10,
                "logout": 0.10,
            },
            "product_view": {
                "product_view": 0.30,
                "add_to_cart": 0.20,
                "wishlist_add": 0.10,
                "page_view": 0.25,
                "begin_checkout": 0.05,
                "logout": 0.10,
            },
            "search": {
                "product_view": 0.40,
                "search": 0.20,
                "page_view": 0.25,
                "add_to_cart": 0.05,
                "logout": 0.10,
            },
            "add_to_cart": {
                "product_view": 0.30,
                "begin_checkout": 0.25,
                "page_view": 0.20,
                "add_to_cart": 0.10,
                "remove_from_cart": 0.05,
                "logout": 0.10,
            },
            "begin_checkout": {
                "complete_checkout": 0.60,
                "page_view": 0.20,
                "add_to_cart": 0.10,
                "logout": 0.10,
            },
        }

    def _weighted_choice(self, choices: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Make a weighted random choice from a list of options."""
        weights = [choice.get("weight", 1.0) for choice in choices]
        return random.choices(choices, weights=weights, k=1)[0]

    def _get_time_multiplier(self, timestamp: datetime) -> float:
        """Get activity multiplier based on time patterns."""
        hour = timestamp.hour
        day_of_week = timestamp.weekday()

        multiplier = 1.0

        # Business hours boost
        if self.business_hours[0] <= hour <= self.business_hours[1]:
            multiplier *= 1.2

        # Evening peak for e-commerce
        if self.peak_hours[0] <= hour <= self.peak_hours[1]:
            multiplier *= 2.0

        # Weekend boost
        if day_of_week >= 5:
            multiplier *= self.weekend_multiplier

        # Late night/early morning reduction
        if hour < 6 or hour > 23:
            multiplier *= 0.2

        return multiplier

    def _create_session(self, user_id: str, timestamp: datetime) -> Dict[str, Any]:
        """Create a new user session."""
        session_id = f"sess_{int(timestamp.timestamp())}_{user_id}"
        user_segment = self._weighted_choice(self.user_segments)
        device = self._weighted_choice(self.devices)
        location = random.choice(self.locations)
        referrer = self._weighted_choice(self.referrers)

        # Calculate session duration based on user segment
        base_duration = random.randint(*self.session_duration_range)
        session_duration = int(
            base_duration * user_segment["session_length_multiplier"]
        )

        session = {
            "session_id": session_id,
            "user_id": user_id,
            "user_segment": user_segment["segment"],
            "device_info": device,
            "location": location,
            "referrer": referrer,
            "session_start": timestamp,
            "session_end": timestamp + timedelta(seconds=session_duration),
            "events_count": 0,
            "last_event_type": None,
            "cart_items": [],
            "page_views": [],
        }

        return session

    def _cleanup_expired_sessions(self) -> None:
        """Remove expired sessions from active sessions."""
        current_time = datetime.utcnow()
        expired_sessions = []

        for session_id, session in self.active_sessions.items():
            if current_time > session["session_end"]:
                expired_sessions.append(session_id)

        for session_id in expired_sessions:
            del self.active_sessions[session_id]

        if expired_sessions:
            self.logger.debug(f"Cleaned up {len(expired_sessions)} expired sessions")

    def _get_or_create_session(self, timestamp: datetime) -> Dict[str, Any]:
        """Get an existing session or create a new one."""
        # Cleanup expired sessions periodically
        if time.time() - self.last_cleanup > self.session_cleanup_interval:
            self._cleanup_expired_sessions()
            self.last_cleanup = time.time()

        # Try to reuse existing session (70% chance)
        if self.active_sessions and random.random() < 0.7:
            session = random.choice(list(self.active_sessions.values()))
            if timestamp < session["session_end"]:
                return session

        # Create new session
        user_id = f"user_{random.randint(100000000, 999999999)}"
        session = self._create_session(user_id, timestamp)
        self.active_sessions[session["session_id"]] = session

        return session

    def _get_next_event_type(self, current_event_type: Optional[str]) -> str:
        """Get next event type based on user journey patterns."""
        if current_event_type and current_event_type in self.journey_transitions:
            transitions = self.journey_transitions[current_event_type]
            event_types = list(transitions.keys())
            weights = list(transitions.values())
            return random.choices(event_types, weights=weights, k=1)[0]

        # Default to weighted random choice
        return self._weighted_choice(self.event_types)["type"]

    def _generate_page_info(
        self, event_type: str, session: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate page information based on event type."""
        if event_type == "product_view":
            category = random.choice(self.product_categories)
            product_id = f"prod_{category}_{random.randint(1000, 9999)}"
            return {
                "page_url": f"/product/{product_id}",
                "page_title": f"{self.fake.catch_phrase()} - Product",
                "page_category": "product",
                "product_id": product_id,
                "product_category": category,
            }
        elif event_type == "search":
            search_term = random.choice(
                [
                    self.fake.word(),
                    f"{random.choice(self.product_categories)} {self.fake.word()}",
                    self.fake.catch_phrase(),
                ]
            )
            return {
                "page_url": f"/search?q={search_term.replace(' ', '+')}",
                "page_title": f"Search: {search_term}",
                "page_category": "search",
                "search_term": search_term,
                "search_results_count": random.randint(0, 1000),
            }
        elif event_type in ["add_to_cart", "remove_from_cart"]:
            category = random.choice(self.product_categories)
            product_id = f"prod_{category}_{random.randint(1000, 9999)}"
            return {
                "page_url": f"/product/{product_id}",
                "page_title": f"{self.fake.catch_phrase()} - Product",
                "page_category": "product",
                "product_id": product_id,
                "product_category": category,
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(10, 500), 2),
            }
        elif event_type == "begin_checkout":
            return {
                "page_url": "/checkout",
                "page_title": "Checkout",
                "page_category": "checkout",
                "cart_value": round(random.uniform(50, 1000), 2),
                "cart_items_count": random.randint(1, 10),
            }
        elif event_type == "complete_checkout":
            return {
                "page_url": "/checkout/complete",
                "page_title": "Order Complete",
                "page_category": "checkout",
                "order_id": f"ord_{int(datetime.utcnow().timestamp())}_{random.randint(1000, 9999)}",
                "order_value": round(random.uniform(50, 1000), 2),
                "payment_method": random.choice(["credit_card", "paypal", "apple_pay"]),
            }
        else:
            # Generic page view
            page_cat = self._weighted_choice(self.page_categories)
            page_name = random.choice(page_cat["pages"])
            return {
                "page_url": f"/{page_name}",
                "page_title": f"{page_name.replace('_', ' ').title()}",
                "page_category": page_cat["category"],
            }

    def generate_message(self) -> Dict[str, Any]:
        """Generate a realistic user behavior event message."""
        timestamp = datetime.utcnow()

        # Get or create session
        session = self._get_or_create_session(timestamp)

        # Get next event type based on journey patterns
        event_type = self._get_next_event_type(session["last_event_type"])

        # Generate page information
        page_info = self._generate_page_info(event_type, session)

        # Update session state
        session["events_count"] += 1
        session["last_event_type"] = event_type

        # Generate event ID
        event_id = f"evt_{int(timestamp.timestamp())}_{random.randint(100000, 999999)}"

        # Generate additional event-specific data
        event_data = {}
        if event_type == "search":
            event_data = {
                "search_term": page_info.get("search_term"),
                "search_results_count": page_info.get("search_results_count"),
                "search_duration_ms": random.randint(500, 5000),
            }
        elif event_type in ["add_to_cart", "remove_from_cart"]:
            event_data = {
                "product_id": page_info.get("product_id"),
                "product_category": page_info.get("product_category"),
                "quantity": page_info.get("quantity"),
                "price": page_info.get("price"),
            }
        elif event_type == "complete_checkout":
            event_data = {
                "order_id": page_info.get("order_id"),
                "order_value": page_info.get("order_value"),
                "payment_method": page_info.get("payment_method"),
            }

        # Generate coordinates near the city
        lat_offset = random.uniform(-0.1, 0.1)
        lon_offset = random.uniform(-0.1, 0.1)

        return {
            "event_id": event_id,
            "event_type": event_type,
            "timestamp": timestamp.isoformat() + "Z",
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "user_segment": session["user_segment"],
            "page_info": page_info,
            "event_data": event_data,
            "device_info": {
                "type": session["device_info"]["type"],
                "os": session["device_info"]["os"],
                "browser": session["device_info"]["browser"],
                "user_agent": self.fake.user_agent(),
                "screen_resolution": f"{random.choice(['1920x1080', '1366x768', '1440x900', '375x667', '414x896'])}",
                "viewport_size": f"{random.randint(300, 1920)}x{random.randint(600, 1080)}",
            },
            "location": {
                "country": session["location"]["country"],
                "state": session["location"]["state"],
                "city": session["location"]["city"],
                "timezone": session["location"]["timezone"],
                "latitude": round(37.7749 + lat_offset, 6),
                "longitude": round(-122.4194 + lon_offset, 6),
            },
            "referrer": {
                "type": session["referrer"]["type"],
                "source": session["referrer"]["source"],
                "utm_source": session["referrer"].get("utm_source"),
                "utm_medium": session["referrer"].get("utm_medium"),
                "utm_campaign": session["referrer"].get("utm_campaign"),
            },
            "session_info": {
                "session_start": session["session_start"].isoformat() + "Z",
                "events_in_session": session["events_count"],
                "session_duration_so_far": int(
                    (timestamp - session["session_start"]).total_seconds()
                ),
                "is_new_session": session["events_count"] == 1,
            },
            "technical_info": {
                "ip_address": self.fake.ipv4(),
                "user_agent": self.fake.user_agent(),
                "page_load_time_ms": random.randint(500, 5000),
                "time_on_page_ms": random.randint(1000, 300000),
            },
        }

    def get_message_key(self, message: Dict[str, Any]) -> Optional[str]:
        """Get message key for partitioning by session_id."""
        return message.get("session_id")

    def get_delay_seconds(self) -> float:
        """Calculate delay between messages based on generation rate and time patterns."""
        current_time = datetime.utcnow()
        time_multiplier = self._get_time_multiplier(current_time)

        # Adjust rate based on time patterns
        adjusted_rate = self.generation_rate * time_multiplier

        # Convert to messages per second
        messages_per_second = adjusted_rate / 3600.0

        # Add some randomness (±30% for more realistic user behavior)
        base_delay = 1.0 / messages_per_second
        random_factor = random.uniform(0.7, 1.3)

        return base_delay * random_factor

    def run_continuous(self, duration_seconds: Optional[int] = None) -> None:
        """Run the producer continuously.

        Args:
            duration_seconds: How long to run (None for infinite)
        """
        start_time = time.time()

        self.logger.info(
            f"Starting user behavior producer with rate: {self.generation_rate} events/hour"
        )

        try:
            while True:
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break

                message = self.generate_message()
                key = self.get_message_key(message)

                if self.send_message(message, key):
                    self.logger.debug(
                        f"Sent event: {message['event_type']} for session: {message['session_id']}"
                    )

                # Wait before next message
                delay = self.get_delay_seconds()
                time.sleep(delay)

        except KeyboardInterrupt:
            self.logger.info("Stopping user behavior producer...")
        finally:
            self.flush()
            self.close()

            # Log final metrics
            metrics = self.get_metrics()
            active_sessions_count = len(self.active_sessions)
            self.logger.info(f"Final metrics: {metrics}")
            self.logger.info(f"Active sessions at shutdown: {active_sessions_count}")

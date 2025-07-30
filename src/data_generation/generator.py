"""
Core data generation framework for e-commerce analytics platform.
"""

import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json
import hashlib

from faker import Faker
import numpy as np

from .config import DataGenerationConfig
from .patterns import (
    TemporalPatterns, GeographicPatterns, FraudPatterns, 
    SeasonalPatterns, FraudType, GeographicLocation
)


class ECommerceDataGenerator:
    """
    Comprehensive e-commerce data generator with realistic patterns.
    
    This generator creates realistic e-commerce data including:
    - Customer profiles with demographics
    - Product catalogs with categories and pricing
    - Transaction data with temporal patterns
    - User behavior events with session tracking
    - Fraud patterns for detection testing
    - Geographic and seasonal variations
    """
    
    def __init__(self, config: DataGenerationConfig = None):
        """Initialize the data generator with configuration."""
        self.config = config or DataGenerationConfig()
        self.faker = Faker()
        
        # Initialize pattern generators
        self.temporal_patterns = TemporalPatterns(self.config.__dict__)
        self.geographic_patterns = GeographicPatterns(self.config.__dict__)
        self.fraud_patterns = FraudPatterns(self.config.__dict__)
        self.seasonal_patterns = SeasonalPatterns(self.config.__dict__)
        
        # Initialize data caches
        self.users: Dict[str, Dict[str, Any]] = {}
        self.products: Dict[str, Dict[str, Any]] = {}
        self.sessions: Dict[str, Dict[str, Any]] = {}
        
        # Initialize user behavior tracking
        self.user_purchase_history: Dict[str, List[str]] = {}
        self.user_locations: Dict[str, GeographicLocation] = {}
        self.user_devices: Dict[str, Dict[str, Any]] = {}
        
        # Generate base data
        self._generate_users()
        self._generate_products()
        
    def _generate_users(self) -> None:
        """Generate realistic user base."""
        print(f"Generating {self.config.total_users} users...")
        
        for i in range(self.config.total_users):
            user_id = f"user_{i+1:06d}"
            
            # Select age group
            age_group = np.random.choice(
                list(self.config.age_distribution.keys()),
                p=list(self.config.age_distribution.values())
            )
            age_range = self.config.age_ranges[age_group]
            age = random.randint(*age_range)
            
            # Generate user profile
            profile = self.faker.profile()
            location = self.geographic_patterns.generate_location()
            
            # Determine user type
            user_type = "regular"
            if random.random() < self.config.vip_user_ratio:
                user_type = "vip"
            elif random.random() < 0.2:  # 20% new users
                user_type = "new"
            
            # Generate marketing attribution
            marketing_channel = np.random.choice(
                list(self.config.marketing_channels.keys()),
                p=list(self.config.marketing_channels.values())
            )
            
            user = {
                'user_id': user_id,
                'email': profile['mail'],
                'name': profile['name'],
                'age': age,
                'age_group': age_group,
                'gender': random.choice(['M', 'F', 'O']),
                'location': location.__dict__,
                'user_type': user_type,
                'registration_date': self.faker.date_between(start_date='-2y', end_date='today'),
                'marketing_channel': marketing_channel,
                'is_active': random.random() < self.config.active_user_ratio,
                'preferred_categories': self._generate_user_preferences(),
                'avg_order_value': random.uniform(50, 500),
                'total_orders': random.randint(1, 100) if user_type != "new" else 0,
                'created_at': datetime.now() - timedelta(days=random.randint(1, 730))
            }
            
            self.users[user_id] = user
            self.user_locations[user_id] = location
            self.user_purchase_history[user_id] = []
            
            # Generate device profile
            device_type = np.random.choice(
                list(self.config.device_types.keys()),
                p=list(self.config.device_types.values())
            )
            
            os_options = self.config.operating_systems[device_type]
            os = np.random.choice(list(os_options.keys()), p=list(os_options.values()))
            
            browser = np.random.choice(
                list(self.config.browsers.keys()),
                p=list(self.config.browsers.values())
            )
            
            self.user_devices[user_id] = {
                'device_id': f"device_{hashlib.md5(user_id.encode(), usedforsecurity=False).hexdigest()[:8]}",
                'device_type': device_type,
                'os': os,
                'browser': browser,
                'screen_resolution': self._generate_screen_resolution(device_type),
                'user_agent': self._generate_user_agent(device_type, os, browser)
            }
    
    def _generate_products(self) -> None:
        """Generate realistic product catalog."""
        print(f"Generating {self.config.total_products} products...")
        
        for i in range(self.config.total_products):
            product_id = f"prod_{i+1:06d}"
            
            # Select category
            category = np.random.choice(
                self.config.categories,
                p=[self.config.get_category_weight(cat) for cat in self.config.categories]
            )
            
            # Generate product details
            price_range = self.config.price_ranges[category]
            price = round(random.uniform(*price_range), 2)
            
            # Generate realistic product name
            product_name = self._generate_product_name(category)
            
            product = {
                'product_id': product_id,
                'name': product_name,
                'category': category,
                'price': price,
                'cost': round(price * random.uniform(0.3, 0.7), 2),  # 30-70% margin
                'brand': self._generate_brand_name(category),
                'description': self._generate_product_description(category, product_name),
                'weight_kg': round(random.uniform(0.1, 10.0), 2),
                'dimensions': f"{random.randint(5, 50)}x{random.randint(5, 50)}x{random.randint(5, 50)}",
                'sku': f"SKU-{category[:3].upper()}-{i+1:06d}",
                'stock_quantity': random.randint(0, 1000),
                'rating': round(random.uniform(3.0, 5.0), 1),
                'review_count': random.randint(0, 500),
                'created_at': self.faker.date_between(start_date='-1y', end_date='today'),
                'is_active': random.random() < 0.95,  # 95% active products
                'tags': self._generate_product_tags(category)
            }
            
            self.products[product_id] = product
    
    def _generate_user_preferences(self) -> List[str]:
        """Generate realistic user category preferences."""
        num_preferences = random.randint(2, 5)
        return random.sample(self.config.categories, num_preferences)
    
    def _generate_screen_resolution(self, device_type: str) -> str:
        """Generate realistic screen resolution based on device type."""
        if device_type == "desktop":
            resolutions = ["1920x1080", "2560x1440", "1366x768", "1680x1050", "3840x2160"]
        elif device_type == "mobile":
            resolutions = ["375x667", "414x896", "360x640", "412x915", "390x844"]
        else:  # tablet
            resolutions = ["768x1024", "1024x768", "820x1180", "1112x834", "1366x1024"]
        
        return random.choice(resolutions)
    
    def _generate_user_agent(self, device_type: str, os: str, browser: str) -> str:
        """Generate realistic user agent string."""
        if device_type == "desktop":
            if os == "Windows":
                return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/91.0.4472.124"
            elif os == "macOS":
                return f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/91.0.4472.124"
            else:  # Linux
                return f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/91.0.4472.124"
        else:  # mobile/tablet
            if os == "iOS":
                return f"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
            else:  # Android
                return f"Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/91.0.4472.120 Mobile Safari/537.36"
    
    def _generate_product_name(self, category: str) -> str:
        """Generate realistic product name based on category."""
        name_templates = {
            'Electronics': [
                "Ultra HD {adjective} {device}",
                "Professional {adjective} {device}",
                "Smart {adjective} {device}",
                "Wireless {adjective} {device}"
            ],
            'Clothing': [
                "{adjective} {garment}",
                "Premium {adjective} {garment}",
                "Casual {adjective} {garment}",
                "Designer {adjective} {garment}"
            ],
            'Books': [
                "The {adjective} {topic}",
                "Guide to {topic}",
                "Understanding {topic}",
                "Complete {topic} Handbook"
            ]
        }
        
        adjectives = ["Premium", "Professional", "Ultra", "Advanced", "Classic", "Modern", "Deluxe", "Essential"]
        
        # Category-specific terms
        category_terms = {
            'Electronics': ["Monitor", "Keyboard", "Mouse", "Headphones", "Speaker", "Camera"],
            'Clothing': ["Shirt", "Pants", "Jacket", "Dress", "Sweater", "Shoes"],
            'Books': ["Business", "Technology", "Science", "History", "Fiction", "Biography"],
            'Home & Garden': ["Lamp", "Chair", "Table", "Decor", "Plant", "Tool"],
            'Sports & Outdoors': ["Ball", "Equipment", "Gear", "Apparel", "Accessory", "Kit"]
        }
        
        if category in name_templates:
            template = random.choice(name_templates[category])
            return template.format(
                adjective=random.choice(adjectives),
                device=random.choice(category_terms.get(category, ["Item"])),
                garment=random.choice(category_terms.get(category, ["Item"])),
                topic=random.choice(category_terms.get(category, ["Subject"]))
            )
        else:
            return f"{random.choice(adjectives)} {category} Item"
    
    def _generate_brand_name(self, category: str) -> str:
        """Generate realistic brand name."""
        brand_prefixes = ["Tech", "Pro", "Ultra", "Prime", "Elite", "Smart", "Neo", "Alpha"]
        brand_suffixes = ["Corp", "Inc", "Tech", "Solutions", "Systems", "Labs", "Works", "Co"]
        
        return f"{random.choice(brand_prefixes)}{random.choice(brand_suffixes)}"
    
    def _generate_product_description(self, category: str, name: str) -> str:
        """Generate realistic product description."""
        descriptions = [
            f"High-quality {name.lower()} designed for professionals and enthusiasts.",
            f"Premium {name.lower()} with advanced features and excellent build quality.",
            f"Reliable {name.lower()} that delivers exceptional performance and value.",
            f"Innovative {name.lower()} with cutting-edge technology and design."
        ]
        return random.choice(descriptions)
    
    def _generate_product_tags(self, category: str) -> List[str]:
        """Generate relevant tags for products."""
        base_tags = ["popular", "bestseller", "quality", "reliable"]
        category_tags = {
            'Electronics': ["tech", "digital", "smart", "wireless"],
            'Clothing': ["fashion", "style", "comfortable", "trendy"],
            'Books': ["educational", "informative", "bestseller", "classic"],
            'Home & Garden': ["home", "decor", "functional", "stylish"],
            'Sports & Outdoors': ["fitness", "outdoor", "performance", "durable"]
        }
        
        tags = base_tags + category_tags.get(category, [])
        return random.sample(tags, random.randint(2, 4))
    
    def generate_transactions(self, count: int, 
                            start_time: datetime = None,
                            end_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate realistic transaction data."""
        if start_time is None:
            start_time = datetime.now() - timedelta(days=30)
        if end_time is None:
            end_time = datetime.now()
        
        transactions = []
        print(f"Generating {count} transactions...")
        
        for i in range(count):
            # Select user (weighted by activity)
            active_users = [uid for uid, user in self.users.items() if user['is_active']]
            user_id = random.choice(active_users)
            user = self.users[user_id]
            
            # Generate timestamp with temporal patterns
            timestamp = self._generate_weighted_timestamp(start_time, end_time)
            temporal_context = self.temporal_patterns.get_temporal_context(timestamp)
            
            # Select products based on user preferences and seasonal patterns
            products = self._select_products_for_user(user, temporal_context)
            
            # Check if fraud should be injected
            is_fraud = self.fraud_patterns.should_inject_fraud()
            fraud_type = None
            fraud_indicators = {}
            
            if is_fraud:
                fraud_type = self.fraud_patterns.select_fraud_type()
                fraud_indicators = self._apply_fraud_pattern(user, temporal_context, fraud_type)
            
            # Generate transaction
            transaction = self._create_transaction(
                user, products, timestamp, temporal_context,
                is_fraud, fraud_type, fraud_indicators
            )
            
            transactions.append(transaction)
            
            # Update user purchase history
            for product in products:
                self.user_purchase_history[user_id].append(product['category'])
        
        return transactions
    
    def generate_user_events(self, count: int,
                           start_time: datetime = None,
                           end_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate realistic user behavior events."""
        if start_time is None:
            start_time = datetime.now() - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now()
        
        events = []
        print(f"Generating {count} user events...")
        
        for i in range(count):
            # Select user
            user_id = random.choice(list(self.users.keys()))
            user = self.users[user_id]
            
            # Generate or continue session
            session_id = self._get_or_create_session(user_id, start_time, end_time)
            
            # Generate timestamp
            timestamp = self._generate_weighted_timestamp(start_time, end_time)
            
            # Select event type
            event_type = np.random.choice(
                list(self.config.event_types.keys()),
                p=[data['weight'] for data in self.config.event_types.values()]
            )
            
            # Generate event
            event = self._create_user_event(user, session_id, event_type, timestamp)
            events.append(event)
        
        return events
    
    def generate_product_updates(self, count: int) -> List[Dict[str, Any]]:
        """Generate product catalog update events."""
        updates = []
        print(f"Generating {count} product updates...")
        
        for i in range(count):
            product_id = random.choice(list(self.products.keys()))
            product = self.products[product_id]
            
            update_type = random.choice(['price_change', 'stock_update', 'description_update', 'status_change'])
            
            update = {
                'product_id': product_id,
                'update_type': update_type,
                'timestamp': datetime.now(),
                'old_value': self._get_product_field_value(product, update_type),
                'new_value': self._generate_new_product_value(product, update_type),
                'updated_by': 'system'
            }
            
            updates.append(update)
        
        return updates
    
    def _generate_weighted_timestamp(self, start_time: datetime, end_time: datetime) -> datetime:
        """Generate timestamp weighted by business hours and patterns."""
        # Generate random timestamp in range
        time_diff = end_time - start_time
        random_seconds = random.randint(0, int(time_diff.total_seconds()))
        base_timestamp = start_time + timedelta(seconds=random_seconds)
        
        # Apply temporal patterns
        return self.temporal_patterns.generate_realistic_timestamp(base_timestamp)
    
    def _select_products_for_user(self, user: Dict[str, Any], 
                                temporal_context) -> List[Dict[str, Any]]:
        """Select products for a user based on preferences and patterns."""
        # Number of products in order
        num_products = random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.07, 0.03])[0]
        
        selected_products = []
        preferred_categories = user['preferred_categories']
        
        for _ in range(num_products):
            # Weight selection by user preferences and seasonal patterns
            category_weights = []
            for category in self.config.categories:
                weight = self.config.get_category_weight(category)
                
                # Boost for user preferences
                if category in preferred_categories:
                    weight *= 3.0
                
                # Apply seasonal boost
                weight = self.seasonal_patterns.apply_seasonal_boost(
                    category, weight, temporal_context.timestamp
                )
                
                category_weights.append(weight)
            
            # Normalize weights
            total_weight = sum(category_weights)
            category_probs = [w / total_weight for w in category_weights]
            
            # Select category
            selected_category = np.random.choice(self.config.categories, p=category_probs)
            
            # Select product from category
            category_products = [p for p in self.products.values() 
                               if p['category'] == selected_category and p['is_active']]
            
            if category_products:
                product = random.choice(category_products)
                selected_products.append(product)
        
        return selected_products
    
    def _apply_fraud_pattern(self, user: Dict[str, Any], temporal_context,
                           fraud_type: FraudType) -> Dict[str, Any]:
        """Apply specific fraud pattern and return indicators."""
        fraud_indicators = {'fraud_type': fraud_type.value}
        
        if fraud_type == FraudType.LOCATION_ANOMALY:
            # Generate location far from user's typical location
            user_location = self.user_locations[user['user_id']]
            fraud_location = self.fraud_patterns.generate_location_anomaly(user_location)
            fraud_indicators['anomaly_location'] = fraud_location.__dict__
            fraud_indicators['distance_km'] = self.geographic_patterns.calculate_distance(
                user_location, fraud_location
            )
        
        elif fraud_type == FraudType.AMOUNT_ANOMALY:
            # Generate unusually high or low amount
            avg_amount = user['avg_order_value']
            fraud_indicators['anomaly_amount'] = self.fraud_patterns.generate_amount_anomaly(avg_amount)
            fraud_indicators['user_avg_amount'] = avg_amount
        
        elif fraud_type == FraudType.TIME_ANOMALY:
            # Generate transaction at unusual time
            fraud_indicators['anomaly_time'] = self.fraud_patterns.generate_time_anomaly(
                user['location']['timezone']
            )
        
        elif fraud_type == FraudType.DEVICE_ANOMALY:
            # Generate new/unusual device
            fraud_indicators['anomaly_device'] = self.fraud_patterns.generate_device_anomaly()
        
        elif fraud_type == FraudType.PATTERN_ANOMALY:
            # Generate unusual category purchase
            user_categories = self.user_purchase_history.get(user['user_id'], [])
            fraud_indicators['anomaly_category'] = self.fraud_patterns.generate_pattern_anomaly(
                user_categories
            )
        
        return fraud_indicators
    
    def _create_transaction(self, user: Dict[str, Any], products: List[Dict[str, Any]],
                          timestamp: datetime, temporal_context,
                          is_fraud: bool, fraud_type: FraudType,
                          fraud_indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Create a complete transaction record."""
        transaction_id = str(uuid.uuid4())
        
        # Calculate totals
        subtotal = sum(p['price'] for p in products)
        tax_rate = 0.08  # 8% tax
        tax_amount = round(subtotal * tax_rate, 2)
        total_amount = round(subtotal + tax_amount, 2)
        
        # Apply fraud amount if relevant
        if is_fraud and fraud_type == FraudType.AMOUNT_ANOMALY:
            total_amount = fraud_indicators['anomaly_amount']
        
        # Select payment method
        payment_method = np.random.choice(
            list(self.config.payment_methods.keys()),
            p=list(self.config.payment_methods.values())
        )
        
        # Location (use fraud location if relevant)
        if is_fraud and fraud_type == FraudType.LOCATION_ANOMALY:
            location = fraud_indicators['anomaly_location']
        else:
            location = user['location']
        
        # Device info (use fraud device if relevant)
        if is_fraud and fraud_type == FraudType.DEVICE_ANOMALY:
            device_info = fraud_indicators['anomaly_device']
        else:
            device_info = self.user_devices[user['user_id']]
        
        # Create transaction
        transaction = {
            'transaction_id': transaction_id,
            'user_id': user['user_id'],
            'timestamp': timestamp.isoformat(),
            'products': [
                {
                    'product_id': p['product_id'],
                    'name': p['name'],
                    'category': p['category'],
                    'price': p['price'],
                    'quantity': 1
                }
                for p in products
            ],
            'subtotal': subtotal,
            'tax_amount': tax_amount,
            'total_amount': total_amount,
            'payment_method': payment_method,
            'location': location,
            'device_info': device_info,
            'marketing_attribution': {
                'channel': user['marketing_channel'],
                'campaign_id': f"campaign_{random.randint(1000, 9999)}"
            },
            'is_fraud': is_fraud,
            'fraud_score': random.uniform(0.8, 1.0) if is_fraud else random.uniform(0.0, 0.3),
            'fraud_indicators': fraud_indicators if is_fraud else {}
        }
        
        return transaction
    
    def _get_or_create_session(self, user_id: str, start_time: datetime, 
                              end_time: datetime) -> str:
        """Get existing session or create new one for user."""
        # Check if user has active session
        active_sessions = [s for s in self.sessions.values() 
                          if s['user_id'] == user_id and s['is_active']]
        
        if active_sessions and random.random() < 0.7:  # 70% chance to continue session
            return active_sessions[0]['session_id']
        
        # Create new session
        session_id = str(uuid.uuid4())
        session_start = self._generate_weighted_timestamp(start_time, end_time)
        session_duration = self.temporal_patterns.generate_session_duration(
            self.users[user_id]['user_type']
        )
        
        session = {
            'session_id': session_id,
            'user_id': user_id,
            'start_time': session_start,
            'expected_end_time': session_start + session_duration,
            'is_active': True,
            'event_count': 0,
            'page_views': 0,
            'products_viewed': [],
            'cart_items': []
        }
        
        self.sessions[session_id] = session
        return session_id
    
    def _create_user_event(self, user: Dict[str, Any], session_id: str,
                          event_type: str, timestamp: datetime) -> Dict[str, Any]:
        """Create a user behavior event."""
        event_id = str(uuid.uuid4())
        session = self.sessions[session_id]
        
        # Update session
        session['event_count'] += 1
        if event_type == 'page_view':
            session['page_views'] += 1
        
        # Generate event-specific data
        event_data = {
            'event_id': event_id,
            'user_id': user['user_id'],
            'session_id': session_id,
            'event_type': event_type,
            'timestamp': timestamp.isoformat(),
            'device_info': self.user_devices[user['user_id']],
            'location': user['location']
        }
        
        # Add event-specific properties
        if event_type in ['page_view', 'product_view']:
            if event_type == 'product_view':
                product = random.choice(list(self.products.values()))
                event_data['product_id'] = product['product_id']
                event_data['category'] = product['category']
                session['products_viewed'].append(product['product_id'])
            
            event_data['page_url'] = self._generate_page_url(event_type, event_data.get('category'))
            event_data['referrer'] = self._generate_referrer(user['marketing_channel'])
            event_data['duration_seconds'] = random.randint(10, 300)
        
        elif event_type == 'search':
            event_data['search_query'] = self._generate_search_query()
            event_data['results_count'] = random.randint(0, 100)
        
        elif event_type in ['add_to_cart', 'remove_from_cart']:
            product = random.choice(list(self.products.values()))
            event_data['product_id'] = product['product_id']
            event_data['quantity'] = random.randint(1, 3)
            
            if event_type == 'add_to_cart':
                session['cart_items'].append(product['product_id'])
            elif product['product_id'] in session['cart_items']:
                session['cart_items'].remove(product['product_id'])
        
        return event_data
    
    def _generate_page_url(self, event_type: str, category: str = None) -> str:
        """Generate realistic page URL."""
        base_urls = {
            'page_view': ['/home', '/about', '/contact', '/help', '/terms'],
            'product_view': [f'/products/{category.lower().replace(" ", "-")}' if category else '/products']
        }
        
        if event_type in base_urls:
            return random.choice(base_urls[event_type])
        return '/home'
    
    def _generate_referrer(self, marketing_channel: str) -> str:
        """Generate realistic referrer URL."""
        referrers = {
            'organic_search': 'https://www.google.com/',
            'paid_search': 'https://www.google.com/search?q=',
            'social_media': 'https://www.facebook.com/',
            'email_marketing': 'https://mail.google.com/',
            'direct_traffic': '',
            'referral': 'https://partner-site.com/',
            'display_ads': 'https://ad-network.com/'
        }
        
        return referrers.get(marketing_channel, '')
    
    def _generate_search_query(self) -> str:
        """Generate realistic search query."""
        query_templates = [
            "{category} {adjective}",
            "best {category}",
            "cheap {category}",
            "{brand} {category}",
            "{adjective} {category} reviews"
        ]
        
        category = random.choice(self.config.categories)
        adjective = random.choice(["premium", "affordable", "professional", "quality"])
        brand = random.choice(["Apple", "Samsung", "Nike", "Adidas", "Sony"])
        
        template = random.choice(query_templates)
        return template.format(category=category.lower(), adjective=adjective, brand=brand)
    
    def _get_product_field_value(self, product: Dict[str, Any], update_type: str) -> Any:
        """Get current value of product field being updated."""
        field_map = {
            'price_change': 'price',
            'stock_update': 'stock_quantity',
            'description_update': 'description',
            'status_change': 'is_active'
        }
        
        field = field_map.get(update_type, 'price')
        return product.get(field)
    
    def _generate_new_product_value(self, product: Dict[str, Any], update_type: str) -> Any:
        """Generate new value for product field update."""
        if update_type == 'price_change':
            current_price = product['price']
            # Price change within ±20%
            change_percent = random.uniform(-0.2, 0.2)
            return round(current_price * (1 + change_percent), 2)
        
        elif update_type == 'stock_update':
            return random.randint(0, 1000)
        
        elif update_type == 'description_update':
            return self._generate_product_description(product['category'], product['name'])
        
        elif update_type == 'status_change':
            return not product['is_active']
        
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get generator statistics."""
        return {
            'total_users': len(self.users),
            'active_users': len([u for u in self.users.values() if u['is_active']]),
            'total_products': len(self.products),
            'active_products': len([p for p in self.products.values() if p['is_active']]),
            'total_sessions': len(self.sessions),
            'active_sessions': len([s for s in self.sessions.values() if s['is_active']]),
            'categories': self.config.categories,
            'configuration': self.config.__dict__
        }
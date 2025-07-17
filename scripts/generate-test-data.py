#!/usr/bin/env python3
"""
E-Commerce Analytics Platform - Test Data Generator
Generates realistic test data for development and testing purposes.
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
import psycopg2
from faker import Faker
import sys

# Initialize Faker
fake = Faker()

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_analytics',
    'user': 'ecap_user',
    'password': 'ecap_password'
}

class TestDataGenerator:
    """Generates realistic test data for e-commerce analytics platform."""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✅ Connected to PostgreSQL database")
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            sys.exit(1)
    
    def disconnect(self):
        """Disconnect from database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✅ Disconnected from database")
    
    def generate_customers(self, count: int = 1000) -> List[Dict[str, Any]]:
        """Generate customer data."""
        print(f"📊 Generating {count} customers...")
        
        customers = []
        for i in range(count):
            customer = {
                'email': fake.email(),
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'phone': fake.phone_number(),
                'address': fake.address(),
                'city': fake.city(),
                'state': fake.state(),
                'postal_code': fake.zipcode(),
                'country': fake.country(),
                'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
                'registration_date': fake.date_between(start_date='-2y', end_date='today'),
                'is_active': random.choice([True, True, True, False]),  # 75% active
                'customer_segment': random.choice(['premium', 'standard', 'basic']),
                'marketing_consent': random.choice([True, False])
            }
            customers.append(customer)
        
        return customers
    
    def generate_products(self, count: int = 500) -> List[Dict[str, Any]]:
        """Generate product data."""
        print(f"📦 Generating {count} products...")
        
        categories = [
            'Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports',
            'Beauty', 'Toys', 'Automotive', 'Food', 'Health'
        ]
        
        brands = [
            'TechCorp', 'StyleBrand', 'HomePlus', 'BookWorld', 'SportsPro',
            'BeautyLux', 'ToyLand', 'AutoMax', 'FreshFood', 'WellnessPlus'
        ]
        
        products = []
        for i in range(count):
            category = random.choice(categories)
            brand = random.choice(brands)
            
            product = {
                'name': f"{brand} {fake.word().title()} {fake.word().title()}",
                'description': fake.text(max_nb_chars=200),
                'category': category,
                'brand': brand,
                'price': round(random.uniform(10.00, 1000.00), 2),
                'cost': round(random.uniform(5.00, 500.00), 2),
                'stock_quantity': random.randint(0, 1000),
                'weight': round(random.uniform(0.1, 10.0), 2),
                'dimensions': f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(10, 100)}",
                'sku': f"{category[:3].upper()}-{brand[:3].upper()}-{random.randint(1000, 9999)}",
                'is_active': random.choice([True, True, True, False]),  # 75% active
                'created_at': fake.date_between(start_date='-1y', end_date='today'),
                'rating': round(random.uniform(1.0, 5.0), 1),
                'review_count': random.randint(0, 1000)
            }
            products.append(product)
        
        return products
    
    def generate_orders(self, customer_count: int, product_count: int, count: int = 2000) -> List[Dict[str, Any]]:
        """Generate order data."""
        print(f"🛍️ Generating {count} orders...")
        
        orders = []
        for i in range(count):
            order_date = fake.date_between(start_date='-1y', end_date='today')
            
            order = {
                'customer_id': random.randint(1, customer_count),
                'order_date': order_date,
                'status': random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled']),
                'total_amount': 0.0,  # Will be calculated from order items
                'shipping_address': fake.address(),
                'billing_address': fake.address(),
                'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
                'shipping_method': random.choice(['standard', 'express', 'overnight']),
                'tracking_number': fake.uuid4(),
                'notes': fake.text(max_nb_chars=100) if random.random() < 0.1 else None
            }
            orders.append(order)
        
        return orders
    
    def generate_order_items(self, order_count: int, product_count: int) -> List[Dict[str, Any]]:
        """Generate order items data."""
        print(f"📋 Generating order items...")
        
        order_items = []
        for order_id in range(1, order_count + 1):
            # Each order has 1-5 items
            num_items = random.randint(1, 5)
            
            for _ in range(num_items):
                quantity = random.randint(1, 3)
                unit_price = round(random.uniform(10.00, 500.00), 2)
                
                order_item = {
                    'order_id': order_id,
                    'product_id': random.randint(1, product_count),
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'total_price': round(quantity * unit_price, 2),
                    'discount_amount': round(random.uniform(0, unit_price * 0.2), 2) if random.random() < 0.1 else 0.0
                }
                order_items.append(order_item)
        
        return order_items
    
    def insert_customers(self, customers: List[Dict[str, Any]]):
        """Insert customers into database."""
        print("💾 Inserting customers into database...")
        
        insert_query = """
            INSERT INTO ecommerce.customers (
                email, first_name, last_name, phone, address, city, state, 
                postal_code, country, date_of_birth, registration_date, 
                is_active, customer_segment, marketing_consent, created_at, updated_at
            ) VALUES (
                %(email)s, %(first_name)s, %(last_name)s, %(phone)s, %(address)s, 
                %(city)s, %(state)s, %(postal_code)s, %(country)s, %(date_of_birth)s, 
                %(registration_date)s, %(is_active)s, %(customer_segment)s, 
                %(marketing_consent)s, NOW(), NOW()
            )
        """
        
        try:
            self.cursor.executemany(insert_query, customers)
            self.conn.commit()
            print(f"✅ Inserted {len(customers)} customers")
        except Exception as e:
            print(f"❌ Failed to insert customers: {e}")
            self.conn.rollback()
            raise
    
    def insert_products(self, products: List[Dict[str, Any]]):
        """Insert products into database."""
        print("💾 Inserting products into database...")
        
        insert_query = """
            INSERT INTO ecommerce.products (
                name, description, category, brand, price, cost, stock_quantity,
                weight, dimensions, sku, is_active, rating, review_count, 
                created_at, updated_at
            ) VALUES (
                %(name)s, %(description)s, %(category)s, %(brand)s, %(price)s, 
                %(cost)s, %(stock_quantity)s, %(weight)s, %(dimensions)s, 
                %(sku)s, %(is_active)s, %(rating)s, %(review_count)s, 
                NOW(), NOW()
            )
        """
        
        try:
            self.cursor.executemany(insert_query, products)
            self.conn.commit()
            print(f"✅ Inserted {len(products)} products")
        except Exception as e:
            print(f"❌ Failed to insert products: {e}")
            self.conn.rollback()
            raise
    
    def insert_orders(self, orders: List[Dict[str, Any]]):
        """Insert orders into database."""
        print("💾 Inserting orders into database...")
        
        insert_query = """
            INSERT INTO ecommerce.orders (
                customer_id, order_date, status, total_amount, shipping_address,
                billing_address, payment_method, shipping_method, tracking_number,
                notes, created_at, updated_at
            ) VALUES (
                %(customer_id)s, %(order_date)s, %(status)s, %(total_amount)s,
                %(shipping_address)s, %(billing_address)s, %(payment_method)s,
                %(shipping_method)s, %(tracking_number)s, %(notes)s, NOW(), NOW()
            )
        """
        
        try:
            self.cursor.executemany(insert_query, orders)
            self.conn.commit()
            print(f"✅ Inserted {len(orders)} orders")
        except Exception as e:
            print(f"❌ Failed to insert orders: {e}")
            self.conn.rollback()
            raise
    
    def insert_order_items(self, order_items: List[Dict[str, Any]]):
        """Insert order items into database."""
        print("💾 Inserting order items into database...")
        
        insert_query = """
            INSERT INTO ecommerce.order_items (
                order_id, product_id, quantity, unit_price, total_price, discount_amount
            ) VALUES (
                %(order_id)s, %(product_id)s, %(quantity)s, %(unit_price)s, 
                %(total_price)s, %(discount_amount)s
            )
        """
        
        try:
            self.cursor.executemany(insert_query, order_items)
            self.conn.commit()
            print(f"✅ Inserted {len(order_items)} order items")
        except Exception as e:
            print(f"❌ Failed to insert order items: {e}")
            self.conn.rollback()
            raise
    
    def update_order_totals(self):
        """Update order totals based on order items."""
        print("🔄 Updating order totals...")
        
        update_query = """
            UPDATE ecommerce.orders 
            SET total_amount = (
                SELECT COALESCE(SUM(total_price - discount_amount), 0)
                FROM ecommerce.order_items 
                WHERE order_id = orders.order_id
            )
        """
        
        try:
            self.cursor.execute(update_query)
            self.conn.commit()
            print("✅ Updated order totals")
        except Exception as e:
            print(f"❌ Failed to update order totals: {e}")
            self.conn.rollback()
            raise
    
    def generate_all_data(self, customers: int = 1000, products: int = 500, orders: int = 2000):
        """Generate all test data."""
        print(f"🚀 Starting test data generation...")
        print(f"   - Customers: {customers}")
        print(f"   - Products: {products}")
        print(f"   - Orders: {orders}")
        print("")
        
        self.connect()
        
        try:
            # Generate and insert customers
            customer_data = self.generate_customers(customers)
            self.insert_customers(customer_data)
            
            # Generate and insert products
            product_data = self.generate_products(products)
            self.insert_products(product_data)
            
            # Generate and insert orders
            order_data = self.generate_orders(customers, products, orders)
            self.insert_orders(order_data)
            
            # Generate and insert order items
            order_item_data = self.generate_order_items(orders, products)
            self.insert_order_items(order_item_data)
            
            # Update order totals
            self.update_order_totals()
            
            print("")
            print("🎉 Test data generation completed successfully!")
            print(f"   - Generated {customers} customers")
            print(f"   - Generated {products} products")
            print(f"   - Generated {orders} orders")
            print(f"   - Generated {len(order_item_data)} order items")
            print("")
            print("You can now run analytics queries on the generated data.")
            
        except Exception as e:
            print(f"❌ Test data generation failed: {e}")
            sys.exit(1)
        finally:
            self.disconnect()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Generate test data for e-commerce analytics platform')
    parser.add_argument('--customers', type=int, default=1000, help='Number of customers to generate (default: 1000)')
    parser.add_argument('--products', type=int, default=500, help='Number of products to generate (default: 500)')
    parser.add_argument('--orders', type=int, default=2000, help='Number of orders to generate (default: 2000)')
    parser.add_argument('--quick', action='store_true', help='Generate smaller dataset for quick testing')
    
    args = parser.parse_args()
    
    # Adjust numbers for quick mode
    if args.quick:
        args.customers = 100
        args.products = 50
        args.orders = 200
        print("🏃 Quick mode: Generating smaller dataset for testing")
    
    # Validate inputs
    if args.customers < 1 or args.products < 1 or args.orders < 1:
        print("❌ All counts must be positive integers")
        sys.exit(1)
    
    # Create generator and run
    generator = TestDataGenerator(DB_CONFIG)
    generator.generate_all_data(args.customers, args.products, args.orders)

if __name__ == "__main__":
    main()
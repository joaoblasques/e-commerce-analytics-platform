"""Seed data generation for database tables."""

import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional
from faker import Faker
from sqlalchemy.orm import Session
from database.models import (
    Customer, Product, Order, OrderItem, 
    Category, Supplier, Base
)
from database.config import get_database_session, get_database_engine
from loguru import logger


class SeedDataGenerator:
    """Generate realistic seed data for e-commerce database."""
    
    def __init__(self, locale: str = 'en_US'):
        """Initialize seed data generator."""
        self.faker = Faker(locale)
        Faker.seed(42)  # For reproducible data
        random.seed(42)
        
        # Business data constants
        self.categories_data = [
            {"category_id": "cat_electronics", "name": "Electronics", "description": "Electronic devices and gadgets"},
            {"category_id": "cat_clothing", "name": "Clothing", "description": "Apparel and accessories"},
            {"category_id": "cat_books", "name": "Books", "description": "Books and publications"},
            {"category_id": "cat_home", "name": "Home & Garden", "description": "Home improvement and garden items"},
            {"category_id": "cat_sports", "name": "Sports & Outdoors", "description": "Sports equipment and outdoor gear"},
            {"category_id": "cat_beauty", "name": "Beauty & Health", "description": "Beauty products and health items"},
            {"category_id": "cat_automotive", "name": "Automotive", "description": "Car parts and accessories"},
            {"category_id": "cat_toys", "name": "Toys & Games", "description": "Children's toys and games"},
        ]
        
        self.brands = [
            "Apple", "Samsung", "Sony", "Nike", "Adidas", "Dell", "HP", "Canon", 
            "Nikon", "Amazon", "Microsoft", "Google", "Netflix", "Spotify", "Adobe"
        ]
        
        self.suppliers_data = [
            {"name": "Tech Innovations Inc.", "email": "contact@techinnovations.com"},
            {"name": "Fashion Forward Ltd.", "email": "info@fashionforward.com"},
            {"name": "Home Essentials Co.", "email": "sales@homeessentials.com"},
            {"name": "Sports Gear Pro", "email": "orders@sportsgear.com"},
            {"name": "Beauty Supply Chain", "email": "supply@beautysupply.com"},
        ]
        
        self.payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
        self.currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]
        
    def generate_categories(self, session: Session) -> List[Category]:
        """Generate category data."""
        categories = []
        
        for cat_data in self.categories_data:
            category = Category(
                category_id=cat_data["category_id"],
                name=cat_data["name"],
                description=cat_data["description"],
                level=1,
                sort_order=len(categories),
                status="active"
            )
            categories.append(category)
            session.add(category)
        
        logger.info(f"Generated {len(categories)} categories")
        return categories
    
    def generate_suppliers(self, session: Session) -> List[Supplier]:
        """Generate supplier data."""
        suppliers = []
        
        for i, supplier_data in enumerate(self.suppliers_data):
            supplier = Supplier(
                supplier_id=f"supplier_{i+1:03d}",
                name=supplier_data["name"],
                contact_person=self.faker.name(),
                email=supplier_data["email"],
                phone=self.faker.phone_number(),
                address={
                    "street": self.faker.street_address(),
                    "city": self.faker.city(),
                    "state": self.faker.state_abbr(),
                    "zip_code": self.faker.zipcode(),
                    "country": "US"
                },
                tax_id=self.faker.bothify("##-#######"),
                registration_number=self.faker.bothify("REG########"),
                rating=Decimal(str(round(random.uniform(3.5, 5.0), 2))),
                total_orders=random.randint(50, 500),
                status="active"
            )
            suppliers.append(supplier)
            session.add(supplier)
        
        logger.info(f"Generated {len(suppliers)} suppliers")
        return suppliers
    
    def generate_customers(self, session: Session, count: int = 1000) -> List[Customer]:
        """Generate customer data."""
        customers = []
        tiers = ["bronze", "silver", "gold", "platinum"]
        tier_weights = [0.6, 0.25, 0.12, 0.03]  # 60% bronze, 25% silver, etc.
        
        for i in range(count):
            registration_date = self.faker.date_time_between(
                start_date='-2y', 
                end_date='now'
            )
            
            customer = Customer(
                user_id=f"user_{i+1:06d}",
                email=self.faker.unique.email(),
                first_name=self.faker.first_name(),
                last_name=self.faker.last_name(),
                phone=self.faker.phone_number(),
                date_of_birth=self.faker.date_of_birth(minimum_age=18, maximum_age=80),
                gender=random.choice(["M", "F", "Other"]),
                registration_date=registration_date,
                last_login=self.faker.date_time_between(
                    start_date=registration_date, 
                    end_date='now'
                ) if random.random() > 0.1 else None,
                account_status=random.choices(
                    ["active", "inactive", "suspended"],
                    weights=[0.85, 0.12, 0.03]
                )[0],
                customer_tier=random.choices(tiers, weights=tier_weights)[0],
                lifetime_value=Decimal(str(round(random.uniform(0, 5000), 2))),
                total_orders=random.randint(0, 50),
                preferred_language=random.choice(["en", "es", "fr", "de"]),
                marketing_consent=random.choice([True, False]),
                address={
                    "street": self.faker.street_address(),
                    "city": self.faker.city(),
                    "state": self.faker.state_abbr(),
                    "zip_code": self.faker.zipcode(),
                    "country": "US"
                },
                preferences={
                    "newsletter": random.choice([True, False]),
                    "sms_notifications": random.choice([True, False]),
                    "preferred_categories": random.sample(
                        [cat["category_id"] for cat in self.categories_data], 
                        k=random.randint(1, 3)
                    )
                }
            )
            customers.append(customer)
            session.add(customer)
        
        logger.info(f"Generated {len(customers)} customers")
        return customers
    
    def generate_products(self, session: Session, suppliers: List[Supplier], count: int = 500) -> List[Product]:
        """Generate product data."""
        products = []
        
        for i in range(count):
            category = random.choice(self.categories_data)
            supplier = random.choice(suppliers)
            brand = random.choice(self.brands)
            
            price = Decimal(str(round(random.uniform(10, 2000), 2)))
            cost = price * Decimal(str(round(random.uniform(0.4, 0.8), 2)))
            
            product = Product(
                product_id=f"prod_{category['category_id']}_{i+1:04d}",
                name=self.faker.catch_phrase(),
                description=self.faker.text(max_nb_chars=500),
                sku=self.faker.bothify("SKU-####-????").upper(),
                category_id=category["category_id"],
                category_name=category["name"],
                subcategory=self.faker.word().title(),
                brand=brand,
                price=price,
                cost=cost,
                inventory_count=random.randint(0, 1000),
                weight_kg=Decimal(str(round(random.uniform(0.1, 50), 2))),
                dimensions_cm=f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(5, 50)}",
                status=random.choices(
                    ["active", "inactive", "discontinued"],
                    weights=[0.8, 0.15, 0.05]
                )[0],
                tags=[self.faker.word() for _ in range(random.randint(2, 5))],
                supplier_id=supplier.supplier_id,
                rating_avg=Decimal(str(round(random.uniform(2.0, 5.0), 2))),
                review_count=random.randint(0, 500)
            )
            products.append(product)
            session.add(product)
        
        logger.info(f"Generated {len(products)} products")
        return products
    
    def generate_orders(self, session: Session, customers: List[Customer], 
                       products: List[Product], count: int = 2000) -> List[Order]:
        """Generate order data."""
        orders = []
        
        for i in range(count):
            customer = random.choice(customers)
            order_date = self.faker.date_time_between(
                start_date=customer.registration_date,
                end_date='now'
            )
            
            order = Order(
                order_id=f"order_{order_date.strftime('%Y%m%d')}_{i+1:06d}",
                user_id=customer.user_id,
                order_date=order_date,
                order_status=random.choices(
                    ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"],
                    weights=[0.05, 0.1, 0.15, 0.25, 0.35, 0.08, 0.02]
                )[0],
                payment_method=random.choice(self.payment_methods),
                payment_status=random.choices(
                    ["pending", "paid", "failed", "refunded", "cancelled"],
                    weights=[0.05, 0.8, 0.05, 0.05, 0.05]
                )[0],
                payment_date=order_date + timedelta(hours=random.randint(1, 24)),
                shipping_address=customer.address,
                billing_address=customer.address,
                shipping_method=random.choice(["standard", "express", "overnight"]),
                tracking_number=self.faker.bothify("TRK#########") if random.random() > 0.3 else None,
                shipped_date=order_date + timedelta(days=random.randint(1, 5)) if random.random() > 0.4 else None,
                delivered_date=order_date + timedelta(days=random.randint(3, 10)) if random.random() > 0.5 else None,
                notes=self.faker.text(max_nb_chars=200) if random.random() > 0.7 else None,
                currency=random.choice(self.currencies)
            )
            orders.append(order)
            session.add(order)
        
        logger.info(f"Generated {len(orders)} orders")
        return orders
    
    def generate_order_items(self, session: Session, orders: List[Order], 
                           products: List[Product]) -> List[OrderItem]:
        """Generate order item data."""
        order_items = []
        
        for order in orders:
            # Each order has 1-5 items
            num_items = random.randint(1, 5)
            selected_products = random.sample(products, min(num_items, len(products)))
            
            order_subtotal = Decimal('0.00')
            
            for product in selected_products:
                quantity = random.randint(1, 3)
                unit_price = product.price
                discount_amount = unit_price * Decimal(str(random.uniform(0, 0.2)))
                line_total = (unit_price - discount_amount) * quantity
                
                order_item = OrderItem(
                    order_item_id=str(uuid.uuid4()),
                    order_id=order.order_id,
                    product_id=product.product_id,
                    quantity=quantity,
                    unit_price=unit_price,
                    discount_amount=discount_amount,
                    line_total=line_total,
                    product_name=product.name,
                    product_sku=product.sku,
                    product_category=product.category_name
                )
                order_items.append(order_item)
                session.add(order_item)
                order_subtotal += line_total
            
            # Update order totals
            order.subtotal = order_subtotal
            order.tax_amount = order_subtotal * Decimal('0.08')  # 8% tax
            order.shipping_cost = Decimal('9.99') if order_subtotal < 50 else Decimal('0.00')
            order.discount_amount = order_subtotal * Decimal(str(random.uniform(0, 0.1)))
            order.total_amount = (order.subtotal + order.tax_amount + 
                                order.shipping_cost - order.discount_amount)
        
        logger.info(f"Generated {len(order_items)} order items")
        return order_items
    
    def generate_all_data(self, session: Session, 
                         customers_count: int = 1000,
                         products_count: int = 500,
                         orders_count: int = 2000) -> Dict[str, Any]:
        """Generate all seed data."""
        logger.info("Starting seed data generation...")
        
        # Generate in dependency order
        categories = self.generate_categories(session)
        suppliers = self.generate_suppliers(session)
        customers = self.generate_customers(session, customers_count)
        products = self.generate_products(session, suppliers, products_count)
        orders = self.generate_orders(session, customers, products, orders_count)
        order_items = self.generate_order_items(session, orders, products)
        
        # Commit all changes
        session.commit()
        
        logger.info("Seed data generation completed successfully!")
        
        return {
            "categories": len(categories),
            "suppliers": len(suppliers),
            "customers": len(customers),
            "products": len(products),
            "orders": len(orders),
            "order_items": len(order_items)
        }


def create_seed_data(
    customers_count: int = 1000,
    products_count: int = 500,
    orders_count: int = 2000,
    recreate_tables: bool = False
) -> Dict[str, Any]:
    """Create seed data for the database."""
    
    if recreate_tables:
        logger.info("Recreating database tables...")
        engine = get_database_engine()
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        logger.info("Database tables recreated")
    
    generator = SeedDataGenerator()
    
    with get_database_session() as session:
        return generator.generate_all_data(
            session=session,
            customers_count=customers_count,
            products_count=products_count,
            orders_count=orders_count
        )


if __name__ == "__main__":
    # Run seed data generation
    results = create_seed_data(
        customers_count=1000,
        products_count=500,
        orders_count=2000,
        recreate_tables=False
    )
    
    logger.info(f"Seed data generation results: {results}")
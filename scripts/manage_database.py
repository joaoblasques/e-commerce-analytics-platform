#!/usr/bin/env python3
"""Database management script for e-commerce analytics platform."""

import os
import sys
from pathlib import Path

import click

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from loguru import logger

from database.config import get_database_engine, get_database_session
from database.models import Base
from database.seed_data import create_seed_data


@click.group()
def cli():
    """Database management commands."""
    pass


@cli.command()
def create_tables():
    """Create all database tables."""
    try:
        logger.info("Creating database tables...")
        engine = get_database_engine()
        Base.metadata.create_all(engine)
        logger.info("✅ Database tables created successfully")
    except Exception as e:
        logger.error(f"❌ Failed to create tables: {e}")
        sys.exit(1)


@cli.command()
def drop_tables():
    """Drop all database tables."""
    try:
        logger.info("Dropping database tables...")
        engine = get_database_engine()
        Base.metadata.drop_all(engine)
        logger.info("✅ Database tables dropped successfully")
    except Exception as e:
        logger.error(f"❌ Failed to drop tables: {e}")
        sys.exit(1)


@cli.command()
@click.option("--customers", default=1000, help="Number of customers to generate")
@click.option("--products", default=500, help="Number of products to generate")
@click.option("--orders", default=2000, help="Number of orders to generate")
@click.option("--recreate", is_flag=True, help="Recreate tables before seeding")
def seed_data(customers, products, orders, recreate):
    """Generate seed data for the database."""
    try:
        logger.info(
            f"Generating seed data: {customers} customers, {products} products, {orders} orders"
        )

        results = create_seed_data(
            customers_count=customers,
            products_count=products,
            orders_count=orders,
            recreate_tables=recreate,
        )

        logger.info("✅ Seed data generated successfully")
        logger.info(f"Generated records: {results}")

    except Exception as e:
        logger.error(f"❌ Failed to generate seed data: {e}")
        sys.exit(1)


@cli.command()
def reset_database():
    """Reset database (drop, create, seed)."""
    try:
        logger.info("Resetting database...")

        # Drop tables
        logger.info("Dropping tables...")
        engine = get_database_engine()
        Base.metadata.drop_all(engine)

        # Create tables
        logger.info("Creating tables...")
        Base.metadata.create_all(engine)

        # Seed data
        logger.info("Seeding data...")
        results = create_seed_data(
            customers_count=1000,
            products_count=500,
            orders_count=2000,
            recreate_tables=False,
        )

        logger.info("✅ Database reset completed successfully")
        logger.info(f"Generated records: {results}")

    except Exception as e:
        logger.error(f"❌ Failed to reset database: {e}")
        sys.exit(1)


@cli.command()
def test_connection():
    """Test database connection."""
    try:
        logger.info("Testing database connection...")
        engine = get_database_engine()

        with engine.connect() as conn:
            result = conn.execute("SELECT 1 as test")
            test_value = result.scalar()

        if test_value == 1:
            logger.info("✅ Database connection successful")
        else:
            logger.error("❌ Database connection test failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        sys.exit(1)


@cli.command()
def show_stats():
    """Show database statistics."""
    try:
        logger.info("Fetching database statistics...")

        with get_database_session() as session:
            from database.models import (
                Category,
                Customer,
                Order,
                OrderItem,
                Product,
                Supplier,
            )

            stats = {
                "customers": session.query(Customer).count(),
                "products": session.query(Product).count(),
                "orders": session.query(Order).count(),
                "order_items": session.query(OrderItem).count(),
                "categories": session.query(Category).count(),
                "suppliers": session.query(Supplier).count(),
            }

            logger.info("📊 Database Statistics:")
            for table, count in stats.items():
                logger.info(f"  {table.title()}: {count:,}")

    except Exception as e:
        logger.error(f"❌ Failed to fetch statistics: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()

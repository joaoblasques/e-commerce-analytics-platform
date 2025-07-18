"""Unit tests for database functionality."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from database.config import DatabaseManager, DatabaseSettings  # noqa: E402
from database.models import (  # noqa: E402
    Base,
    Category,
    Customer,
    Order,
    Product,
    Supplier,
)


class TestDatabaseSettings:
    """Test database settings configuration."""

    def test_database_settings_defaults(self):
        """Test default database settings."""
        settings = DatabaseSettings()
        assert settings.db_host == "localhost"
        assert settings.db_port == 5432
        assert settings.db_name == "ecommerce_analytics"
        assert settings.db_user == "analytics_user"
        assert settings.pool_size == 20

    def test_database_url_generation(self):
        """Test database URL generation."""
        settings = DatabaseSettings()
        expected_url = "postgresql://analytics_user:dev_password@localhost:5432/ecommerce_analytics"
        assert settings.database_url == expected_url

    def test_async_database_url_generation(self):
        """Test async database URL generation."""
        settings = DatabaseSettings()
        expected_url = "postgresql+asyncpg://analytics_user:dev_password@localhost:5432/ecommerce_analytics"
        assert settings.async_database_url == expected_url


class TestDatabaseManager:
    """Test database manager functionality."""

    def test_database_manager_initialization(self):
        """Test database manager initialization."""
        manager = DatabaseManager()
        assert manager.settings is not None
        assert manager._engine is None
        assert manager._session_factory is None

    @patch("database.config.create_engine")
    def test_create_engine(self, mock_create_engine):
        """Test engine creation."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_engine.connect.return_value.__exit__.return_value = None
        mock_conn.execute.return_value = None
        mock_create_engine.return_value = mock_engine

        manager = DatabaseManager()
        engine = manager.create_engine()

        assert engine == mock_engine
        mock_create_engine.assert_called_once()

    def test_session_factory_creation(self):
        """Test session factory creation."""
        with patch.object(DatabaseManager, "create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            manager = DatabaseManager()
            session_factory = manager.create_session_factory()

            assert session_factory is not None
            mock_create_engine.assert_called_once()


class TestDatabaseModels:
    """Test database model definitions."""

    def test_customer_model(self):
        """Test Customer model structure."""
        customer = Customer(
            user_id="test_user_001",
            email="test@example.com",
            first_name="John",
            last_name="Doe",
            account_status="active",
            customer_tier="bronze",
        )

        assert customer.user_id == "test_user_001"
        assert customer.email == "test@example.com"
        assert customer.account_status == "active"
        assert customer.customer_tier == "bronze"

    def test_product_model(self):
        """Test Product model structure."""
        product = Product(
            product_id="prod_001",
            name="Test Product",
            category_id="cat_electronics",
            category_name="Electronics",
            brand="TestBrand",
            price=99.99,
            cost=50.00,
            status="active",
        )

        assert product.product_id == "prod_001"
        assert product.name == "Test Product"
        assert product.category_id == "cat_electronics"
        assert product.brand == "TestBrand"
        assert product.price == 99.99

    def test_order_model(self):
        """Test Order model structure."""
        order = Order(
            order_id="order_001",
            user_id="user_001",
            order_status="pending",
            payment_status="pending",
            subtotal=100.00,
            tax_amount=8.00,
            shipping_cost=9.99,
            total_amount=117.99,
            currency="USD",
        )

        assert order.order_id == "order_001"
        assert order.user_id == "user_001"
        assert order.order_status == "pending"
        assert order.total_amount == 117.99
        assert order.currency == "USD"

    def test_category_model(self):
        """Test Category model structure."""
        category = Category(
            category_id="cat_001",
            name="Electronics",
            description="Electronic devices",
            level=1,
            sort_order=0,
            status="active",
        )

        assert category.category_id == "cat_001"
        assert category.name == "Electronics"
        assert category.level == 1
        assert category.status == "active"

    def test_supplier_model(self):
        """Test Supplier model structure."""
        supplier = Supplier(
            supplier_id="sup_001",
            name="Test Supplier",
            email="supplier@example.com",
            status="active",
        )

        assert supplier.supplier_id == "sup_001"
        assert supplier.name == "Test Supplier"
        assert supplier.email == "supplier@example.com"
        assert supplier.status == "active"


class TestDatabaseSchema:
    """Test database schema validation."""

    def test_base_metadata_exists(self):
        """Test that Base metadata is properly configured."""
        assert Base.metadata is not None
        assert len(Base.metadata.tables) > 0

    def test_required_tables_defined(self):
        """Test that all required tables are defined."""
        table_names = list(Base.metadata.tables.keys())
        required_tables = [
            "customers",
            "products",
            "orders",
            "order_items",
            "categories",
            "suppliers",
        ]

        for table in required_tables:
            assert table in table_names, f"Table '{table}' not found in schema"

    def test_table_relationships(self):
        """Test that table relationships are properly defined."""
        # Get table objects
        customers_table = Base.metadata.tables["customers"]
        orders_table = Base.metadata.tables["orders"]
        products_table = Base.metadata.tables["products"]
        order_items_table = Base.metadata.tables["order_items"]

        # Check foreign key relationships
        orders_fks = [fk.column.name for fk in orders_table.foreign_keys]
        assert "user_id" in orders_fks

        order_items_fks = [fk.column.name for fk in order_items_table.foreign_keys]
        assert "order_id" in order_items_fks
        assert "product_id" in order_items_fks


@pytest.mark.integration
class TestDatabaseIntegration:
    """Integration tests for database functionality."""

    @pytest.fixture
    def in_memory_db(self):
        """Create an in-memory SQLite database for testing."""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        SessionLocal = sessionmaker(bind=engine)
        return SessionLocal()

    def test_create_customer_record(self, in_memory_db):
        """Test creating a customer record."""
        customer = Customer(
            user_id="test_user_001",
            email="test@example.com",
            first_name="John",
            last_name="Doe",
            account_status="active",
            customer_tier="bronze",
        )

        in_memory_db.add(customer)
        in_memory_db.commit()

        # Verify record was created
        saved_customer = (
            in_memory_db.query(Customer).filter_by(user_id="test_user_001").first()
        )
        assert saved_customer is not None
        assert saved_customer.email == "test@example.com"

    def test_create_product_record(self, in_memory_db):
        """Test creating a product record."""
        product = Product(
            product_id="prod_001",
            name="Test Product",
            category_id="cat_electronics",
            category_name="Electronics",
            brand="TestBrand",
            price=99.99,
            cost=50.00,
            status="active",
        )

        in_memory_db.add(product)
        in_memory_db.commit()

        # Verify record was created
        saved_product = (
            in_memory_db.query(Product).filter_by(product_id="prod_001").first()
        )
        assert saved_product is not None
        assert saved_product.name == "Test Product"

    def test_customer_order_relationship(self, in_memory_db):
        """Test customer-order relationship."""
        # Create customer
        customer = Customer(
            user_id="test_user_001",
            email="test@example.com",
            first_name="John",
            last_name="Doe",
            account_status="active",
            customer_tier="bronze",
        )
        in_memory_db.add(customer)

        # Create order
        order = Order(
            order_id="order_001",
            user_id="test_user_001",
            order_status="pending",
            payment_status="pending",
            subtotal=100.00,
            tax_amount=8.00,
            shipping_cost=9.99,
            total_amount=117.99,
            currency="USD",
        )
        in_memory_db.add(order)
        in_memory_db.commit()

        # Verify relationship
        saved_customer = (
            in_memory_db.query(Customer).filter_by(user_id="test_user_001").first()
        )
        assert len(saved_customer.orders) == 1
        assert saved_customer.orders[0].order_id == "order_001"

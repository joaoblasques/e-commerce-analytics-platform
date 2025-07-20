"""
Unit tests for database models.
"""
from datetime import datetime
from decimal import Decimal

from src.database.models import Base, Category, Customer, Order, Product, Supplier


def test_customer_model_creation():
    """Test Customer model can be instantiated."""
    customer = Customer(
        user_id="test_user_123",
        email="test@example.com",
        first_name="John",
        last_name="Doe",
    )

    assert customer.user_id == "test_user_123"
    assert customer.email == "test@example.com"
    assert customer.first_name == "John"
    assert customer.last_name == "Doe"


def test_product_model_creation():
    """Test Product model can be instantiated."""
    product = Product(
        product_id="prod_123",
        name="Test Product",
        category_id="cat_123",
        brand="Test Brand",
        price=Decimal("99.99"),
        cost=Decimal("50.00"),
        inventory_count=100,
    )

    assert product.product_id == "prod_123"
    assert product.name == "Test Product"
    assert product.category_id == "cat_123"
    assert product.brand == "Test Brand"
    assert product.price == Decimal("99.99")
    assert product.cost == Decimal("50.00")
    assert product.inventory_count == 100


def test_order_model_creation():
    """Test Order model can be instantiated."""
    order = Order(
        order_id="order_123",
        user_id="user_123",
        total_amount=Decimal("199.98"),
        payment_method="credit_card",
        order_status="completed",
        order_date=datetime.now(),
    )

    assert order.order_id == "order_123"
    assert order.user_id == "user_123"
    assert order.total_amount == Decimal("199.98")
    assert order.payment_method == "credit_card"
    assert order.order_status == "completed"


def test_category_model_creation():
    """Test Category model can be instantiated."""
    category = Category(
        category_id="cat_123",
        name="Electronics",
        description="Electronic devices and accessories",
    )

    assert category.category_id == "cat_123"
    assert category.name == "Electronics"
    assert category.description == "Electronic devices and accessories"


def test_supplier_model_creation():
    """Test Supplier model can be instantiated."""
    supplier = Supplier(
        supplier_id="sup_123",
        name="Test Supplier",
        email="supplier@example.com",
        phone="+1234567890",
    )

    assert supplier.supplier_id == "sup_123"
    assert supplier.name == "Test Supplier"
    assert supplier.email == "supplier@example.com"
    assert supplier.phone == "+1234567890"


def test_models_have_tablename():
    """Test that all models have proper table names."""
    assert Customer.__tablename__ == "customers"
    assert Product.__tablename__ == "products"
    assert Order.__tablename__ == "orders"
    assert Category.__tablename__ == "categories"
    assert Supplier.__tablename__ == "suppliers"


def test_model_attributes_exist():
    """Test that all models have expected attributes."""
    # Test Customer attributes
    customer_attrs = [
        "user_id",
        "email",
        "first_name",
        "last_name",
        "phone",
        "date_of_birth",
        "gender",
        "registration_date",
        "last_login",
        "account_status",
        "customer_tier",
        "lifetime_value",
        "total_orders",
    ]

    for attr in customer_attrs:
        assert hasattr(Customer, attr), f"Customer missing attribute: {attr}"

    # Test Product attributes
    product_attrs = [
        "product_id",
        "name",
        "description",
        "category_id",
        "brand",
        "price",
        "cost",
        "inventory_count",
        "weight_kg",
        "sku",
        "status",
    ]

    for attr in product_attrs:
        assert hasattr(Product, attr), f"Product missing attribute: {attr}"


def test_base_model_exists():
    """Test that Base declarative model exists."""
    assert Base is not None
    assert hasattr(Base, "metadata")

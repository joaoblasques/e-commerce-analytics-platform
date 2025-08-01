"""Initial database schema

Revision ID: 70e4ba23d616
Revises:
Create Date: 2025-07-18 08:16:56.070568

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "70e4ba23d616"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    # Create categories table
    op.create_table(
        "categories",
        sa.Column("category_id", sa.String(length=50), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("parent_category_id", sa.String(length=50), nullable=True),
        sa.Column("level", sa.Integer(), nullable=False, default=1),
        sa.Column("sort_order", sa.Integer(), nullable=False, default=0),
        sa.Column("status", sa.String(length=20), nullable=False, default="active"),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["parent_category_id"],
            ["categories.category_id"],
        ),
        sa.PrimaryKeyConstraint("category_id"),
        sa.CheckConstraint(
            "status IN ('active', 'inactive')", name="check_category_status"
        ),
        sa.CheckConstraint("level > 0", name="check_positive_level"),
    )

    # Create indexes for categories
    op.create_index("idx_categories_parent", "categories", ["parent_category_id"])
    op.create_index("idx_categories_status", "categories", ["status"])
    op.create_index("idx_categories_level", "categories", ["level"])
    op.create_index("idx_categories_sort_order", "categories", ["sort_order"])

    # Create suppliers table
    op.create_table(
        "suppliers",
        sa.Column("supplier_id", sa.String(length=50), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("contact_person", sa.String(length=255), nullable=True),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("phone", sa.String(length=20), nullable=True),
        sa.Column("address", sa.JSON(), nullable=True),
        sa.Column("tax_id", sa.String(length=50), nullable=True),
        sa.Column("registration_number", sa.String(length=100), nullable=True),
        sa.Column("rating", sa.DECIMAL(precision=3, scale=2), nullable=True),
        sa.Column("total_orders", sa.Integer(), nullable=False, default=0),
        sa.Column("status", sa.String(length=20), nullable=False, default="active"),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.PrimaryKeyConstraint("supplier_id"),
        sa.CheckConstraint(
            "status IN ('active', 'inactive', 'suspended')",
            name="check_supplier_status",
        ),
    )

    # Create indexes for suppliers
    op.create_index("idx_suppliers_status", "suppliers", ["status"])
    op.create_index("idx_suppliers_name", "suppliers", ["name"])
    op.create_index("idx_suppliers_email", "suppliers", ["email"])

    # Create customers table
    op.create_table(
        "customers",
        sa.Column("user_id", sa.String(length=50), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("first_name", sa.String(length=100), nullable=True),
        sa.Column("last_name", sa.String(length=100), nullable=True),
        sa.Column("phone", sa.String(length=20), nullable=True),
        sa.Column("date_of_birth", sa.DateTime(), nullable=True),
        sa.Column("gender", sa.String(length=10), nullable=True),
        sa.Column(
            "registration_date",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_login", sa.DateTime(), nullable=True),
        sa.Column(
            "account_status", sa.String(length=20), nullable=False, default="active"
        ),
        sa.Column(
            "customer_tier", sa.String(length=20), nullable=False, default="bronze"
        ),
        sa.Column(
            "lifetime_value",
            sa.DECIMAL(precision=12, scale=2),
            nullable=False,
            default=0.00,
        ),
        sa.Column("total_orders", sa.Integer(), nullable=False, default=0),
        sa.Column(
            "preferred_language", sa.String(length=10), nullable=False, default="en"
        ),
        sa.Column("marketing_consent", sa.Boolean(), nullable=False, default=False),
        sa.Column("address", sa.JSON(), nullable=True),
        sa.Column("preferences", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("user_id"),
        sa.UniqueConstraint("email"),
        sa.CheckConstraint(
            "account_status IN ('active', 'inactive', 'suspended')",
            name="check_account_status",
        ),
        sa.CheckConstraint(
            "customer_tier IN ('bronze', 'silver', 'gold', 'platinum')",
            name="check_customer_tier",
        ),
    )

    # Create indexes for customers
    op.create_index("idx_customers_email", "customers", ["email"])
    op.create_index("idx_customers_status", "customers", ["account_status"])
    op.create_index("idx_customers_tier", "customers", ["customer_tier"])
    op.create_index("idx_customers_registration", "customers", ["registration_date"])

    # Create products table
    op.create_table(
        "products",
        sa.Column("product_id", sa.String(length=50), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("sku", sa.String(length=100), nullable=True),
        sa.Column("category_id", sa.String(length=50), nullable=False),
        sa.Column("category_name", sa.String(length=100), nullable=False),
        sa.Column("subcategory", sa.String(length=100), nullable=True),
        sa.Column("brand", sa.String(length=100), nullable=False),
        sa.Column("price", sa.DECIMAL(precision=10, scale=2), nullable=False),
        sa.Column("cost", sa.DECIMAL(precision=10, scale=2), nullable=False),
        sa.Column("inventory_count", sa.Integer(), nullable=False, default=0),
        sa.Column("weight_kg", sa.DECIMAL(precision=8, scale=2), nullable=True),
        sa.Column("dimensions_cm", sa.String(length=50), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, default="active"),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("tags", sa.ARRAY(sa.String()), nullable=True),
        sa.Column("supplier_id", sa.String(length=50), nullable=True),
        sa.Column("rating_avg", sa.DECIMAL(precision=3, scale=2), nullable=True),
        sa.Column("review_count", sa.Integer(), nullable=False, default=0),
        sa.ForeignKeyConstraint(
            ["supplier_id"],
            ["suppliers.supplier_id"],
        ),
        sa.PrimaryKeyConstraint("product_id"),
        sa.UniqueConstraint("sku"),
        sa.CheckConstraint(
            "status IN ('active', 'inactive', 'discontinued')",
            name="check_product_status",
        ),
        sa.CheckConstraint("price >= 0", name="check_positive_price"),
        sa.CheckConstraint("cost >= 0", name="check_positive_cost"),
        sa.CheckConstraint("inventory_count >= 0", name="check_positive_inventory"),
    )

    # Create indexes for products
    op.create_index("idx_products_category", "products", ["category_id"])
    op.create_index("idx_products_brand", "products", ["brand"])
    op.create_index("idx_products_status", "products", ["status"])
    op.create_index("idx_products_price", "products", ["price"])
    op.create_index("idx_products_created", "products", ["created_at"])
    op.create_index("idx_products_sku", "products", ["sku"])

    # Create orders table
    op.create_table(
        "orders",
        sa.Column("order_id", sa.String(length=50), nullable=False),
        sa.Column("user_id", sa.String(length=50), nullable=False),
        sa.Column(
            "order_date", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "order_status", sa.String(length=20), nullable=False, default="pending"
        ),
        sa.Column(
            "subtotal", sa.DECIMAL(precision=12, scale=2), nullable=False, default=0.00
        ),
        sa.Column(
            "tax_amount",
            sa.DECIMAL(precision=12, scale=2),
            nullable=False,
            default=0.00,
        ),
        sa.Column(
            "shipping_cost",
            sa.DECIMAL(precision=12, scale=2),
            nullable=False,
            default=0.00,
        ),
        sa.Column(
            "discount_amount",
            sa.DECIMAL(precision=12, scale=2),
            nullable=False,
            default=0.00,
        ),
        sa.Column(
            "total_amount",
            sa.DECIMAL(precision=12, scale=2),
            nullable=False,
            default=0.00,
        ),
        sa.Column("payment_method", sa.String(length=50), nullable=True),
        sa.Column(
            "payment_status", sa.String(length=20), nullable=False, default="pending"
        ),
        sa.Column("payment_date", sa.DateTime(), nullable=True),
        sa.Column("shipping_address", sa.JSON(), nullable=True),
        sa.Column("billing_address", sa.JSON(), nullable=True),
        sa.Column("shipping_method", sa.String(length=50), nullable=True),
        sa.Column("tracking_number", sa.String(length=100), nullable=True),
        sa.Column("shipped_date", sa.DateTime(), nullable=True),
        sa.Column("delivered_date", sa.DateTime(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("currency", sa.String(length=3), nullable=False, default="USD"),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["customers.user_id"],
        ),
        sa.PrimaryKeyConstraint("order_id"),
        sa.CheckConstraint(
            "order_status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded')",
            name="check_order_status",
        ),
        sa.CheckConstraint(
            "payment_status IN ('pending', 'paid', 'failed', 'refunded', 'cancelled')",
            name="check_payment_status",
        ),
        sa.CheckConstraint("subtotal >= 0", name="check_positive_subtotal"),
        sa.CheckConstraint("tax_amount >= 0", name="check_positive_tax"),
        sa.CheckConstraint("shipping_cost >= 0", name="check_positive_shipping"),
        sa.CheckConstraint("discount_amount >= 0", name="check_positive_discount"),
        sa.CheckConstraint("total_amount >= 0", name="check_positive_total"),
    )

    # Create indexes for orders
    op.create_index("idx_orders_user_id", "orders", ["user_id"])
    op.create_index("idx_orders_status", "orders", ["order_status"])
    op.create_index("idx_orders_date", "orders", ["order_date"])
    op.create_index("idx_orders_payment_status", "orders", ["payment_status"])
    op.create_index("idx_orders_total", "orders", ["total_amount"])

    # Create order_items table
    op.create_table(
        "order_items",
        sa.Column("order_item_id", sa.String(length=50), nullable=False),
        sa.Column("order_id", sa.String(length=50), nullable=False),
        sa.Column("product_id", sa.String(length=50), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("unit_price", sa.DECIMAL(precision=10, scale=2), nullable=False),
        sa.Column(
            "discount_amount",
            sa.DECIMAL(precision=10, scale=2),
            nullable=False,
            default=0.00,
        ),
        sa.Column("line_total", sa.DECIMAL(precision=12, scale=2), nullable=False),
        sa.Column("product_name", sa.String(length=255), nullable=False),
        sa.Column("product_sku", sa.String(length=100), nullable=True),
        sa.Column("product_category", sa.String(length=100), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.order_id"],
        ),
        sa.ForeignKeyConstraint(
            ["product_id"],
            ["products.product_id"],
        ),
        sa.PrimaryKeyConstraint("order_item_id"),
        sa.CheckConstraint("quantity > 0", name="check_positive_quantity"),
        sa.CheckConstraint("unit_price >= 0", name="check_positive_unit_price"),
        sa.CheckConstraint("discount_amount >= 0", name="check_positive_discount"),
        sa.CheckConstraint("line_total >= 0", name="check_positive_line_total"),
    )

    # Create indexes for order_items
    op.create_index("idx_order_items_order_id", "order_items", ["order_id"])
    op.create_index("idx_order_items_product_id", "order_items", ["product_id"])
    op.create_index("idx_order_items_created", "order_items", ["created_at"])


def downgrade() -> None:
    """Downgrade schema."""
    # Drop tables in reverse order
    op.drop_table("order_items")
    op.drop_table("orders")
    op.drop_table("products")
    op.drop_table("customers")
    op.drop_table("suppliers")
    op.drop_table("categories")

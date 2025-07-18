"""Database utility functions."""

from typing import Dict, Any, Optional, List
from sqlalchemy import text, inspect
from sqlalchemy.orm import Session
from database.config import get_database_session, get_database_engine
from database.models import Base
from loguru import logger


def get_table_info(table_name: str) -> Dict[str, Any]:
    """Get information about a database table."""
    engine = get_database_engine()
    inspector = inspect(engine)
    
    if table_name not in inspector.get_table_names():
        raise ValueError(f"Table '{table_name}' does not exist")
    
    columns = inspector.get_columns(table_name)
    indexes = inspector.get_indexes(table_name)
    foreign_keys = inspector.get_foreign_keys(table_name)
    
    return {
        'table_name': table_name,
        'columns': columns,
        'indexes': indexes,
        'foreign_keys': foreign_keys
    }


def get_table_count(table_name: str) -> int:
    """Get the number of rows in a table."""
    with get_database_session() as session:
        result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return result.scalar()


def get_database_size() -> Dict[str, Any]:
    """Get database size information."""
    with get_database_session() as session:
        # Get database size
        db_size_query = text("""
            SELECT pg_size_pretty(pg_database_size(current_database())) as database_size
        """)
        db_size = session.execute(db_size_query).scalar()
        
        # Get table sizes
        table_sizes_query = text("""
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """)
        
        table_sizes = []
        for row in session.execute(table_sizes_query):
            table_sizes.append({
                'table_name': row.tablename,
                'size': row.size,
                'size_bytes': row.size_bytes
            })
        
        return {
            'database_size': db_size,
            'table_sizes': table_sizes
        }


def verify_database_schema() -> Dict[str, Any]:
    """Verify that the database schema matches the models."""
    engine = get_database_engine()
    inspector = inspect(engine)
    
    # Get expected tables from models
    expected_tables = set(Base.metadata.tables.keys())
    
    # Get actual tables from database
    actual_tables = set(inspector.get_table_names())
    
    # Compare
    missing_tables = expected_tables - actual_tables
    extra_tables = actual_tables - expected_tables
    
    result = {
        'schema_valid': len(missing_tables) == 0 and len(extra_tables) == 0,
        'expected_tables': list(expected_tables),
        'actual_tables': list(actual_tables),
        'missing_tables': list(missing_tables),
        'extra_tables': list(extra_tables)
    }
    
    if missing_tables:
        logger.warning(f"Missing tables: {missing_tables}")
    if extra_tables:
        logger.warning(f"Extra tables: {extra_tables}")
    
    return result


def get_database_stats() -> Dict[str, Any]:
    """Get comprehensive database statistics."""
    with get_database_session() as session:
        from database.models import Customer, Product, Order, OrderItem, Category, Supplier
        
        stats = {
            'record_counts': {
                'customers': session.query(Customer).count(),
                'products': session.query(Product).count(),
                'orders': session.query(Order).count(),
                'order_items': session.query(OrderItem).count(),
                'categories': session.query(Category).count(),
                'suppliers': session.query(Supplier).count(),
            },
            'customer_stats': {
                'active_customers': session.query(Customer).filter(Customer.account_status == 'active').count(),
                'customers_with_orders': session.query(Customer).filter(Customer.total_orders > 0).count(),
            },
            'product_stats': {
                'active_products': session.query(Product).filter(Product.status == 'active').count(),
                'out_of_stock': session.query(Product).filter(Product.inventory_count == 0).count(),
            },
            'order_stats': {
                'completed_orders': session.query(Order).filter(Order.order_status == 'delivered').count(),
                'pending_orders': session.query(Order).filter(Order.order_status.in_(['pending', 'confirmed', 'processing'])).count(),
            }
        }
        
        # Add schema verification
        stats['schema_info'] = verify_database_schema()
        
        return stats


def execute_query(query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Execute a raw SQL query and return results."""
    with get_database_session() as session:
        result = session.execute(text(query), parameters or {})
        
        # Convert to list of dictionaries
        columns = result.keys()
        rows = []
        for row in result:
            rows.append(dict(zip(columns, row)))
        
        return rows


def health_check() -> Dict[str, Any]:
    """Perform a comprehensive database health check."""
    health_status = {
        'database_connection': False,
        'schema_valid': False,
        'has_data': False,
        'errors': []
    }
    
    try:
        # Test connection
        engine = get_database_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health_status['database_connection'] = True
        
        # Verify schema
        schema_info = verify_database_schema()
        health_status['schema_valid'] = schema_info['schema_valid']
        
        # Check for data
        stats = get_database_stats()
        total_records = sum(stats['record_counts'].values())
        health_status['has_data'] = total_records > 0
        
        # Add additional info
        health_status['record_counts'] = stats['record_counts']
        health_status['total_records'] = total_records
        
    except Exception as e:
        health_status['errors'].append(str(e))
        logger.error(f"Database health check failed: {e}")
    
    return health_status
# Database Setup Guide

This guide explains how to set up and manage the PostgreSQL database for the E-Commerce Analytics Platform.

## Database Architecture

The platform uses PostgreSQL as the primary operational database with the following components:

- **SQLAlchemy ORM**: Object-relational mapping for Python
- **Alembic**: Database migration management
- **Connection Pooling**: Efficient database connection management
- **Seed Data Generator**: Realistic test data generation

## Database Schema

The database contains the following main tables:

### Core Tables

1. **customers**: Customer master data
   - Primary key: `user_id`
   - Contains profile information, preferences, and business metrics
   - Supports JSON fields for flexible address and preference storage

2. **products**: Product catalog
   - Primary key: `product_id`
   - Includes pricing, inventory, and categorization
   - Supports product tags using PostgreSQL arrays

3. **orders**: Order headers
   - Primary key: `order_id`
   - Contains order totals, payment information, and fulfillment data
   - Links to customers via foreign key

4. **order_items**: Order line items
   - Primary key: `order_item_id`
   - Links orders to products with quantity and pricing details
   - Stores product snapshot at time of order

5. **categories**: Product categories
   - Primary key: `category_id`
   - Supports hierarchical categorization
   - Self-referential foreign key for parent categories

6. **suppliers**: Supplier master data
   - Primary key: `supplier_id`
   - Contains supplier contact and performance information

### Key Features

- **Referential Integrity**: Proper foreign key constraints
- **Data Validation**: Check constraints for business rules
- **Indexes**: Optimized for common query patterns
- **Audit Fields**: Created/updated timestamps
- **Flexible JSON**: JSON fields for semi-structured data

## Setup Instructions

### Prerequisites

1. **PostgreSQL Database**: Ensure PostgreSQL is running
   ```bash
   # Using Docker (recommended for development)
   docker-compose up -d postgres
   ```

2. **Python Dependencies**: Install required packages
   ```bash
   pip install sqlalchemy alembic psycopg2-binary pydantic-settings loguru faker click
   ```

### Database Configuration

1. **Environment Variables**: Configure database connection
   ```bash
   export DB_HOST=localhost
   export DB_PORT=5432
   export DB_NAME=ecommerce_analytics
   export DB_USER=analytics_user
   export DB_PASSWORD=dev_password
   ```

2. **Configuration File**: Settings are managed via `src/database/config.py`
   - Uses Pydantic settings for configuration management
   - Supports environment variable override
   - Includes connection pooling configuration

### Database Initialization

1. **Create Tables**: Initialize database schema
   ```bash
   python scripts/manage_database.py create-tables
   ```

2. **Run Migrations**: Apply database migrations
   ```bash
   python -m alembic upgrade head
   ```

3. **Generate Seed Data**: Create test data
   ```bash
   python scripts/manage_database.py seed-data --customers 1000 --products 500 --orders 2000
   ```

### Database Management Commands

The platform includes a comprehensive database management script:

```bash
# Test database connection
python scripts/manage_database.py test-connection

# Create database tables
python scripts/manage_database.py create-tables

# Generate seed data
python scripts/manage_database.py seed-data

# Show database statistics
python scripts/manage_database.py show-stats

# Reset database (drop, create, seed)
python scripts/manage_database.py reset-database

# Drop all tables
python scripts/manage_database.py drop-tables
```

## Migration Management

### Creating New Migrations

1. **Auto-generate Migration**: Create migration from model changes
   ```bash
   python -m alembic revision --autogenerate -m "Description of changes"
   ```

2. **Manual Migration**: Create empty migration file
   ```bash
   python -m alembic revision -m "Description of changes"
   ```

### Applying Migrations

1. **Upgrade to Latest**: Apply all pending migrations
   ```bash
   python -m alembic upgrade head
   ```

2. **Upgrade to Specific Version**: Apply migrations up to specific version
   ```bash
   python -m alembic upgrade <revision_id>
   ```

3. **Downgrade**: Roll back migrations
   ```bash
   python -m alembic downgrade <revision_id>
   ```

## Seed Data Generation

The platform includes a sophisticated seed data generator that creates realistic e-commerce data:

### Features

- **Realistic Data**: Uses Faker library for authentic-looking data
- **Relationships**: Maintains proper foreign key relationships
- **Business Logic**: Implements realistic business patterns
- **Configurable Volume**: Adjustable data generation counts
- **Reproducible**: Seeded random number generation

### Usage

```python
from database.seed_data import create_seed_data

# Generate seed data programmatically
results = create_seed_data(
    customers_count=1000,
    products_count=500,
    orders_count=2000,
    recreate_tables=False
)
```

### Generated Data Includes

- **Customers**: 1000 customers with realistic profiles
- **Products**: 500 products across multiple categories
- **Orders**: 2000 orders with realistic order patterns
- **Categories**: 8 product categories with descriptions
- **Suppliers**: 5 suppliers with contact information

## Connection Management

### Connection Pooling

The platform uses SQLAlchemy's connection pooling for efficient database access:

```python
# Configuration settings
pool_size = 20          # Base connection pool size
max_overflow = 30       # Additional connections under high load
pool_timeout = 30       # Connection timeout in seconds
pool_recycle = 3600     # Connection recycling in seconds
```

### Session Management

Database sessions are managed through context managers:

```python
from database.config import get_database_session

# Using session directly
with get_database_session() as session:
    customers = session.query(Customer).all()

# Using context manager
from database.config import create_database_session

with create_database_session() as session:
    customer = session.query(Customer).filter_by(user_id="user_001").first()
```

## Performance Considerations

### Indexing Strategy

The database includes optimized indexes for common query patterns:

- **Customer indexes**: email, status, tier, registration_date
- **Product indexes**: category, brand, status, price, creation_date
- **Order indexes**: user_id, status, date, payment_status, total_amount
- **Order item indexes**: order_id, product_id, creation_date

### Query Optimization

- Use appropriate indexes for WHERE clauses
- Leverage connection pooling for concurrent access
- Implement pagination for large result sets
- Use bulk operations for data loading

## Monitoring and Maintenance

### Database Statistics

Monitor database health and performance:

```bash
# View record counts and statistics
python scripts/manage_database.py show-stats

# Check database size and table sizes
python -c "
import sys
sys.path.insert(0, 'src')
from database.utils import get_database_size
print(get_database_size())
"
```

### Health Checks

The platform includes comprehensive health check functionality:

```python
from database.utils import health_check

# Perform complete health check
health_status = health_check()
print(health_status)
```

### Backup and Recovery

1. **Database Backup**: Regular PostgreSQL backups
   ```bash
   pg_dump -h localhost -U analytics_user ecommerce_analytics > backup.sql
   ```

2. **Database Restore**: Restore from backup
   ```bash
   psql -h localhost -U analytics_user ecommerce_analytics < backup.sql
   ```

## Troubleshooting

### Common Issues

1. **Connection Refused**: Ensure PostgreSQL is running
   ```bash
   # Check PostgreSQL status
   brew services list | grep postgresql
   # or
   sudo systemctl status postgresql
   ```

2. **Permission Denied**: Verify database user permissions
   ```sql
   GRANT ALL PRIVILEGES ON DATABASE ecommerce_analytics TO analytics_user;
   ```

3. **Migration Conflicts**: Resolve migration conflicts
   ```bash
   # View migration history
   python -m alembic history
   
   # Reset to specific migration
   python -m alembic downgrade <revision_id>
   ```

### Debug Mode

Enable SQL logging for debugging:

```python
# In database configuration
echo_sql = True  # Shows all SQL statements
```

## Testing

The database setup includes comprehensive unit tests:

```bash
# Run database tests
python -m pytest tests/unit/test_database.py -v

# Run specific test class
python -m pytest tests/unit/test_database.py::TestDatabaseModels -v
```

## Security Considerations

1. **Environment Variables**: Never commit database credentials
2. **Connection Security**: Use SSL for production connections
3. **Access Control**: Implement proper database user permissions
4. **Data Validation**: Use model validation and constraints
5. **SQL Injection**: Always use parameterized queries

## Next Steps

After completing database setup:

1. **Configure Kafka Topics**: Set up message streaming infrastructure
2. **Implement Data Producers**: Create data ingestion pipeline
3. **Set up Spark Jobs**: Configure data processing workflows
4. **Create API Endpoints**: Build REST API for data access
5. **Deploy Monitoring**: Set up database monitoring and alerts

---

This database setup provides a solid foundation for the E-Commerce Analytics Platform with proper schema design, migration management, and realistic test data generation.
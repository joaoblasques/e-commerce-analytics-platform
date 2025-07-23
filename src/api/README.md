# E-Commerce Analytics Platform API

A FastAPI-based REST API for accessing e-commerce analytics, fraud detection, and business intelligence data.

## Features

- **RESTful API**: Clean, well-documented REST endpoints
- **API Versioning**: Structured versioning with v1 endpoints
- **Dependency Injection**: Proper dependency management for database and Redis connections
- **Error Handling**: Comprehensive error handling with structured responses
- **Configuration Management**: Environment-based configuration with validation
- **Health Checks**: Kubernetes-ready health and readiness probes
- **Documentation**: Auto-generated OpenAPI/Swagger documentation
- **Security**: Built-in security headers and CORS support
- **Rate Limiting**: Redis-based rate limiting for API protection
- **Pagination**: Consistent pagination across all list endpoints

## Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL database
- Redis server
- All dependencies installed via Poetry

### Environment Variables

Configure the API using environment variables with the `ECAP_` prefix:

```bash
# Application settings
ECAP_ENVIRONMENT=development  # development, staging, production
ECAP_DEBUG=false
ECAP_HOST=0.0.0.0
ECAP_PORT=8000
ECAP_LOG_LEVEL=INFO

# Security settings
ECAP_SECRET_KEY=your-secret-key-change-in-production
ECAP_ALLOWED_ORIGINS=["http://localhost:3000", "http://localhost:8080"]

# Database settings
ECAP_DATABASE_URL=postgresql://ecap_user:ecap_password@localhost:5432/ecommerce_analytics
ECAP_DATABASE_POOL_SIZE=20
ECAP_DATABASE_MAX_OVERFLOW=30

# Redis settings
ECAP_REDIS_URL=redis://localhost:6379/0
ECAP_REDIS_EXPIRE_TIME=3600

# Analytics settings
ECAP_DEFAULT_PAGE_SIZE=100
ECAP_MAX_PAGE_SIZE=1000

# Rate limiting
ECAP_RATE_LIMIT_REQUESTS=100
ECAP_RATE_LIMIT_WINDOW=60
```

### Running the API

#### Method 1: Using the run script

```bash
cd src/api
python run.py
```

#### Method 2: Using uvicorn directly

```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

#### Method 3: Using the main module

```bash
python -m src.api.main
```

### API Documentation

Once the server is running, access the documentation at:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

*Note: Documentation endpoints are disabled in production for security.*

## API Structure

### Base URLs

- **Root**: `http://localhost:8000/`
- **Health**: `http://localhost:8000/health`
- **API v1**: `http://localhost:8000/api/v1/`

### Endpoint Categories

#### Health & Monitoring
- `GET /health` - Basic health check
- `GET /api/v1/health/` - Basic health check
- `GET /api/v1/health/detailed` - Detailed health with dependency checks
- `GET /api/v1/health/readiness` - Kubernetes readiness probe
- `GET /api/v1/health/liveness` - Kubernetes liveness probe

#### Customer Analytics
- `GET /api/v1/customers/` - List customers with pagination
- `GET /api/v1/customers/{customer_id}` - Get customer details
- `GET /api/v1/customers/{customer_id}/analytics` - Get customer analytics

#### Business Intelligence
- `GET /api/v1/analytics/revenue` - Revenue analytics with time series
- `GET /api/v1/analytics/products` - Product performance analytics
- `GET /api/v1/analytics/geographic` - Geographic distribution analytics
- `GET /api/v1/analytics/marketing` - Marketing attribution analytics

#### Fraud Detection
- `GET /api/v1/fraud/alerts` - List fraud alerts with filtering
- `GET /api/v1/fraud/alerts/{alert_id}` - Get fraud alert details
- `GET /api/v1/fraud/dashboard` - Fraud detection dashboard metrics
- `GET /api/v1/fraud/rules` - Get fraud detection rules

### Response Format

All API responses follow a consistent format:

#### Success Response
```json
{
  "data": { ... },
  "metadata": {
    "timestamp": "2025-01-23T10:30:00Z",
    "version": "1.0.0"
  }
}
```

#### Error Response
```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "details": { ... }
  }
}
```

#### Paginated Response
```json
{
  "items": [ ... ],
  "pagination": {
    "page": 1,
    "size": 100,
    "total": 1000,
    "pages": 10
  }
}
```

### Query Parameters

#### Common Parameters
- `page` (int): Page number (1-based, default: 1)
- `size` (int): Page size (default: 100, max: 1000)

#### Date Filtering
- `start_date` (string): Start date in YYYY-MM-DD format
- `end_date` (string): End date in YYYY-MM-DD format

#### Analytics Parameters
- `granularity` (string): Time granularity (daily, weekly, monthly)
- `include_predictions` (bool): Include ML predictions in response
- `sort_by` (string): Sort field for ordering results

## Configuration

### Settings Class

The API uses Pydantic Settings for configuration management with automatic validation:

```python
from src.api.config import get_settings

settings = get_settings()
print(f"Running in {settings.environment} mode")
```

### Database Configuration

Database connections are managed through SQLAlchemy with connection pooling:

```python
from src.api.dependencies import get_database_session

# In endpoint function
def get_data(db: Session = Depends(get_database_session)):
    # Use db session
    pass
```

### Redis Configuration

Redis is used for caching and rate limiting:

```python
from src.api.dependencies import get_redis_dependency

# In endpoint function
async def get_cached_data(redis = Depends(get_redis_dependency)):
    # Use redis client
    pass
```

## Error Handling

The API provides comprehensive error handling with custom exception classes:

### Exception Types

- `ECAPException` - Base exception class
- `ValidationError` - Request validation errors (422)
- `NotFoundError` - Resource not found errors (404)
- `AuthenticationError` - Authentication failures (401)
- `AuthorizationError` - Authorization failures (403)
- `RateLimitError` - Rate limit exceeded (429)
- `DatabaseError` - Database connection/query errors (500)
- `CacheError` - Redis/cache errors (500)

### Custom Error Usage

```python
from src.api.exceptions import NotFoundError, ValidationError

def get_customer(customer_id: str):
    if not customer_exists(customer_id):
        raise NotFoundError("Customer", customer_id)

    if not is_valid_id(customer_id):
        raise ValidationError("Invalid customer ID format")
```

## Security

### Rate Limiting

API includes Redis-based rate limiting:
- Default: 100 requests per minute per IP
- Configurable via `ECAP_RATE_LIMIT_REQUESTS` and `ECAP_RATE_LIMIT_WINDOW`
- Returns 429 status with retry-after header when exceeded

### CORS Configuration

CORS is configured for cross-origin requests:
- Development: Permissive settings for local development
- Production: Restrictive settings for security

### Security Headers

The API includes security-focused headers:
- Server header disabled
- Date header disabled
- Proper error message sanitization

## Development

### Project Structure

```
src/api/
├── __init__.py              # API module init
├── main.py                  # FastAPI application
├── config.py                # Configuration management
├── dependencies.py          # Dependency injection
├── exceptions.py            # Custom exceptions
├── run.py                   # Application runner
├── README.md               # This file
└── v1/                     # API version 1
    ├── __init__.py         # v1 router
    └── endpoints/          # API endpoints
        ├── __init__.py
        ├── health.py       # Health check endpoints
        ├── customers.py    # Customer endpoints
        ├── analytics.py    # Analytics endpoints
        └── fraud.py        # Fraud detection endpoints
```

### Adding New Endpoints

1. Create endpoint module in `src/api/v1/endpoints/`
2. Define router and endpoint functions
3. Add router to `src/api/v1/__init__.py`
4. Add tests in `tests/api/`

Example:

```python
# src/api/v1/endpoints/new_feature.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ...dependencies import get_database_session

router = APIRouter()

@router.get("/")
async def get_feature_data(db: Session = Depends(get_database_session)):
    return {"message": "New feature endpoint"}
```

```python
# src/api/v1/__init__.py
from .endpoints import new_feature

api_router.include_router(
    new_feature.router,
    prefix="/new-feature",
    tags=["new-feature"]
)
```

### Testing

Run API tests using pytest:

```bash
# Run all API tests
pytest tests/api/

# Run specific test file
pytest tests/api/test_main.py

# Run with coverage
pytest tests/api/ --cov=src.api
```

### Environment-Specific Behavior

#### Development
- Debug mode available
- Auto-reload enabled
- Documentation endpoints enabled
- Permissive CORS settings
- Mock authentication (returns development user)

#### Production
- Debug mode disabled
- Documentation endpoints disabled
- Restrictive CORS settings
- Proper authentication required
- Enhanced security headers

## Health Checks

The API provides multiple health check endpoints for monitoring:

### Basic Health Check
```bash
curl http://localhost:8000/health
```

### Detailed Health Check
```bash
curl http://localhost:8000/api/v1/health/detailed
```

### Kubernetes Probes
```yaml
# In Kubernetes deployment
livenessProbe:
  httpGet:
    path: /api/v1/health/liveness
    port: 8000
readinessProbe:
  httpGet:
    path: /api/v1/health/readiness
    port: 8000
```

## Troubleshooting

### Common Issues

#### Database Connection Failures
```bash
# Check database connectivity
curl http://localhost:8000/api/v1/health/detailed

# Verify database configuration
python -c "from src.api.config import get_database_config; print(get_database_config())"
```

#### Redis Connection Issues
```bash
# Test Redis connectivity
redis-cli ping

# Check Redis configuration
python -c "from src.api.config import get_redis_config; print(get_redis_config())"
```

#### Configuration Validation Errors
```bash
# Validate current configuration
python -c "from src.api.config import validate_configuration; validate_configuration()"
```

### Logging

API logs are structured and include:
- Request/response information
- Error details with stack traces
- Performance metrics
- Security events

Log level can be controlled via `ECAP_LOG_LEVEL` environment variable.

## Integration

### With Analytics Engines

The API integrates with analytics engines for data processing:

```python
# Example integration (Task 4.1.3 will implement this)
from src.analytics.business_intelligence import RevenueAnalytics

revenue_engine = RevenueAnalytics(spark_session)
revenue_data = revenue_engine.analyze_revenue_trends(...)
```

### With Authentication System

Authentication will be implemented in Task 4.1.2:

```python
# Future implementation
from src.api.auth import get_current_user

@router.get("/protected")
async def protected_endpoint(user = Depends(get_current_user)):
    return {"user": user}
```

## Production Deployment

### Docker Deployment

```dockerfile
FROM python:3.10-slim
COPY . /app
WORKDIR /app
RUN pip install poetry && poetry install
CMD ["python", "src/api/run.py"]
```

### Environment Variables for Production

```bash
ECAP_ENVIRONMENT=production
ECAP_DEBUG=false
ECAP_SECRET_KEY=<secure-random-key>
ECAP_DATABASE_URL=<production-database-url>
ECAP_REDIS_URL=<production-redis-url>
ECAP_LOG_LEVEL=INFO
```

### Monitoring

Integrate with monitoring systems:
- Health check endpoints for load balancers
- Structured logging for log aggregation
- Metrics endpoints for Prometheus (future enhancement)

---

This API provides the foundation for accessing all ECAP analytics capabilities through a clean, well-documented REST interface. It will be expanded in subsequent tasks to include authentication, real analytics integration, and performance optimizations.

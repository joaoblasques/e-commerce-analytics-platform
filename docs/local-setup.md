# Local Development Setup Guide

This guide will help you set up the E-Commerce Analytics Platform development environment in less than 5 minutes.

## Prerequisites

Before starting, ensure you have the following installed:

### Required Software
- **Docker Desktop** (version 4.0 or higher)
- **Docker Compose** (version 2.0 or higher)
- **Git** (for version control)
- **Python 3.9+** (for development and scripts)

### Optional but Recommended
- **Make** (for using Makefile commands)
- **jq** (for JSON processing in scripts)
- **curl** (for API testing)

## Quick Start (< 5 minutes)

### 1. Clone the Repository
```bash
git clone https://github.com/joaoblasques/e-commerce-analytics-platform.git
cd e-commerce-analytics-platform
```

### 2. Start the Development Environment
```bash
# Start all services with initialization
./scripts/start-dev-env.sh

# Or use the shortcut (if Make is available)
make dev-start
```

### 3. Verify Everything is Working
```bash
# Check all services are healthy
./scripts/check-health.py

# Or check specific services
./scripts/test-services.py
./scripts/test-monitoring.py
```

### 4. Generate Test Data (Optional)
```bash
# Generate sample data for development
./scripts/generate-test-data.py --quick
```

That's it! Your development environment is now ready. 🎉

## Service URLs

Once the environment is running, you can access the following services:

| Service | URL | Purpose |
|---------|-----|---------|
| **Spark Master UI** | http://localhost:8080 | Monitor Spark cluster |
| **Spark Worker 1** | http://localhost:8081 | Worker node status |
| **Spark Worker 2** | http://localhost:8082 | Worker node status |
| **Spark History Server** | http://localhost:18080 | Historical job information |
| **MinIO Console** | http://localhost:9001 | Object storage management |
| **Prometheus** | http://localhost:9090 | Metrics collection |
| **Grafana** | http://localhost:3000 | Monitoring dashboards |
| **Alertmanager** | http://localhost:9093 | Alert management |

### Default Credentials
- **Grafana**: admin/admin
- **MinIO**: minioadmin/minioadmin123
- **PostgreSQL**: ecap_user/ecap_password
- **Redis**: redis_password

## Development Scripts

The platform includes several utility scripts to streamline development:

### 🚀 Environment Management
```bash
# Start development environment
./scripts/start-dev-env.sh

# Start with clean state
./scripts/start-dev-env.sh --clean

# Start without service initialization
./scripts/start-dev-env.sh --skip-init

# Stop environment (preserve data)
./scripts/stop-dev-env.sh

# Stop and remove all data
./scripts/stop-dev-env.sh --volumes

# Stop and clean up Docker resources
./scripts/stop-dev-env.sh --prune
```

### 🔍 Health Checks
```bash
# Check core services (PostgreSQL, Redis, Kafka, Spark, MinIO)
./scripts/test-services.py

# Check monitoring services (Prometheus, Grafana, Alertmanager)
./scripts/test-monitoring.py

# Comprehensive health check (all services + system resources)
./scripts/check-health.py

# Save health report to file
./scripts/check-health.py --save-report health_report.json
```

### 🗄️ Data Management
```bash
# Reset all data (with confirmation)
./scripts/reset-data.sh

# Reset specific services
./scripts/reset-data.sh --postgres --redis
./scripts/reset-data.sh --kafka --minio

# Reset all data without confirmation
./scripts/reset-data.sh --all --confirm

# Generate test data
./scripts/generate-test-data.py

# Generate smaller dataset for quick testing
./scripts/generate-test-data.py --quick

# Generate specific amounts of data
./scripts/generate-test-data.py --customers 500 --products 100 --orders 1000
```

## Database Schema

The platform uses PostgreSQL with the following schema structure:

### Core Tables
- **ecommerce.customers** - Customer information
- **ecommerce.products** - Product catalog
- **ecommerce.orders** - Order headers
- **ecommerce.order_items** - Order line items

### Analytics Tables
- **analytics.customer_segments** - Customer segmentation results
- **analytics.fraud_alerts** - Fraud detection alerts
- **analytics.product_analytics** - Product performance metrics
- **analytics.revenue_analytics** - Revenue analysis results

### System Tables
- **system.service_health** - Service health monitoring

## Kafka Topics

The following Kafka topics are automatically created:

| Topic | Partitions | Purpose |
|-------|------------|---------|
| **transactions** | 6 | E-commerce transaction events |
| **user-events** | 6 | User behavior tracking events |
| **product-updates** | 3 | Product catalog changes |
| **fraud-alerts** | 3 | Fraud detection alerts |
| **analytics-results** | 3 | Analytics processing results |

## MinIO Buckets

The following S3-compatible buckets are created:

- **raw-data** - Raw data ingestion
- **processed-data** - Processed/transformed data
- **analytics-results** - Analytics outputs
- **model-artifacts** - ML model storage
- **logs** - Application logs
- **backups** - Data backups

## Monitoring and Observability

### Grafana Dashboards
Pre-configured dashboards are available:
- **System Overview** - General system health
- **Kafka Monitoring** - Kafka cluster metrics
- **Spark Monitoring** - Spark job performance

### Prometheus Metrics
Metrics are collected from:
- System resources (CPU, memory, disk)
- Database performance
- Cache usage
- Message queue throughput
- Spark job execution

### Alerting
Alerts are configured for:
- Service downtime
- High resource usage
- Database connection issues
- Kafka partition problems

## Troubleshooting

### Common Issues

#### Services Won't Start
```bash
# Check Docker is running
docker info

# Check for port conflicts
netstat -tulpn | grep :9092

# Clean up and restart
./scripts/stop-dev-env.sh --volumes
./scripts/start-dev-env.sh --clean
```

#### Services Are Slow
```bash
# Check system resources
./scripts/check-health.py

# Restart specific service
docker-compose restart postgres

# Check logs
docker-compose logs -f postgres
```

#### Database Connection Issues
```bash
# Check PostgreSQL is running
./scripts/test-services.py

# Reset database
./scripts/reset-data.sh --postgres

# Check database logs
docker-compose logs -f postgres
```

#### Kafka Topics Missing
```bash
# Check Kafka status
./scripts/test-services.py

# Recreate topics
./scripts/reset-data.sh --kafka
```

### Log Locations
- **Container logs**: `docker-compose logs -f [service-name]`
- **Application logs**: `logs/` directory
- **Spark logs**: Available in Spark History Server UI

### Resource Requirements
- **Minimum**: 8GB RAM, 4 CPU cores, 20GB disk space
- **Recommended**: 16GB RAM, 8 CPU cores, 50GB disk space

## Development Workflow

### 1. Start Development Session
```bash
# Start environment
./scripts/start-dev-env.sh

# Check everything is healthy
./scripts/check-health.py

# Generate test data if needed
./scripts/generate-test-data.py --quick
```

### 2. During Development
```bash
# Check service health periodically
./scripts/test-services.py

# Reset data when needed
./scripts/reset-data.sh --postgres

# View logs
docker-compose logs -f [service-name]
```

### 3. End Development Session
```bash
# Stop services (preserve data)
./scripts/stop-dev-env.sh

# Or stop and clean everything
./scripts/stop-dev-env.sh --volumes --prune
```

## Performance Optimization

### Docker Resource Allocation
- Allocate at least 6GB RAM to Docker Desktop
- Enable at least 4 CPU cores
- Use SSD storage for better performance

### Service Scaling
```bash
# Scale Spark workers
docker-compose up -d --scale spark-worker=3

# Scale Kafka partitions (in topic creation)
# Edit start-dev-env.sh to adjust partition counts
```

### Data Volume Management
```bash
# Check volume usage
docker volume ls
docker system df

# Clean up unused volumes
docker volume prune
```

## Next Steps

After setting up your local environment:

1. **Explore the Services** - Visit the web UIs to understand each component
2. **Generate Data** - Use the test data generator to create sample datasets
3. **Run Analytics** - Start developing your analytics pipelines
4. **Monitor Performance** - Use Grafana dashboards to monitor system health
5. **Develop Features** - Begin implementing new analytics features

## Getting Help

- **Documentation**: Check the `docs/` directory for detailed guides
- **Health Checks**: Use `./scripts/check-health.py` for diagnostics
- **Logs**: Check `docker-compose logs -f [service]` for service logs
- **Issues**: Report problems in the GitHub repository

## Makefile Commands

If you have Make installed, you can use these shortcuts:

```bash
make dev-start         # Start development environment
make dev-stop          # Stop development environment
make dev-clean         # Clean and restart environment
make test-services     # Run service health checks
make test-monitoring   # Run monitoring health checks
make generate-data     # Generate test data
make reset-data        # Reset all data
make health-check      # Comprehensive health check
```

---

**Happy coding! 🚀**

Your e-commerce analytics platform is now ready for development. The environment provides a realistic, scalable foundation for building advanced analytics capabilities.
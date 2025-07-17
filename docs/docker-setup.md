# Docker Development Environment Setup

This document describes the Docker-based development environment for the E-Commerce Analytics Platform.

## Architecture Overview

The development environment consists of the following services:

### Core Services
- **Zookeeper**: Coordination service for Kafka
- **Kafka**: Message streaming platform
- **PostgreSQL**: Primary database
- **Redis**: Caching layer
- **MinIO**: S3-compatible object storage

### Spark Cluster
- **Spark Master**: Cluster coordinator
- **Spark Worker 1**: Processing node
- **Spark Worker 2**: Processing node
- **Spark History Server**: Job history and monitoring

## Quick Start

### Prerequisites
- Docker (>= 20.10.0)
- Docker Compose (>= 2.0.0)
- At least 8GB RAM available for Docker
- At least 20GB free disk space

### Starting the Environment

1. **Start all services:**
   ```bash
   ./scripts/start-dev-env.sh
   ```

2. **Start with clean slate:**
   ```bash
   ./scripts/start-dev-env.sh --clean
   ```

3. **Start without initialization:**
   ```bash
   ./scripts/start-dev-env.sh --skip-init
   ```

### Stopping the Environment

1. **Stop services (preserve data):**
   ```bash
   ./scripts/stop-dev-env.sh
   ```

2. **Stop services and remove all data:**
   ```bash
   ./scripts/stop-dev-env.sh --volumes
   ```

3. **Stop and cleanup Docker resources:**
   ```bash
   ./scripts/stop-dev-env.sh --prune
   ```

## Service Details

### Kafka (Port 9092)
- **Bootstrap servers**: `localhost:9092`
- **Web UI**: Not included (use CLI tools)
- **Default topics**: transactions, user-events, product-updates, fraud-alerts, analytics-results

**Common operations:**
```bash
# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Create topic
docker-compose exec kafka kafka-topics --create --topic my-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Produce messages
docker-compose exec kafka kafka-console-producer --topic my-topic --bootstrap-server localhost:9092

# Consume messages
docker-compose exec kafka kafka-console-consumer --topic my-topic --bootstrap-server localhost:9092 --from-beginning
```

### PostgreSQL (Port 5432)
- **Database**: `ecommerce_analytics`
- **User**: `ecap_user`
- **Password**: `ecap_password`
- **Connection**: `postgresql://ecap_user:ecap_password@localhost:5432/ecommerce_analytics`

**Common operations:**
```bash
# Connect to database
docker-compose exec postgres psql -U ecap_user -d ecommerce_analytics

# Run SQL file
docker-compose exec postgres psql -U ecap_user -d ecommerce_analytics -f /path/to/file.sql

# Create backup
docker-compose exec postgres pg_dump -U ecap_user ecommerce_analytics > backup.sql
```

### Redis (Port 6379)
- **Host**: `localhost:6379`
- **Password**: `redis_password`
- **Database**: `0`

**Common operations:**
```bash
# Connect to Redis
docker-compose exec redis redis-cli -a redis_password

# Monitor commands
docker-compose exec redis redis-cli -a redis_password monitor

# Get all keys
docker-compose exec redis redis-cli -a redis_password keys "*"
```

### MinIO (Ports 9000, 9001)
- **API Endpoint**: `http://localhost:9000`
- **Web Console**: `http://localhost:9001`
- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin123`

**Default buckets:**
- `raw-data`: Raw data ingestion
- `processed-data`: Processed datasets
- `analytics-results`: Analysis outputs
- `model-artifacts`: ML model storage
- `logs`: Application logs
- `backups`: System backups

**Common operations:**
```bash
# List buckets
docker-compose exec minio mc ls ecap-minio

# Upload file
docker-compose exec minio mc cp /path/to/file ecap-minio/raw-data/

# Download file
docker-compose exec minio mc cp ecap-minio/raw-data/file /path/to/destination
```

### Spark Cluster

#### Spark Master (Port 8080)
- **Web UI**: `http://localhost:8080`
- **Master URL**: `spark://localhost:7077`

#### Spark Workers
- **Worker 1 UI**: `http://localhost:8081`
- **Worker 2 UI**: `http://localhost:8082`
- **Resources**: 2GB RAM, 2 cores each

#### Spark History Server (Port 18080)
- **Web UI**: `http://localhost:18080`
- **Log Directory**: Shared volume `/opt/bitnami/spark/logs`

**Common operations:**
```bash
# Submit Spark job
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 /path/to/job.py

# Access Spark shell
docker-compose exec spark-master spark-shell --master spark://spark-master:7077

# Check Spark logs
docker-compose logs spark-master
docker-compose logs spark-worker-1
```

## Configuration

### Environment Variables
Copy `.env.example` to `.env` and modify as needed:
```bash
cp .env.example .env
```

### Service Configuration
- **PostgreSQL**: `config/postgres/init/`
- **MinIO**: `config/minio/`
- **Logs**: `logs/` (created automatically)

## Troubleshooting

### Common Issues

1. **Port conflicts**
   ```bash
   # Check what's using a port
   lsof -i :9092
   
   # Stop conflicting services
   sudo systemctl stop kafka  # or similar
   ```

2. **Out of memory**
   ```bash
   # Increase Docker memory limit
   # Docker Desktop -> Settings -> Resources -> Memory
   ```

3. **Services not starting**
   ```bash
   # Check service logs
   docker-compose logs [service-name]
   
   # Restart specific service
   docker-compose restart [service-name]
   ```

4. **Database connection issues**
   ```bash
   # Check PostgreSQL logs
   docker-compose logs postgres
   
   # Test connection
   docker-compose exec postgres pg_isready -U ecap_user
   ```

5. **Kafka connection issues**
   ```bash
   # Check Kafka logs
   docker-compose logs kafka
   
   # Test Kafka connectivity
   docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

### Useful Commands

```bash
# View all service status
docker-compose ps

# View logs for all services
docker-compose logs -f

# View logs for specific service
docker-compose logs -f kafka

# Execute command in container
docker-compose exec [service-name] [command]

# Scale workers
docker-compose up -d --scale spark-worker=4

# Update single service
docker-compose up -d --no-deps spark-master
```

### Performance Tuning

1. **Resource allocation**
   - Increase Docker memory to 8GB+
   - Increase Docker CPU to 4+ cores
   - Use SSD storage for Docker volumes

2. **Kafka optimization**
   - Increase `KAFKA_HEAP_OPTS` for large message volumes
   - Adjust partition counts based on throughput needs

3. **Spark optimization**
   - Increase worker memory for large datasets
   - Adjust executor cores based on available CPU

4. **Database optimization**
   - Increase PostgreSQL shared_buffers
   - Configure work_mem for complex queries

## Security Notes

⚠️ **Important**: This setup is for development only!

- Default passwords are used
- Services are exposed on all interfaces
- No SSL/TLS encryption
- No authentication on some services

For production deployment, ensure:
- Strong passwords
- Proper network segmentation
- SSL/TLS encryption
- Authentication and authorization
- Regular security updates

## Monitoring

### Health Checks
All services include health checks that can be monitored:
```bash
# Check health status
docker-compose ps

# View health check logs
docker inspect [container-name] | grep -A 10 Health
```

### Resource Usage
```bash
# View resource usage
docker stats

# View specific container usage
docker stats [container-name]
```

### Log Aggregation
Logs are available through Docker:
```bash
# All logs
docker-compose logs

# Specific service logs
docker-compose logs -f [service-name]

# Filter logs
docker-compose logs | grep ERROR
```

## Backup and Recovery

### Database Backup
```bash
# Create backup
docker-compose exec postgres pg_dump -U ecap_user ecommerce_analytics > backup_$(date +%Y%m%d_%H%M%S).sql

# Restore backup
docker-compose exec postgres psql -U ecap_user -d ecommerce_analytics -f backup.sql
```

### Volume Backup
```bash
# Create volume backup
docker run --rm -v ecap_postgres-data:/source -v $(pwd):/backup alpine tar czf /backup/postgres_backup.tar.gz -C /source .

# Restore volume backup
docker run --rm -v ecap_postgres-data:/target -v $(pwd):/backup alpine tar xzf /backup/postgres_backup.tar.gz -C /target
```

## Development Workflow

1. **Start environment**: `./scripts/start-dev-env.sh`
2. **Develop and test** your applications
3. **View logs**: `docker-compose logs -f [service]`
4. **Make changes** and restart services as needed
5. **Stop environment**: `./scripts/stop-dev-env.sh`

For more information, see the main [README.md](../README.md) file.
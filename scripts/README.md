# Development Scripts

This directory contains utility scripts for managing the E-Commerce Analytics Platform development environment.

## Available Scripts

### 🚀 Environment Management

#### `start-dev-env.sh`
**Purpose**: Start the complete development environment with all services  
**Usage**: `./start-dev-env.sh [OPTIONS]`

**Options**:
- `-h, --help`: Show help message
- `-c, --clean`: Clean up existing containers before starting
- `--skip-init`: Skip service initialization (MinIO buckets, Kafka topics)

**Features**:
- Starts all Docker services with health checks
- Initializes MinIO buckets automatically
- Creates Kafka topics with optimal partitioning
- Waits for services to be healthy before completing
- Displays service URLs and connection information

**Example**:
```bash
# Start with clean state
./start-dev-env.sh --clean

# Start without initialization
./start-dev-env.sh --skip-init
```

#### `stop-dev-env.sh`
**Purpose**: Stop the development environment  
**Usage**: `./stop-dev-env.sh [OPTIONS]`

**Options**:
- `-h, --help`: Show help message
- `-v, --volumes`: Also remove Docker volumes (data will be lost)
- `-f, --force`: Force removal of containers
- `--prune`: Clean up unused Docker resources after shutdown

**Features**:
- Graceful shutdown of all services
- Optional data volume cleanup
- Docker resource cleanup
- Confirmation prompts for destructive operations

**Example**:
```bash
# Stop and remove all data
./stop-dev-env.sh --volumes

# Stop and clean up Docker resources
./stop-dev-env.sh --prune
```

### 🔍 Health Checks

#### `test-services.py`
**Purpose**: Test core services connectivity and health  
**Usage**: `./test-services.py`

**Tests**:
- PostgreSQL database connectivity
- Redis cache operations
- MinIO object storage
- Spark cluster components
- Kafka broker connectivity

**Output**: Detailed status report with service-specific information

#### `test-monitoring.py`
**Purpose**: Test monitoring and observability services  
**Usage**: `./test-monitoring.py`

**Tests**:
- Prometheus metrics collection
- Grafana dashboard access
- Alertmanager configuration
- All exporters (Node, Postgres, Redis, Kafka JMX)

**Output**: Comprehensive monitoring stack health report

#### `check-health.py`
**Purpose**: Comprehensive health check for all services and system resources  
**Usage**: `./check-health.py [OPTIONS]`

**Options**:
- `--no-monitoring`: Skip monitoring services check
- `--no-docker`: Skip Docker status check
- `--save-report FILE`: Save report to specified file
- `--json-only`: Output only JSON results

**Features**:
- Tests all core and monitoring services
- Checks system resources (CPU, memory, disk)
- Network connectivity validation
- Docker environment verification
- Generates detailed reports

**Example**:
```bash
# Full health check with report
./check-health.py --save-report health_report.json

# Skip monitoring checks
./check-health.py --no-monitoring
```

### 🗄️ Data Management

#### `reset-data.sh`
**Purpose**: Reset data in specific services or all services  
**Usage**: `./reset-data.sh [OPTIONS]`

**Options**:
- `--postgres`: Reset PostgreSQL database only
- `--redis`: Reset Redis cache only
- `--kafka`: Reset Kafka topics only
- `--minio`: Reset MinIO objects only
- `--spark`: Reset Spark logs only
- `--logs`: Reset application logs only
- `--all`: Reset all data (default)
- `--confirm`: Skip confirmation prompt

**Features**:
- Selective data reset by service
- Preserves database schemas while clearing data
- Recreates Kafka topics with proper configuration
- Maintains MinIO bucket structure
- Safety confirmations for destructive operations

**Example**:
```bash
# Reset only database and cache
./reset-data.sh --postgres --redis

# Reset everything without confirmation
./reset-data.sh --all --confirm
```

#### `generate-test-data.py`
**Purpose**: Generate realistic test data for development  
**Usage**: `./generate-test-data.py [OPTIONS]`

**Options**:
- `--customers N`: Number of customers to generate (default: 1000)
- `--products N`: Number of products to generate (default: 500)
- `--orders N`: Number of orders to generate (default: 2000)
- `--quick`: Generate smaller dataset for quick testing

**Features**:
- Generates realistic customer data with demographics
- Creates diverse product catalog with categories
- Generates orders with realistic patterns
- Maintains referential integrity
- Supports customizable data volumes

**Example**:
```bash
# Generate small dataset for testing
./generate-test-data.py --quick

# Generate large dataset
./generate-test-data.py --customers 5000 --products 2000 --orders 10000
```

## Script Dependencies

All scripts require:
- **Docker** and **Docker Compose** (for container management)
- **Python 3.9+** (for Python scripts)
- **Bash** (for shell scripts)

Python script dependencies:
- `requests` - HTTP requests
- `psycopg2-binary` - PostgreSQL connectivity
- `redis` - Redis connectivity
- `faker` - Test data generation
- `psutil` - System monitoring (optional)

## Common Usage Patterns

### Quick Development Setup
```bash
# Start environment
./start-dev-env.sh

# Check everything is working
./check-health.py

# Generate test data
./generate-test-data.py --quick
```

### Clean Development Reset
```bash
# Reset all data
./reset-data.sh --all --confirm

# Generate fresh test data
./generate-test-data.py
```

### Troubleshooting
```bash
# Check specific services
./test-services.py

# Check monitoring stack
./test-monitoring.py

# Full system check with report
./check-health.py --save-report debug_report.json
```

### End of Development Session
```bash
# Stop services (preserve data)
./stop-dev-env.sh

# Or stop and clean everything
./stop-dev-env.sh --volumes --prune
```

## Error Handling

All scripts include:
- **Comprehensive error handling** with meaningful error messages
- **Colored output** for better visibility
- **Graceful failure** with proper exit codes
- **Help documentation** accessible via `--help`
- **Confirmation prompts** for destructive operations

## Integration with Makefile

These scripts are integrated with the project Makefile for convenience:

```bash
make dev-start          # ./start-dev-env.sh
make dev-stop           # ./stop-dev-env.sh
make dev-clean          # Clean and restart
make health-check       # ./check-health.py
make test-services      # ./test-services.py
make test-monitoring    # ./test-monitoring.py
make generate-data      # ./generate-test-data.py
make reset-data         # ./reset-data.sh --all --confirm
```

## Security Considerations

- Scripts use environment variables for configuration
- Database passwords are managed through Docker Compose
- No secrets are hardcoded in scripts
- Confirmation prompts prevent accidental data loss
- Scripts validate prerequisites before execution

## Troubleshooting Common Issues

### Script Permission Errors
```bash
# Make scripts executable
chmod +x scripts/*.sh scripts/*.py
```

### Docker Not Running
```bash
# Check Docker status
docker info

# Start Docker Desktop or daemon
```

### Service Connection Issues
```bash
# Check service status
docker-compose ps

# Check service logs
docker-compose logs -f [service-name]

# Restart specific service
docker-compose restart [service-name]
```

### Python Dependencies Missing
```bash
# Install dependencies
pip install -r requirements.txt
# or
poetry install
```

## Contributing

When adding new scripts:

1. Follow the existing naming convention
2. Include comprehensive help documentation
3. Add error handling and validation
4. Use colored output for better UX
5. Include the script in this README
6. Update the Makefile if appropriate
7. Make scripts executable (`chmod +x`)

## Support

For issues with scripts:
1. Check the help documentation: `./script-name.sh --help`
2. Run health checks: `./check-health.py`
3. Check logs: `docker-compose logs -f`
4. Review the troubleshooting section above
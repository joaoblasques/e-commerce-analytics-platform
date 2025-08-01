#!/bin/bash
# E-Commerce Analytics Platform - Development Environment Startup Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    print_status "Checking Docker availability..."
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running"
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed or not in PATH"
        exit 1
    fi

    print_success "Docker is available and running"
}

# Function to clean up existing containers
cleanup_containers() {
    print_status "Cleaning up existing containers..."
    cd "$PROJECT_DIR"

    # Stop and remove existing containers
    docker-compose down -v 2>/dev/null || true

    # Remove any dangling volumes
    docker volume prune -f 2>/dev/null || true

    print_success "Cleanup completed"
}

# Function to start services
start_services() {
    print_status "Starting e-commerce analytics platform services..."
    cd "$PROJECT_DIR"

    # Build and start all services
    docker-compose up -d --build

    print_success "Services started successfully"
}

# Function to wait for services to be healthy
wait_for_services() {
    print_status "Waiting for services to be healthy..."

    local max_attempts=60
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        attempt=$((attempt + 1))

        # Check service health
        local healthy_services=0
        local total_services=0

        # Get service health status
        local services_status=$(docker-compose ps --format json 2>/dev/null | jq -r '.Name + ":" + .Health' 2>/dev/null || echo "")

        if [ -n "$services_status" ]; then
            while IFS= read -r line; do
                if [ -n "$line" ]; then
                    total_services=$((total_services + 1))
                    if echo "$line" | grep -q "healthy"; then
                        healthy_services=$((healthy_services + 1))
                    fi
                fi
            done <<< "$services_status"
        fi

        # Simple fallback: check if containers are running
        if [ $total_services -eq 0 ]; then
            local running_containers=$(docker-compose ps -q | wc -l)
            local expected_containers=9  # Total number of services

            if [ "$running_containers" -eq "$expected_containers" ]; then
                print_success "All services are running"
                break
            fi
        else
            print_status "Health check: $healthy_services/$total_services services healthy"

            if [ $healthy_services -eq $total_services ] && [ $total_services -gt 0 ]; then
                print_success "All services are healthy"
                break
            fi
        fi

        if [ $attempt -eq $max_attempts ]; then
            print_warning "Some services may not be fully healthy yet, but continuing..."
            break
        fi

        sleep 10
    done
}

# Function to initialize MinIO buckets
init_minio() {
    print_status "Initializing MinIO buckets..."

    # Wait for MinIO to be ready
    local minio_ready=false
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ] && [ "$minio_ready" = false ]; do
        attempt=$((attempt + 1))

        if curl -s -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            minio_ready=true
            print_success "MinIO is ready"
        else
            print_status "Waiting for MinIO to be ready... (attempt $attempt/$max_attempts)"
            sleep 5
        fi
    done

    if [ "$minio_ready" = false ]; then
        print_warning "MinIO may not be fully ready, but continuing with bucket initialization"
    fi

    # Create MinIO client configuration
    docker-compose exec -T minio sh -c "
        mc alias set ecap-minio http://localhost:9000 minioadmin minioadmin123

        # Create buckets
        mc mb ecap-minio/raw-data --ignore-existing
        mc mb ecap-minio/processed-data --ignore-existing
        mc mb ecap-minio/analytics-results --ignore-existing
        mc mb ecap-minio/model-artifacts --ignore-existing
        mc mb ecap-minio/logs --ignore-existing
        mc mb ecap-minio/backups --ignore-existing

        # Set bucket policies
        mc anonymous set download ecap-minio/raw-data
        mc anonymous set download ecap-minio/processed-data
        mc anonymous set download ecap-minio/analytics-results

        echo 'MinIO buckets initialized successfully!'
    " 2>/dev/null || print_warning "MinIO bucket initialization had some issues, but continuing"
}

# Function to create Kafka topics
create_kafka_topics() {
    print_status "Creating Kafka topics..."

    # Wait for Kafka to be ready
    local kafka_ready=false
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ] && [ "$kafka_ready" = false ]; do
        attempt=$((attempt + 1))

        if docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
            kafka_ready=true
            print_success "Kafka is ready"
        else
            print_status "Waiting for Kafka to be ready... (attempt $attempt/$max_attempts)"
            sleep 5
        fi
    done

    if [ "$kafka_ready" = false ]; then
        print_warning "Kafka may not be fully ready, but continuing with topic creation"
    fi

    # Create topics
    docker-compose exec -T kafka bash -c "
        kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1 --if-not-exists
        kafka-topics --create --topic user-events --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1 --if-not-exists
        kafka-topics --create --topic product-updates --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
        kafka-topics --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
        kafka-topics --create --topic analytics-results --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

        echo 'Kafka topics created successfully!'
    " 2>/dev/null || print_warning "Kafka topic creation had some issues, but continuing"
}

# Function to display service information
display_service_info() {
    print_success "E-Commerce Analytics Platform is running!"
    echo ""
    echo "Service URLs:"
    echo "============="
    echo "• Kafka:                  localhost:9092"
    echo "• Zookeeper:              localhost:2181"
    echo "• PostgreSQL:             localhost:5432 (db: ecommerce_analytics, user: ecap_user)"
    echo "• Redis:                  localhost:6379 (password: redis_password)"
    echo "• MinIO Console:          http://localhost:9001 (admin/minioadmin123)"
    echo "• MinIO API:              http://localhost:9000"
    echo "• Spark Master UI:        http://localhost:8080"
    echo "• Spark Worker 1 UI:      http://localhost:8081"
    echo "• Spark Worker 2 UI:      http://localhost:8082"
    echo "• Spark History Server:   http://localhost:18080"
    echo ""
    echo "Docker containers:"
    echo "=================="
    docker-compose ps
    echo ""
    echo "To stop all services: docker-compose down"
    echo "To view logs: docker-compose logs -f [service-name]"
    echo "To rebuild services: docker-compose up -d --build"
}

# Function to show help
show_help() {
    echo "E-Commerce Analytics Platform - Development Environment Startup"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help      Show this help message"
    echo "  -c, --clean     Clean up existing containers before starting"
    echo "  --skip-init     Skip service initialization (MinIO buckets, Kafka topics)"
    echo ""
    echo "Examples:"
    echo "  $0              Start the development environment"
    echo "  $0 --clean      Clean up and start fresh"
    echo "  $0 --skip-init  Start services without initialization"
}

# Main execution
main() {
    local clean_first=false
    local skip_init=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -c|--clean)
                clean_first=true
                shift
                ;;
            --skip-init)
                skip_init=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done

    print_status "Starting E-Commerce Analytics Platform Development Environment"

    # Check prerequisites
    check_docker

    # Clean up if requested
    if [ "$clean_first" = true ]; then
        cleanup_containers
    fi

    # Start services
    start_services

    # Wait for services to be healthy
    wait_for_services

    # Initialize services if not skipped
    if [ "$skip_init" = false ]; then
        init_minio
        create_kafka_topics
    fi

    # Display service information
    display_service_info
}

# Run main function with all arguments
main "$@"

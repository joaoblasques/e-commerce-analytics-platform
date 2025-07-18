#!/bin/bash
# E-Commerce Analytics Platform - Data Reset Script
# This script resets all data in the development environment

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

# Function to show help
show_help() {
    echo "E-Commerce Analytics Platform - Data Reset Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help          Show this help message"
    echo "  --postgres          Reset PostgreSQL database only"
    echo "  --redis             Reset Redis cache only"
    echo "  --kafka             Reset Kafka topics only"
    echo "  --minio             Reset MinIO objects only"
    echo "  --spark             Reset Spark logs only"
    echo "  --logs              Reset application logs only"
    echo "  --all               Reset all data (default if no specific option)"
    echo "  --confirm           Skip confirmation prompt"
    echo ""
    echo "Examples:"
    echo "  $0                  Reset all data (with confirmation)"
    echo "  $0 --postgres       Reset only PostgreSQL data"
    echo "  $0 --kafka --redis  Reset Kafka and Redis data"
    echo "  $0 --all --confirm  Reset all data without confirmation"
}

# Function to check if services are running
check_services() {
    print_status "Checking if services are running..."
    cd "$PROJECT_DIR"
    
    local running_containers=$(docker-compose ps -q 2>/dev/null | wc -l)
    
    if [ "$running_containers" -eq 0 ]; then
        print_error "No services are running. Please start services first with:"
        print_error "  ./scripts/start-dev-env.sh"
        exit 1
    fi
    
    print_success "Services are running"
}

# Function to reset PostgreSQL database
reset_postgres() {
    print_status "Resetting PostgreSQL database..."
    
    # Connect to PostgreSQL and reset data
    docker-compose exec -T postgres psql -U ecap_user -d ecommerce_analytics << 'EOF'
-- Reset all tables in the correct order (respecting foreign key constraints)
TRUNCATE TABLE IF EXISTS system.service_health RESTART IDENTITY CASCADE;
TRUNCATE TABLE IF EXISTS analytics.customer_segments RESTART IDENTITY CASCADE;
TRUNCATE TABLE IF EXISTS analytics.fraud_alerts RESTART IDENTITY CASCADE;
TRUNCATE TABLE IF EXISTS analytics.product_analytics RESTART IDENTITY CASCADE;
TRUNCATE TABLE IF EXISTS analytics.revenue_analytics RESTART IDENTITY CASCADE;
TRUNCATE TABLE IF EXISTS ecommerce.order_items RESTART IDENTITY CASCADE;
TRUNCATE TABLE IF EXISTS ecommerce.orders RESTART IDENTITY CASCADE;
TRUNCATE TABLE IF EXISTS ecommerce.products RESTART IDENTITY CASCADE;
TRUNCATE TABLE IF EXISTS ecommerce.customers RESTART IDENTITY CASCADE;

-- Reset sequences
SELECT setval('ecommerce.customers_customer_id_seq', 1, false);
SELECT setval('ecommerce.products_product_id_seq', 1, false);
SELECT setval('ecommerce.orders_order_id_seq', 1, false);

-- Verify reset
SELECT 'customers' as table_name, COUNT(*) as count FROM ecommerce.customers
UNION ALL
SELECT 'products', COUNT(*) FROM ecommerce.products
UNION ALL
SELECT 'orders', COUNT(*) FROM ecommerce.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM ecommerce.order_items;

-- Insert fresh health check record
INSERT INTO system.service_health (service_name, status, last_check) 
VALUES ('data_reset', 'completed', NOW());
EOF

    if [ $? -eq 0 ]; then
        print_success "PostgreSQL database reset completed"
    else
        print_error "Failed to reset PostgreSQL database"
        return 1
    fi
}

# Function to reset Redis cache
reset_redis() {
    print_status "Resetting Redis cache..."
    
    # Connect to Redis and flush all data
    docker-compose exec -T redis redis-cli -a redis_password FLUSHALL
    
    if [ $? -eq 0 ]; then
        print_success "Redis cache reset completed"
    else
        print_error "Failed to reset Redis cache"
        return 1
    fi
}

# Function to reset Kafka topics
reset_kafka() {
    print_status "Resetting Kafka topics..."
    
    # Delete and recreate topics
    docker-compose exec -T kafka bash -c "
        # Delete existing topics
        kafka-topics --bootstrap-server localhost:9092 --delete --topic transactions 2>/dev/null || true
        kafka-topics --bootstrap-server localhost:9092 --delete --topic user-events 2>/dev/null || true
        kafka-topics --bootstrap-server localhost:9092 --delete --topic product-updates 2>/dev/null || true
        kafka-topics --bootstrap-server localhost:9092 --delete --topic fraud-alerts 2>/dev/null || true
        kafka-topics --bootstrap-server localhost:9092 --delete --topic analytics-results 2>/dev/null || true
        
        # Wait a moment for deletion to complete
        sleep 2
        
        # Recreate topics
        kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1 --if-not-exists
        kafka-topics --create --topic user-events --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1 --if-not-exists
        kafka-topics --create --topic product-updates --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
        kafka-topics --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
        kafka-topics --create --topic analytics-results --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
        
        echo 'Kafka topics reset completed'
    "
    
    if [ $? -eq 0 ]; then
        print_success "Kafka topics reset completed"
    else
        print_error "Failed to reset Kafka topics"
        return 1
    fi
}

# Function to reset MinIO objects
reset_minio() {
    print_status "Resetting MinIO objects..."
    
    # Remove all objects from buckets and recreate bucket structure
    docker-compose exec -T minio sh -c "
        # Configure MinIO client
        mc alias set ecap-minio http://localhost:9000 minioadmin minioadmin123
        
        # Remove all objects from buckets
        mc rm --recursive --force ecap-minio/raw-data/ 2>/dev/null || true
        mc rm --recursive --force ecap-minio/processed-data/ 2>/dev/null || true
        mc rm --recursive --force ecap-minio/analytics-results/ 2>/dev/null || true
        mc rm --recursive --force ecap-minio/model-artifacts/ 2>/dev/null || true
        mc rm --recursive --force ecap-minio/logs/ 2>/dev/null || true
        mc rm --recursive --force ecap-minio/backups/ 2>/dev/null || true
        
        # Recreate bucket structure
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
        
        echo 'MinIO objects reset completed'
    "
    
    if [ $? -eq 0 ]; then
        print_success "MinIO objects reset completed"
    else
        print_error "Failed to reset MinIO objects"
        return 1
    fi
}

# Function to reset Spark logs
reset_spark() {
    print_status "Resetting Spark logs..."
    
    # Clear Spark logs from the volume
    docker-compose exec -T spark-master sh -c "
        rm -rf /opt/bitnami/spark/logs/*
        mkdir -p /opt/bitnami/spark/logs
        echo 'Spark logs reset completed'
    "
    
    if [ $? -eq 0 ]; then
        print_success "Spark logs reset completed"
    else
        print_error "Failed to reset Spark logs"
        return 1
    fi
}

# Function to reset application logs
reset_logs() {
    print_status "Resetting application logs..."
    
    cd "$PROJECT_DIR"
    
    # Clear log directories
    rm -rf logs/kafka/*
    rm -rf logs/minio/*
    rm -rf logs/postgres/*
    rm -rf logs/redis/*
    rm -rf logs/spark/*
    
    # Recreate log directories
    mkdir -p logs/kafka
    mkdir -p logs/minio
    mkdir -p logs/postgres
    mkdir -p logs/redis
    mkdir -p logs/spark
    
    # Create .gitkeep files
    touch logs/kafka/.gitkeep
    touch logs/minio/.gitkeep
    touch logs/postgres/.gitkeep
    touch logs/redis/.gitkeep
    touch logs/spark/.gitkeep
    
    print_success "Application logs reset completed"
}

# Function to show reset summary
show_reset_summary() {
    print_status "Data reset summary:"
    echo ""
    echo "The following data has been reset:"
    
    if [ "$reset_postgres_flag" = true ]; then
        echo "  ✅ PostgreSQL database (all tables truncated)"
    fi
    
    if [ "$reset_redis_flag" = true ]; then
        echo "  ✅ Redis cache (all keys flushed)"
    fi
    
    if [ "$reset_kafka_flag" = true ]; then
        echo "  ✅ Kafka topics (deleted and recreated)"
    fi
    
    if [ "$reset_minio_flag" = true ]; then
        echo "  ✅ MinIO objects (all objects removed)"
    fi
    
    if [ "$reset_spark_flag" = true ]; then
        echo "  ✅ Spark logs (cleared)"
    fi
    
    if [ "$reset_logs_flag" = true ]; then
        echo "  ✅ Application logs (cleared)"
    fi
    
    echo ""
    print_success "Data reset completed successfully!"
    echo ""
    echo "Your development environment now has clean data."
    echo "You can start generating new data or run tests."
}

# Main execution
main() {
    local reset_postgres_flag=false
    local reset_redis_flag=false
    local reset_kafka_flag=false
    local reset_minio_flag=false
    local reset_spark_flag=false
    local reset_logs_flag=false
    local reset_all_flag=false
    local skip_confirmation=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            --postgres)
                reset_postgres_flag=true
                shift
                ;;
            --redis)
                reset_redis_flag=true
                shift
                ;;
            --kafka)
                reset_kafka_flag=true
                shift
                ;;
            --minio)
                reset_minio_flag=true
                shift
                ;;
            --spark)
                reset_spark_flag=true
                shift
                ;;
            --logs)
                reset_logs_flag=true
                shift
                ;;
            --all)
                reset_all_flag=true
                shift
                ;;
            --confirm)
                skip_confirmation=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # If no specific options, reset all
    if [ "$reset_postgres_flag" = false ] && [ "$reset_redis_flag" = false ] && \
       [ "$reset_kafka_flag" = false ] && [ "$reset_minio_flag" = false ] && \
       [ "$reset_spark_flag" = false ] && [ "$reset_logs_flag" = false ]; then
        reset_all_flag=true
    fi
    
    # Set flags for reset all
    if [ "$reset_all_flag" = true ]; then
        reset_postgres_flag=true
        reset_redis_flag=true
        reset_kafka_flag=true
        reset_minio_flag=true
        reset_spark_flag=true
        reset_logs_flag=true
    fi
    
    # Check prerequisites
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed or not in PATH"
        exit 1
    fi
    
    # Check services
    check_services
    
    # Confirmation prompt
    if [ "$skip_confirmation" = false ]; then
        echo ""
        print_warning "WARNING: This will permanently delete the following data:"
        
        if [ "$reset_postgres_flag" = true ]; then
            print_warning "  - All PostgreSQL database records"
        fi
        
        if [ "$reset_redis_flag" = true ]; then
            print_warning "  - All Redis cache data"
        fi
        
        if [ "$reset_kafka_flag" = true ]; then
            print_warning "  - All Kafka message history"
        fi
        
        if [ "$reset_minio_flag" = true ]; then
            print_warning "  - All MinIO stored objects"
        fi
        
        if [ "$reset_spark_flag" = true ]; then
            print_warning "  - All Spark execution logs"
        fi
        
        if [ "$reset_logs_flag" = true ]; then
            print_warning "  - All application logs"
        fi
        
        echo ""
        read -p "Are you sure you want to continue? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_status "Operation cancelled"
            exit 0
        fi
    fi
    
    print_status "Starting data reset process..."
    
    # Execute reset operations
    local failed_operations=0
    
    if [ "$reset_postgres_flag" = true ]; then
        reset_postgres || failed_operations=$((failed_operations + 1))
    fi
    
    if [ "$reset_redis_flag" = true ]; then
        reset_redis || failed_operations=$((failed_operations + 1))
    fi
    
    if [ "$reset_kafka_flag" = true ]; then
        reset_kafka || failed_operations=$((failed_operations + 1))
    fi
    
    if [ "$reset_minio_flag" = true ]; then
        reset_minio || failed_operations=$((failed_operations + 1))
    fi
    
    if [ "$reset_spark_flag" = true ]; then
        reset_spark || failed_operations=$((failed_operations + 1))
    fi
    
    if [ "$reset_logs_flag" = true ]; then
        reset_logs || failed_operations=$((failed_operations + 1))
    fi
    
    # Show summary
    echo ""
    if [ $failed_operations -eq 0 ]; then
        show_reset_summary
    else
        print_error "Some reset operations failed ($failed_operations failures)"
        print_error "Check the output above for details"
        exit 1
    fi
}

# Make variables available to functions
reset_postgres_flag=false
reset_redis_flag=false
reset_kafka_flag=false
reset_minio_flag=false
reset_spark_flag=false
reset_logs_flag=false

# Run main function with all arguments
main "$@"
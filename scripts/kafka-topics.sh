#!/bin/bash

# Kafka Topic Management Script for E-Commerce Analytics Platform
# This script provides convenient wrapper functions for common Kafka operations

set -e

# Colors for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_CONTAINER="ecap-kafka"
KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/manage_kafka.py"

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_docker() {
    if ! docker ps >/dev/null 2>&1; then
        log_error "Docker is not running or not accessible"
        exit 1
    fi
}

check_kafka_container() {
    if ! docker ps | grep -q "$KAFKA_CONTAINER"; then
        log_error "Kafka container '$KAFKA_CONTAINER' is not running"
        log_info "Start the development environment with: ./start-dev-env.sh"
        exit 1
    fi
}

check_python_deps() {
    if ! python3 -c "import kafka, click" >/dev/null 2>&1; then
        log_warning "Required Python packages not found"
        log_info "Installing kafka-python and click..."
        pip3 install kafka-python click
    fi
}

# Main functions
create_topics() {
    log_info "Creating Kafka topics for E-Commerce Analytics Platform..."
    
    check_docker
    check_kafka_container
    check_python_deps
    
    # Wait for Kafka to be ready
    log_info "Waiting for Kafka to be ready..."
    timeout=30
    while ! docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
        sleep 2
        timeout=$((timeout - 2))
        if [ $timeout -le 0 ]; then
            log_error "Kafka is not responding after 30 seconds"
            exit 1
        fi
    done
    
    # Create topics using Python script
    if python3 "$PYTHON_SCRIPT" create-topics; then
        log_success "All topics created successfully!"
        
        # Show topic summary
        echo ""
        log_info "Topic Summary:"
        echo "=============="
        python3 "$PYTHON_SCRIPT" list-topics
    else
        log_error "Failed to create topics"
        exit 1
    fi
}

list_topics() {
    log_info "Listing Kafka topics..."
    
    check_docker
    check_kafka_container
    check_python_deps
    
    python3 "$PYTHON_SCRIPT" list-topics
}

describe_topic() {
    local topic_name="$1"
    
    if [ -z "$topic_name" ]; then
        log_error "Please provide a topic name"
        echo "Usage: $0 describe-topic <topic-name>"
        exit 1
    fi
    
    log_info "Describing topic: $topic_name"
    
    check_docker
    check_kafka_container
    check_python_deps
    
    python3 "$PYTHON_SCRIPT" describe-topic "$topic_name"
}

delete_topic() {
    local topic_name="$1"
    
    if [ -z "$topic_name" ]; then
        log_error "Please provide a topic name"
        echo "Usage: $0 delete-topic <topic-name>"
        exit 1
    fi
    
    log_warning "This will permanently delete the topic '$topic_name'"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        check_docker
        check_kafka_container
        check_python_deps
        
        python3 "$PYTHON_SCRIPT" delete-topic "$topic_name"
    else
        log_info "Topic deletion cancelled"
    fi
}

health_check() {
    log_info "Performing Kafka health check..."
    
    check_docker
    check_kafka_container
    check_python_deps
    
    python3 "$PYTHON_SCRIPT" health-check
}

consumer_groups() {
    log_info "Listing consumer groups..."
    
    check_docker
    check_kafka_container
    check_python_deps
    
    python3 "$PYTHON_SCRIPT" list-consumer-groups
}

test_produce() {
    local topic_name="$1"
    local message="$2"
    
    if [ -z "$topic_name" ]; then
        log_error "Please provide a topic name"
        echo "Usage: $0 test-produce <topic-name> [message]"
        exit 1
    fi
    
    if [ -z "$message" ]; then
        message='{"test": "message", "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}'
    fi
    
    log_info "Sending test message to topic: $topic_name"
    
    check_docker
    check_kafka_container
    check_python_deps
    
    python3 "$PYTHON_SCRIPT" test-produce "$topic_name" --message "$message"
}

# Direct Kafka CLI commands (for advanced users)
kafka_cli() {
    local command="$1"
    shift
    
    check_docker
    check_kafka_container
    
    log_info "Executing Kafka CLI command: $command"
    docker exec -it "$KAFKA_CONTAINER" "$command" --bootstrap-server localhost:9092 "$@"
}

# Reset all topics (dangerous operation)
reset_topics() {
    log_warning "This will DELETE ALL TOPICS and recreate them"
    log_warning "All data will be lost!"
    echo
    read -p "Are you sure you want to continue? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Resetting all topics..."
        
        check_docker
        check_kafka_container
        check_python_deps
        
        # Get list of topics to delete (excluding internal topics)
        topics=$(python3 "$PYTHON_SCRIPT" list-topics 2>/dev/null | grep -v "^Found\|^$" | grep -v "__consumer_offsets\|__transaction_state\|_schemas")
        
        if [ -n "$topics" ]; then
            log_info "Deleting existing topics..."
            while IFS= read -r topic; do
                if [[ "$topic" != __* ]]; then
                    log_info "Deleting topic: $topic"
                    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
                fi
            done <<< "$topics"
            
            # Wait a bit for deletion to complete
            sleep 3
        fi
        
        # Recreate topics
        log_info "Creating fresh topics..."
        create_topics
        
        log_success "Topic reset completed!"
    else
        log_info "Topic reset cancelled"
    fi
}

# Show usage information
usage() {
    echo "Kafka Topic Management Script for E-Commerce Analytics Platform"
    echo ""
    echo "Usage: $0 <command> [arguments]"
    echo ""
    echo "Commands:"
    echo "  create-topics              Create all configured topics"
    echo "  list-topics               List all topics"
    echo "  describe-topic <topic>    Describe a specific topic"
    echo "  delete-topic <topic>      Delete a topic"
    echo "  health-check              Perform cluster health check"
    echo "  consumer-groups           List consumer groups"
    echo "  test-produce <topic> [msg] Send a test message"
    echo "  reset-topics              Delete and recreate all topics (DANGEROUS)"
    echo "  kafka-cli <command> [args] Execute Kafka CLI command"
    echo "  help                      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 create-topics"
    echo "  $0 describe-topic transactions"
    echo "  $0 test-produce user-events '{\"user_id\": \"test\", \"event\": \"page_view\"}'"
    echo "  $0 kafka-cli kafka-console-consumer --topic transactions --from-beginning"
    echo ""
    echo "Prerequisites:"
    echo "  - Docker running with Kafka container"
    echo "  - Python 3 with kafka-python and click packages"
    echo "  - Run ./start-dev-env.sh to start the development environment"
    echo ""
}

# Main script logic
case "$1" in
    "create-topics")
        create_topics
        ;;
    "list-topics")
        list_topics
        ;;
    "describe-topic")
        describe_topic "$2"
        ;;
    "delete-topic")
        delete_topic "$2"
        ;;
    "health-check")
        health_check
        ;;
    "consumer-groups")
        consumer_groups
        ;;
    "test-produce")
        test_produce "$2" "$3"
        ;;
    "reset-topics")
        reset_topics
        ;;
    "kafka-cli")
        shift
        kafka_cli "$@"
        ;;
    "help"|"-h"|"--help")
        usage
        ;;
    *)
        log_error "Unknown command: $1"
        echo ""
        usage
        exit 1
        ;;
esac
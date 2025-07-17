#!/bin/bash
# E-Commerce Analytics Platform - Development Environment Shutdown Script

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
    echo "E-Commerce Analytics Platform - Development Environment Shutdown"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help      Show this help message"
    echo "  -v, --volumes   Also remove Docker volumes (data will be lost)"
    echo "  -f, --force     Force removal of containers"
    echo "  --prune         Clean up unused Docker resources after shutdown"
    echo ""
    echo "Examples:"
    echo "  $0              Stop services (keep data)"
    echo "  $0 --volumes    Stop services and remove all data"
    echo "  $0 --prune      Stop services and clean up unused resources"
}

# Function to stop services
stop_services() {
    local remove_volumes=$1
    local force_remove=$2
    
    print_status "Stopping E-Commerce Analytics Platform services..."
    cd "$PROJECT_DIR"
    
    # Build docker-compose command
    local compose_cmd="docker-compose down"
    
    if [ "$remove_volumes" = true ]; then
        compose_cmd="$compose_cmd -v"
        print_warning "This will remove all data volumes!"
    fi
    
    if [ "$force_remove" = true ]; then
        compose_cmd="$compose_cmd --remove-orphans"
    fi
    
    # Execute the command
    eval "$compose_cmd"
    
    if [ "$remove_volumes" = true ]; then
        print_success "Services stopped and data volumes removed"
    else
        print_success "Services stopped (data volumes preserved)"
    fi
}

# Function to clean up Docker resources
cleanup_docker() {
    print_status "Cleaning up unused Docker resources..."
    
    # Remove unused networks
    docker network prune -f 2>/dev/null || true
    
    # Remove unused images
    docker image prune -f 2>/dev/null || true
    
    # Remove build cache
    docker builder prune -f 2>/dev/null || true
    
    print_success "Docker cleanup completed"
}

# Function to show current status
show_status() {
    print_status "Checking current service status..."
    cd "$PROJECT_DIR"
    
    local running_containers=$(docker-compose ps -q 2>/dev/null | wc -l)
    
    if [ "$running_containers" -eq 0 ]; then
        print_success "No services are currently running"
    else
        print_status "Currently running services:"
        docker-compose ps
    fi
}

# Main execution
main() {
    local remove_volumes=false
    local force_remove=false
    local prune_resources=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -v|--volumes)
                remove_volumes=true
                shift
                ;;
            -f|--force)
                force_remove=true
                shift
                ;;
            --prune)
                prune_resources=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed or not in PATH"
        exit 1
    fi
    
    # Show current status
    show_status
    
    # Confirm if removing volumes
    if [ "$remove_volumes" = true ]; then
        echo ""
        print_warning "WARNING: This will permanently delete all data!"
        print_warning "The following will be removed:"
        print_warning "  - PostgreSQL database data"
        print_warning "  - Kafka message history"
        print_warning "  - Redis cache data"
        print_warning "  - MinIO stored objects"
        print_warning "  - Spark logs"
        echo ""
        read -p "Are you sure you want to continue? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_status "Operation cancelled"
            exit 0
        fi
    fi
    
    # Stop services
    stop_services "$remove_volumes" "$force_remove"
    
    # Clean up if requested
    if [ "$prune_resources" = true ]; then
        cleanup_docker
    fi
    
    # Final status
    echo ""
    print_success "E-Commerce Analytics Platform shutdown completed"
    
    if [ "$remove_volumes" = false ]; then
        print_status "Data volumes have been preserved"
        print_status "To start again: ./scripts/start-dev-env.sh"
    else
        print_status "All data has been removed"
        print_status "To start fresh: ./scripts/start-dev-env.sh --clean"
    fi
}

# Run main function with all arguments
main "$@"
#!/bin/bash

# User Behavior Event Producer Runner Script
# This script runs the user behavior event producer with configurable parameters

set -e

# Default configuration
BOOTSTRAP_SERVERS="localhost:9092"
TOPIC="user-events"
RATE=5000
DURATION=""
MIN_SESSION_DURATION=60
MAX_SESSION_DURATION=1800
VERBOSE=false
QUIET=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Help function
show_help() {
    echo "User Behavior Event Producer Runner"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "OPTIONS:"
    echo "  -s, --servers SERVERS     Kafka bootstrap servers (default: localhost:9092)"
    echo "  -t, --topic TOPIC         Kafka topic name (default: user-events)"
    echo "  -r, --rate RATE          Events generation rate per hour (default: 5000)"
    echo "  -d, --duration DURATION   Duration to run in seconds (default: infinite)"
    echo "  --min-session MIN         Minimum session duration in seconds (default: 60)"
    echo "  --max-session MAX         Maximum session duration in seconds (default: 1800)"
    echo "  -v, --verbose             Enable verbose logging"
    echo "  -q, --quiet               Suppress all output except errors"
    echo "  -h, --help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                          # Run with default settings"
    echo "  $0 -r 10000 -d 300                        # Run at 10k events/hour for 5 minutes"
    echo "  $0 -s kafka1:9092,kafka2:9092 -t events  # Use custom servers and topic"
    echo "  $0 --min-session 30 --max-session 900     # Custom session duration range"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--servers)
            BOOTSTRAP_SERVERS="$2"
            shift 2
            ;;
        -t|--topic)
            TOPIC="$2"
            shift 2
            ;;
        -r|--rate)
            RATE="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        --min-session)
            MIN_SESSION_DURATION="$2"
            shift 2
            ;;
        --max-session)
            MAX_SESSION_DURATION="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -q|--quiet)
            QUIET=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate numeric parameters
if ! [[ "$RATE" =~ ^[0-9]+(\.[0-9]+)?$ ]] || (( $(echo "$RATE <= 0" | bc -l) )); then
    echo -e "${RED}Error: Rate must be a positive number${NC}"
    exit 1
fi

if [[ -n "$DURATION" ]] && ! [[ "$DURATION" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: Duration must be a positive integer${NC}"
    exit 1
fi

if ! [[ "$MIN_SESSION_DURATION" =~ ^[0-9]+$ ]] || ! [[ "$MAX_SESSION_DURATION" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: Session durations must be positive integers${NC}"
    exit 1
fi

if (( MIN_SESSION_DURATION >= MAX_SESSION_DURATION )); then
    echo -e "${RED}Error: Minimum session duration must be less than maximum${NC}"
    exit 1
fi

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Check if we're in a poetry environment
if command -v poetry &> /dev/null && [ -f "$PROJECT_ROOT/pyproject.toml" ]; then
    PYTHON_CMD="poetry run python"
else
    PYTHON_CMD="python"
fi

# Check if the producer module exists
PRODUCER_MODULE="$PROJECT_ROOT/src/data_ingestion/producers/user_behavior_cli.py"
if [ ! -f "$PRODUCER_MODULE" ]; then
    echo -e "${RED}Error: Producer module not found at $PRODUCER_MODULE${NC}"
    exit 1
fi

# Build the command
CMD="$PYTHON_CMD $PRODUCER_MODULE --bootstrap-servers $BOOTSTRAP_SERVERS --topic $TOPIC --rate $RATE"
CMD="$CMD --min-session-duration $MIN_SESSION_DURATION --max-session-duration $MAX_SESSION_DURATION"

if [[ -n "$DURATION" ]]; then
    CMD="$CMD --duration $DURATION"
fi

if [[ "$VERBOSE" == true ]]; then
    CMD="$CMD --verbose"
fi

if [[ "$QUIET" == true ]]; then
    CMD="$CMD --quiet"
fi

# Print configuration (unless quiet)
if [[ "$QUIET" != true ]]; then
    echo -e "${BLUE}🔧 User Behavior Event Producer Configuration:${NC}"
    echo -e "${YELLOW}   📡 Kafka Servers:${NC} $BOOTSTRAP_SERVERS"
    echo -e "${YELLOW}   📝 Topic:${NC} $TOPIC"
    echo -e "${YELLOW}   ⚡ Generation Rate:${NC} $RATE events/hour"
    echo -e "${YELLOW}   ⏱️  Session Duration:${NC} $MIN_SESSION_DURATION-$MAX_SESSION_DURATION seconds"
    if [[ -n "$DURATION" ]]; then
        echo -e "${YELLOW}   ⏰ Duration:${NC} $DURATION seconds"
    else
        echo -e "${YELLOW}   ⏰ Duration:${NC} Infinite"
    fi
    echo -e "${YELLOW}   🔧 Python Command:${NC} $PYTHON_CMD"
    echo ""
fi

# Function to cleanup on exit
cleanup() {
    if [[ "$QUIET" != true ]]; then
        echo -e "\n${YELLOW}🛑 Shutting down user behavior producer...${NC}"
    fi
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Change to project directory
cd "$PROJECT_ROOT"

# Run the producer
if [[ "$QUIET" != true ]]; then
    echo -e "${GREEN}🚀 Starting User Behavior Event Producer...${NC}"
    echo -e "${BLUE}Press Ctrl+C to stop${NC}"
    echo ""
fi

# Execute the command
eval $CMD

# If we get here, the producer finished normally
if [[ "$QUIET" != true ]]; then
    echo -e "${GREEN}✅ User Behavior Event Producer completed successfully${NC}"
fi

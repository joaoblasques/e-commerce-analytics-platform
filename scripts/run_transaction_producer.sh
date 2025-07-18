#!/bin/bash
# Script to run the transaction data producer

set -e

# Configuration
BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-"localhost:9092"}
TOPIC=${KAFKA_TOPIC:-"transactions"}
RATE=${GENERATION_RATE:-1000}
DURATION=${DURATION:-}
LOG_LEVEL=${LOG_LEVEL:-"INFO"}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Transaction Data Producer${NC}"
echo "Configuration:"
echo "  Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo "  Topic: $TOPIC"
echo "  Generation Rate: $RATE transactions/hour"
echo "  Duration: ${DURATION:-"infinite"}"
echo "  Log Level: $LOG_LEVEL"
echo ""

# Check if Kafka is accessible
echo -e "${YELLOW}Checking Kafka connectivity...${NC}"
if ! python -c "
from kafka import KafkaProducer
try:
    producer = KafkaProducer(bootstrap_servers='$BOOTSTRAP_SERVERS', request_timeout_ms=5000)
    producer.close()
    print('✓ Kafka is accessible')
except Exception as e:
    print('✗ Kafka is not accessible:', e)
    exit(1)
"; then
    echo -e "${RED}Error: Cannot connect to Kafka at $BOOTSTRAP_SERVERS${NC}"
    echo "Please make sure Kafka is running and accessible."
    exit 1
fi

# Build the command
CMD="python -m src.data_ingestion.producers.producer_cli"
CMD="$CMD --bootstrap-servers $BOOTSTRAP_SERVERS"
CMD="$CMD --log-level $LOG_LEVEL"
CMD="$CMD transaction"
CMD="$CMD --topic $TOPIC"
CMD="$CMD --rate $RATE"

if [ ! -z "$DURATION" ]; then
    CMD="$CMD --duration $DURATION"
fi

echo -e "${GREEN}Starting producer...${NC}"
echo "Command: $CMD"
echo "Press Ctrl+C to stop the producer"
echo ""

# Run the producer
exec $CMD

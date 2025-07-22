# Transaction Data Producer

The Transaction Data Producer is a sophisticated Kafka producer that generates realistic e-commerce transaction data for the analytics platform. It's designed to simulate real-world transaction patterns with configurable generation rates and intelligent time-based multipliers.

## Features

- **Realistic Data Generation**: Creates transaction data that mimics real e-commerce patterns
- **Configurable Generation Rates**: Adjustable transactions per hour with time-based multipliers
- **Intelligent Partitioning**: Uses user_id as partition key for optimal data distribution
- **Comprehensive Monitoring**: Built-in metrics and monitoring capabilities
- **Error Handling**: Robust error handling with retry mechanisms
- **Time-Based Patterns**: Simulates business hours, peak times, and weekend patterns

## Architecture

The producer is built with a modular architecture:

```
src/data_ingestion/producers/
├── base_producer.py          # Base class with common functionality
├── transaction_producer.py   # Transaction-specific producer
├── producer_cli.py          # Command-line interface
└── __init__.py
```

## Transaction Data Schema

Each transaction message contains the following fields:

```json
{
  "transaction_id": "txn_20240716_123456789",
  "user_id": "user_987654321",
  "session_id": "sess_1689520800_user987654321",
  "product_id": "prod_electronics_1234",
  "product_name": "MacBook Pro 14-inch",
  "category_id": "cat_electronics",
  "category_name": "Electronics",
  "subcategory": "Laptops",
  "brand": "Apple",
  "price": 1999.99,
  "quantity": 1,
  "discount_applied": 0.0,
  "payment_method": "credit_card",
  "payment_provider": "stripe",
  "currency": "USD",
  "timestamp": "2024-07-16T14:30:45.123Z",
  "user_location": {
    "country": "US",
    "state": "CA",
    "city": "San Francisco",
    "zip_code": "94105",
    "latitude": 37.7749,
    "longitude": -122.4194
  },
  "device_info": {
    "type": "desktop",
    "os": "macOS",
    "browser": "Chrome",
    "user_agent": "Mozilla/5.0..."
  },
  "marketing_attribution": {
    "campaign_id": "summer_sale_2024",
    "channel": "google_ads",
    "referrer": "google.com"
  }
}
```

## Usage

### Command Line Interface

The producer can be run using the CLI:

```bash
# Run with default settings (1000 transactions/hour to 'transactions' topic)
python -m src.data_ingestion.producers.producer_cli transaction

# Custom configuration
python -m src.data_ingestion.producers.producer_cli transaction \\\n  --topic my_transactions \\\n  --rate 500 \\\n  --duration 3600 \\\n  --bootstrap-servers localhost:9092
```

### Using the Shell Script

A convenient shell script is provided:

```bash
# Run with environment variables
export GENERATION_RATE=2000
export KAFKA_TOPIC=transactions
export DURATION=1800
./scripts/run_transaction_producer.sh
```

### Python API

```python
from src.data_ingestion.producers.transaction_producer import TransactionProducer

# Create producer
producer = TransactionProducer(
    bootstrap_servers=\"localhost:9092\",
    topic=\"transactions\",
    generation_rate=1000.0  # transactions per hour
)

# Generate and send messages
try:
    producer.run_continuous(duration_seconds=3600)  # Run for 1 hour
except KeyboardInterrupt:
    print(\"Stopped by user\")
finally:
    producer.close()
```

## Configuration

### Producer Configuration

The producer accepts standard Kafka producer configuration:

```python
producer_config = {
    \"acks\": \"all\",
    \"retries\": 3,
    \"batch_size\": 16384,
    \"linger_ms\": 10,
    \"compression_type\": \"snappy\",
}

producer = TransactionProducer(
    bootstrap_servers=\"localhost:9092\",
    topic=\"transactions\",
    producer_config=producer_config
)
```

### Environment Variables

The shell script supports these environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: localhost:9092)
- `KAFKA_TOPIC`: Target topic (default: transactions)
- `GENERATION_RATE`: Transactions per hour (default: 1000)
- `DURATION`: Run duration in seconds (default: infinite)
- `LOG_LEVEL`: Logging level (default: INFO)

## Time-Based Patterns

The producer simulates realistic transaction patterns:

### Business Hours (9 AM - 5 PM)
- **Multiplier**: 1.5x
- **Effect**: Increased transaction volume during business hours

### Peak Hours (12 PM - 2 PM)
- **Multiplier**: 2.0x
- **Effect**: Lunch time shopping surge

### Weekend (Saturday & Sunday)
- **Multiplier**: 1.5x
- **Effect**: Higher weekend shopping activity

### Late Night (10 PM - 6 AM)
- **Multiplier**: 0.3x
- **Effect**: Reduced late-night activity

## Data Realism

The producer generates realistic data using:

### Product Categories
- Electronics (25% weight)
- Clothing (20% weight)
- Books (15% weight)
- Home & Garden (15% weight)
- Sports & Outdoors (10% weight)
- Beauty & Health (10% weight)
- Toys & Games (5% weight)

### Payment Methods
- Credit Card (60% weight)
- Debit Card (25% weight)
- PayPal (10% weight)
- Apple Pay (5% weight)

### Geographic Distribution
- US, Canada, UK locations
- Realistic city/state/zip combinations
- Coordinate generation near city centers

## Monitoring

The producer provides comprehensive metrics:

```python
metrics = producer.get_metrics()
print(f\"Messages sent: {metrics['messages_sent']}\")
print(f\"Messages failed: {metrics['messages_failed']}\")
print(f\"Bytes sent: {metrics['bytes_sent']}\")
print(f\"Last send time: {metrics['last_send_time']}\")
```

### Monitoring Integration

The producer integrates with the monitoring system:

```python
from src.data_ingestion.monitoring import monitor

# Register producer
metrics = monitor.register_producer(\"my_producer\")

# Get aggregate metrics
aggregate = monitor.get_aggregate_metrics()
```

## Error Handling

The producer includes robust error handling:

- **Kafka Errors**: Automatic retries with exponential backoff
- **Network Issues**: Connection timeout and retry logic
- **Message Validation**: Schema validation before sending
- **Graceful Shutdown**: Clean resource cleanup on exit

## Performance Optimization

### Batch Processing
- Configurable batch sizes for improved throughput
- Linger settings to optimize batching

### Compression
- Snappy compression by default
- Reduces network bandwidth usage

### Partitioning
- User ID-based partitioning for optimal data distribution
- Ensures related transactions are in the same partition

## Testing

Run the test suite:

```bash
# Run all producer tests
pytest tests/test_transaction_producer.py -v

# Run with coverage
pytest tests/test_transaction_producer.py --cov=src.data_ingestion.producers
```

## Troubleshooting

### Common Issues

1. **Cannot connect to Kafka**
   - Check if Kafka is running: `docker-compose ps kafka`
   - Verify bootstrap servers: `localhost:9092`

2. **Low throughput**
   - Increase batch size: `batch_size=32768`
   - Reduce linger time: `linger_ms=5`

3. **Out of memory errors**
   - Reduce buffer memory: `buffer_memory=16777216`
   - Increase flush frequency

### Debug Mode

Enable debug logging:

```bash
python -m src.data_ingestion.producers.producer_cli \\\n  --log-level DEBUG \\\n  transaction
```

## Development

### Adding New Data Patterns

To add new product categories or patterns:

1. Update `_init_data_pools()` in `TransactionProducer`
2. Add new weighted choices to appropriate lists
3. Update tests to verify new patterns

### Custom Producers

Create custom producers by extending `BaseProducer`:

```python
from src.data_ingestion.producers.base_producer import BaseProducer

class CustomProducer(BaseProducer):
    def generate_message(self):
        # Custom message generation logic
        pass

    def get_message_key(self, message):
        # Custom partitioning logic
        pass
```

## Examples

See `examples/transaction_producer_example.py` for a complete working example.

## Contributing

When contributing to the transaction producer:

1. Follow the existing code style
2. Add tests for new functionality
3. Update documentation
4. Ensure all tests pass
5. Test with real Kafka cluster

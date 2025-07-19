# Kafka Producer Reliability Features

This document describes the enhanced reliability features added to the Kafka data producers in the E-Commerce Analytics Platform.

## Overview

The reliability features provide robust error handling, retry mechanisms, message deduplication, and health monitoring to ensure data integrity and system resilience.

## Key Features

### 1. Dead Letter Queue (DLQ)

Failed messages are automatically moved to a dead letter queue for later inspection and reprocessing.

**Features:**
- Configurable maximum size
- Topic-based message organization
- Statistics and monitoring
- Message replay capabilities

**Usage:**
```python
from src.data_generation import ReliableKafkaProducer

producer = ReliableKafkaProducer(dlq_max_size=10000)

# Get DLQ messages for a specific topic
dlq_messages = producer.get_dlq_messages("transactions")

# Clear DLQ for a topic
cleared_count = producer.clear_dlq("transactions")

# Replay failed messages
replayed_count = producer.replay_dlq_messages("transactions", max_messages=100)
```

### 2. Retry Mechanisms with Exponential Backoff

Automatic retry of failed messages with configurable exponential backoff.

**Configuration:**
```python
from src.data_generation import RetryConfig, ReliableKafkaProducer

retry_config = RetryConfig(
    max_retries=3,
    initial_delay_ms=100,
    max_delay_ms=30000,
    backoff_multiplier=2.0,
    jitter=True,
    retriable_errors=['KafkaTimeoutError', 'NotLeaderForPartitionError']
)

producer = ReliableKafkaProducer(retry_config=retry_config)
```

**Retry Logic:**
- Only retriable errors are retried
- Exponential backoff with optional jitter
- Maximum retry limit prevents infinite loops
- Failed retries are moved to DLQ

### 3. Message Deduplication

Prevents duplicate message processing using Redis or memory-based caching.

**Features:**
- Configurable TTL for deduplication cache
- Redis backend for distributed systems
- Memory fallback for single-instance deployments
- Content-based checksum calculation

**Usage:**
```python
import redis
from src.data_generation import ReliableKafkaProducer

# With Redis backend
redis_client = redis.Redis(host='localhost', port=6379, db=0)
producer = ReliableKafkaProducer(
    redis_client=redis_client,
    enable_deduplication=True
)

# With memory backend only
producer = ReliableKafkaProducer(enable_deduplication=True)
```

### 4. Health Monitoring and Alerting

Continuous health monitoring with configurable alerting.

**Monitored Metrics:**
- Error rates
- Message latency
- DLQ size
- Memory usage
- Connection status

**Alerting:**
```python
def custom_alert_handler(alert):
    print(f"Alert: {alert['type']} - {alert['value']} exceeds {alert['threshold']}")

producer = ReliableKafkaProducer()
producer.add_health_alert_callback(custom_alert_handler)

# Get health status
health = producer.get_health_status()
print(f"Status: {health['status']}")
```

## Usage Examples

### Basic Reliable Producer

```python
from src.data_generation import ReliableKafkaProducer

# Create producer with default settings
producer = ReliableKafkaProducer(
    bootstrap_servers=['localhost:9092'],
    enable_deduplication=True
)

# Send a message
message_id = producer.send_message(
    topic="transactions",
    value={"user_id": "123", "amount": 99.99},
    key="123"
)

# Check message status
status = producer.get_message_status(message_id)
print(f"Message status: {status}")

# Get comprehensive statistics
stats = producer.get_stats()
print(f"Error rate: {stats['error_rate']:.2%}")
print(f"Average latency: {stats['latency_stats']['avg_ms']:.2f}ms")
```

### Batch Operations

```python
# Send batch of messages
messages = [
    {"user_id": "123", "amount": 99.99},
    {"user_id": "456", "amount": 149.99},
    {"user_id": "789", "amount": 199.99}
]

results = producer.send_batch(
    topic="transactions",
    messages=messages,
    key_extractor=lambda x: x['user_id']
)

print(f"Sent: {results['sent']}/{results['total']} messages")
```

### Integration with Data Generation Orchestrator

```python
from src.data_generation import DataGenerationOrchestrator
import redis

# Use reliable producer in orchestrator
redis_client = redis.Redis(host='localhost', port=6379, db=0)
orchestrator = DataGenerationOrchestrator(
    use_reliable_producer=True,
    redis_client=redis_client
)

# Generate and stream data with reliability features
orchestrator.run_scenario("normal_traffic", duration_hours=1.0)
```

## Configuration Options

### Producer Configuration

```python
producer_config = {
    'bootstrap_servers': ['localhost:9092'],
    'acks': 'all',                    # Wait for all replicas
    'retries': 0,                     # Handled by our retry manager
    'enable_idempotence': True,       # Prevent broker-level duplicates
    'max_in_flight_requests_per_connection': 1,  # Ensure ordering
    'compression_type': 'lz4',
    'batch_size': 16384,
    'linger_ms': 10,
    'buffer_memory': 67108864,        # 64MB
    'request_timeout_ms': 30000,
    'delivery_timeout_ms': 120000
}
```

### Retry Configuration

```python
retry_config = RetryConfig(
    max_retries=3,                    # Maximum retry attempts
    initial_delay_ms=100,             # Initial delay
    max_delay_ms=30000,               # Maximum delay (30 seconds)
    backoff_multiplier=2.0,           # Exponential backoff factor
    jitter=True,                      # Add randomness to prevent thundering herd
    retriable_errors=[                # Which errors should be retried
        'KafkaTimeoutError',
        'NotLeaderForPartitionError',
        'RequestTimedOutError',
        'BrokerNotAvailableError'
    ]
)
```

### Health Monitor Configuration

```python
health_thresholds = {
    'error_rate_threshold': 0.05,     # 5% error rate
    'latency_threshold_ms': 1000,     # 1 second latency
    'dlq_size_threshold': 100,        # 100 messages in DLQ
    'memory_usage_threshold': 0.8     # 80% memory usage
}
```

## Monitoring and Observability

### Statistics

The reliable producer provides comprehensive statistics:

```python
stats = producer.get_stats()

# Basic metrics
print(f"Messages sent: {stats['messages_sent']}")
print(f"Messages failed: {stats['messages_failed']}")
print(f"Error rate: {stats['error_rate']:.2%}")

# Latency metrics
latency = stats['latency_stats']
print(f"Average latency: {latency['avg_ms']:.2f}ms")
print(f"P95 latency: {latency['p95_ms']:.2f}ms")
print(f"P99 latency: {latency['p99_ms']:.2f}ms")

# DLQ metrics
dlq = stats['dlq_stats']
print(f"DLQ size: {dlq['total_messages']}")
print(f"DLQ by topic: {dlq['topic_breakdown']}")

# Retry metrics
retry = stats['retry_stats']
print(f"Total retries: {retry['total_retries']}")
print(f"Successful retries: {retry['successful_retries']}")

# Deduplication metrics (if enabled)
if 'deduplication_stats' in stats:
    dedup = stats['deduplication_stats']
    print(f"Duplicate rate: {dedup['duplicate_rate']:.2%}")
```

### Health Monitoring

```python
# Get current health status
health = producer.get_health_status()
print(f"Overall status: {health['status']}")  # healthy, degraded, unhealthy

# Check specific metrics
metrics = health['metrics']
print(f"Current error rate: {metrics['avg_error_rate']:.4f}")
print(f"Current latency: {metrics['avg_latency_ms']:.2f}ms")

# View any issues
if health['issues']:
    for issue in health['issues']:
        print(f"Issue: {issue}")
```

## Error Handling Strategies

### Error Classification

1. **Retriable Errors**: Temporary issues that can be resolved with retry
   - `KafkaTimeoutError`: Network timeouts
   - `NotLeaderForPartitionError`: Leadership changes
   - `RequestTimedOutError`: Request timeouts

2. **Non-Retriable Errors**: Permanent issues that won't be resolved with retry
   - `ValueError`: Invalid data format
   - `SerializationError`: Data serialization failures
   - `TopicAuthorizationFailedError`: Permission issues

### Recovery Strategies

1. **Automatic Retry**: Temporary errors are automatically retried with exponential backoff
2. **Dead Letter Queue**: Failed messages are stored for manual inspection and replay
3. **Circuit Breaker**: Health monitoring can trigger alerts for system-wide issues
4. **Manual Intervention**: DLQ messages can be replayed after fixing underlying issues

## Best Practices

### 1. Configuration

- Use Redis for deduplication in production environments
- Set appropriate retry limits to avoid overwhelming the system
- Configure health thresholds based on your SLA requirements
- Enable idempotence to prevent broker-level duplicates

### 2. Monitoring

- Monitor error rates and latency continuously
- Set up alerts for DLQ size growth
- Track retry success rates
- Monitor deduplication effectiveness

### 3. Error Handling

- Implement custom alert handlers for critical issues
- Regularly review DLQ messages for patterns
- Have runbooks for common failure scenarios
- Test failure scenarios in staging environments

### 4. Performance

- Tune batch sizes for optimal throughput
- Use appropriate compression for your data
- Monitor memory usage and buffer sizes
- Consider network latency in timeout settings

## Troubleshooting

### Common Issues

1. **High Error Rates**
   - Check Kafka broker health
   - Verify network connectivity
   - Review producer configuration

2. **Growing DLQ**
   - Investigate error patterns in DLQ messages
   - Check for data serialization issues
   - Verify topic permissions

3. **High Latency**
   - Check network performance
   - Review Kafka broker performance
   - Optimize producer configuration

4. **Memory Issues**
   - Reduce buffer memory size
   - Implement backpressure handling
   - Monitor JVM heap usage

### Debugging Tools

```python
# Get detailed error information
dlq_messages = producer.get_dlq_messages("transactions")
for msg in dlq_messages:
    print(f"Message {msg.id}: {msg.last_error}")
    print(f"Retry count: {msg.retry_count}")
    print(f"Original value: {msg.value}")

# Check pending messages
pending = producer.get_pending_messages()
print(f"Pending messages: {len(pending)}")

# Get retry queue status
retry_stats = producer.retry_manager.get_stats()
print(f"Pending retries: {retry_stats['pending_retries']}")
```

## Demo Script

A comprehensive demo script is available at `scripts/demo_reliability_features.py`:

```bash
# Run all demos
python scripts/demo_reliability_features.py --demo all

# Run specific demo
python scripts/demo_reliability_features.py --demo deduplication

# Use custom configuration
python scripts/demo_reliability_features.py \
    --kafka-servers broker1:9092,broker2:9092 \
    --redis-url redis://redis-server:6379/1
```

## Future Enhancements

Planned improvements for future versions:

1. **Metrics Export**: Integration with Prometheus/Grafana
2. **Circuit Breaker**: Automatic producer shutdown on severe errors
3. **Message Compression**: Intelligent compression based on content
4. **Distributed Tracing**: Integration with OpenTelemetry
5. **Schema Evolution**: Support for Avro/Protobuf schema evolution
6. **Rate Limiting**: Built-in rate limiting for burst protection

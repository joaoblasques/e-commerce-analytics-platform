# Kafka Structured Streaming Consumers

This document provides comprehensive information about the Kafka Structured Streaming consumers implemented for the E-Commerce Analytics Platform.

## Overview

The streaming consumers are built using Apache Spark Structured Streaming to provide reliable, fault-tolerant, and scalable real-time data processing from Kafka topics. The implementation includes comprehensive error handling, schema validation, checkpointing, and backpressure management.

## Architecture

### Core Components

1. **BaseStreamingConsumer**: Abstract base class providing common streaming functionality
2. **TransactionStreamConsumer**: Consumer for e-commerce transaction data
3. **UserBehaviorStreamConsumer**: Consumer for user behavior event data
4. **StreamingConsumerManager**: Orchestrates multiple consumers with health monitoring
5. **SchemaValidator**: Validates incoming data schemas
6. **DataQualityChecker**: Ensures data quality and freshness
7. **ConsumerHealthMonitor**: Monitors consumer health and performance

### Data Flow

```
[Kafka Topics] → [Structured Streaming] → [Schema Validation] → [Data Quality Checks] → [Transformations] → [Output Sinks]
                                      ↓
                              [Error Handling & Recovery]
```

## Features

### 1. Robust Stream Processing

- **Structured Streaming**: Uses Spark Structured Streaming for exactly-once processing guarantees
- **Backpressure Management**: Adaptive query execution with configurable rate limiting
- **Checkpointing**: Fault-tolerant state management with configurable checkpoint locations
- **Error Recovery**: Graceful error handling with retry mechanisms

### 2. Schema Validation

- **Automatic Schema Validation**: Validates incoming Kafka messages against expected schemas
- **Schema Evolution Support**: Handles schema changes gracefully
- **Type Safety**: Ensures data type consistency throughout the pipeline
- **Validation Reporting**: Detailed schema mismatch reporting

### 3. Data Quality Assurance

- **Null Value Detection**: Identifies null values in critical columns
- **Data Freshness Checks**: Ensures data is processed within acceptable time windows
- **Anomaly Detection**: Basic anomaly detection for data quality issues
- **Quality Metrics**: Comprehensive data quality reporting

### 4. Health Monitoring

- **Real-time Health Checks**: Continuous monitoring of consumer health
- **Performance Metrics**: Tracks processing rates and latency
- **Alert System**: Configurable alerts for health issues
- **Recovery Automation**: Automatic recovery from transient failures

### 5. Consumer Management

- **Multi-Consumer Orchestration**: Manages multiple consumers concurrently
- **Lifecycle Management**: Start, stop, and restart consumers programmatically
- **Resource Management**: Efficient resource utilization with thread pooling
- **Coordination**: Proper shutdown coordination and cleanup

## Consumer Types

### Transaction Stream Consumer

Processes e-commerce transaction data from the `transactions` topic.

**Schema:**
```json
{
  "transaction_id": "string",
  "user_id": "string",
  "session_id": "string",
  "product_id": "string",
  "product_name": "string",
  "category_id": "string",
  "category_name": "string",
  "subcategory": "string",
  "brand": "string",
  "price": "double",
  "quantity": "integer",
  "discount_applied": "double",
  "payment_method": "string",
  "payment_provider": "string",
  "currency": "string",
  "timestamp": "timestamp",
  "user_location": {
    "country": "string",
    "state": "string",
    "city": "string",
    "zip_code": "string",
    "latitude": "double",
    "longitude": "double"
  },
  "device_info": {
    "type": "string",
    "os": "string",
    "browser": "string",
    "user_agent": "string"
  },
  "marketing_attribution": {
    "campaign_id": "string",
    "channel": "string",
    "referrer": "string"
  }
}
```

**Transformations:**
- Calculates total transaction amount
- Adds processing timestamp
- Extracts hour of day and day of week
- Filters out invalid transactions

### User Behavior Stream Consumer

Processes user behavior events from the `user-events` topic.

**Schema:**
```json
{
  "event_id": "string",
  "user_id": "string",
  "session_id": "string",
  "event_type": "string",
  "event_category": "string",
  "page_url": "string",
  "page_title": "string",
  "timestamp": "timestamp",
  "duration_ms": "integer",
  "event_properties": "map<string,string>",
  "user_location": {
    "country": "string",
    "state": "string",
    "city": "string",
    "ip_address": "string"
  },
  "device_info": {
    "type": "string",
    "os": "string",
    "browser": "string",
    "screen_resolution": "string",
    "viewport_size": "string"
  }
}
```

**Transformations:**
- Adds processing timestamp
- Extracts hour of day
- Identifies mobile vs desktop devices
- Categorizes page types (product, category, cart, etc.)
- Filters out invalid events

## Configuration

### Consumer Configuration

```python
consumer_config = {
    "kafka_bootstrap_servers": "localhost:9092",
    "topic": "transactions",
    "consumer_group": "transaction-analytics-group",
    "checkpoint_location": "/tmp/streaming_checkpoints",
    "max_offsets_per_trigger": 1000,
    "enable_backpressure": True,
}
```

### Spark Configuration

```python
spark_config = {
    "spark.sql.streaming.kafka.consumer.pollTimeoutMs": "512",
    "spark.sql.streaming.kafka.consumer.fetchOffset.numRetries": "3",
    "spark.sql.streaming.kafka.consumer.fetchOffset.retryIntervalMs": "64",
    "spark.sql.streaming.checkpointLocation": "/tmp/streaming_checkpoints",
    "spark.sql.streaming.metricsEnabled": "true",
    "spark.sql.streaming.ui.enabled": "true",
}
```

## Usage

### Basic Consumer Usage

```python
from pyspark.sql import SparkSession
from src.streaming.consumers import TransactionStreamConsumer

# Create Spark session
spark = SparkSession.builder \
    .appName("TransactionConsumer") \
    .getOrCreate()

# Create consumer
consumer = TransactionStreamConsumer(
    spark=spark,
    kafka_bootstrap_servers="localhost:9092",
    topic="transactions",
    consumer_group="analytics-group"
)

# Start streaming
query = consumer.start_stream(
    output_mode="append",
    trigger_interval="5 seconds",
    output_format="console"
)

# Wait for termination
query.awaitTermination()
```

### Using Consumer Manager

```python
from src.streaming.consumer_manager import StreamingConsumerManager, create_default_consumers

# Create manager
manager = StreamingConsumerManager(
    spark=spark,
    kafka_bootstrap_servers="localhost:9092"
)

# Set up default consumers
create_default_consumers(manager)

# Start all consumers
manager.start_all_consumers(
    output_mode="append",
    trigger_interval="5 seconds"
)

# Monitor and wait
manager.wait_for_all_consumers()
```

### CLI Usage

```bash
# Set up and run default consumers
python -m src.streaming.consumer_cli run --setup-defaults

# Register a custom consumer
python -m src.streaming.consumer_cli register-consumer \
    --name my_consumer \
    --topic my-topic \
    --consumer-group my-group \
    --consumer-type transaction

# Start specific consumer
python -m src.streaming.consumer_cli start-consumers --name my_consumer

# Monitor consumers
python -m src.streaming.consumer_cli monitor --interval 10

# Check status
python -m src.streaming.consumer_cli status --format json
```

## Error Handling

### Schema Validation Errors

When schema validation fails, the consumer:
1. Logs detailed schema mismatch information
2. Continues processing (configurable to fail fast)
3. Records schema errors in monitoring metrics
4. Triggers health status alerts

### Processing Errors

The consumers handle various error scenarios:
- **Kafka Connection Issues**: Automatic retry with exponential backoff
- **Serialization Errors**: Logs error and skips malformed messages
- **Transform Errors**: Captures and logs transformation failures
- **Sink Errors**: Retries output operations with backoff

### Recovery Strategies

1. **Automatic Retry**: Transient errors trigger automatic retry
2. **Checkpoint Recovery**: State recovery from checkpoints after failures
3. **Consumer Restart**: Health monitor can restart failed consumers
4. **Dead Letter Queue**: Failed messages can be sent to error topics

## Monitoring

### Health Metrics

The health monitor tracks:
- **Consumer Status**: Active, stopped, failed states
- **Processing Rates**: Input and processed rows per second
- **Latency**: End-to-end processing latency
- **Error Rates**: Frequency of processing errors
- **Resource Usage**: Memory and CPU utilization

### Available Metrics

```python
health_summary = {
    "total_consumers": 2,
    "healthy_consumers": 2,
    "unhealthy_consumers": 0,
    "overall_status": "healthy",
    "consumer_details": {
        "transaction_consumer": {
            "status": "healthy",
            "consecutive_failures": 0,
            "last_check": "2024-01-18T10:30:00Z",
            "metrics": {
                "input_rows_per_second": 150.5,
                "processed_rows_per_second": 148.2,
                "processing_ratio": 0.985
            }
        }
    }
}
```

### Alerting

Configure health callbacks for custom alerting:

```python
def health_alert_callback(consumer_name: str, health_data: Dict[str, Any]):
    if not health_data["is_healthy"]:
        # Send alert to monitoring system
        send_alert(f"Consumer {consumer_name} unhealthy: {health_data['errors']}")

monitor.add_health_callback(health_alert_callback)
```

## Performance Tuning

### Throughput Optimization

1. **Partition Alignment**: Align Kafka partitions with Spark parallelism
2. **Batch Size**: Tune `maxOffsetsPerTrigger` for optimal batch sizes
3. **Trigger Interval**: Balance latency vs throughput with trigger intervals
4. **Resource Allocation**: Adequate CPU and memory for Spark executors

### Memory Management

```python
spark_config = {
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.sql.streaming.stateStore.maintenanceInterval": "60s",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}
```

### Checkpoint Optimization

- Use fast storage (SSD) for checkpoint locations
- Configure appropriate checkpoint intervals
- Monitor checkpoint directory sizes
- Implement checkpoint cleanup strategies

## Troubleshooting

### Common Issues

**Consumer Not Starting**
- Check Kafka connectivity
- Verify topic exists and has correct permissions
- Ensure checkpoint directory is writable
- Review Spark logs for configuration errors

**Low Processing Throughput**
- Increase parallelism (`spark.default.parallelism`)
- Tune batch size (`maxOffsetsPerTrigger`)
- Check for data skew in partitions
- Monitor resource utilization

**High Memory Usage**
- Reduce state store size
- Optimize stateful operations
- Configure appropriate watermarks
- Monitor Spark UI for memory usage

**Schema Validation Failures**
- Check producer schema compatibility
- Review schema evolution settings
- Validate message serialization format
- Monitor for data corruption

### Debugging

Enable debug logging:

```python
# Python logging
import logging
logging.getLogger('src.streaming').setLevel(logging.DEBUG)

# Spark logging
spark.sparkContext.setLogLevel("DEBUG")
```

Monitor Spark Streaming UI:
- Access `http://spark-master:4040/streaming/`
- Review batch processing times
- Check input rates and processing rates
- Monitor micro-batch details

## Best Practices

### Development

1. **Test with Sample Data**: Use small datasets for development and testing
2. **Schema Management**: Version and manage schemas centrally
3. **Error Handling**: Implement comprehensive error handling
4. **Monitoring**: Add monitoring from the beginning
5. **Documentation**: Document transformations and business logic

### Production

1. **Resource Planning**: Plan compute and storage resources carefully
2. **Scaling Strategy**: Design for horizontal scaling
3. **Monitoring**: Implement comprehensive monitoring and alerting
4. **Backup Strategy**: Backup checkpoint directories regularly
5. **Version Management**: Use proper versioning for consumer deployments

### Security

1. **Authentication**: Use proper Kafka authentication (SASL, SSL)
2. **Authorization**: Implement topic-level authorization
3. **Encryption**: Enable encryption in transit and at rest
4. **Secrets Management**: Use secure secret management for credentials
5. **Network Security**: Secure network communication between services

## Integration with Other Components

### Data Lake Integration

```python
# Write to Delta Lake
def write_to_delta(df, batch_id):
    df.write \
        .format("delta") \
        .mode("append") \
        .option("path", "s3a://data-lake/transactions/") \
        .save()

query = consumer.start_stream(
    output_mode="append",
    foreach_batch_function=write_to_delta
)
```

### Analytics Integration

```python
# Real-time aggregations
def process_batch(df, batch_id):
    # Calculate real-time metrics
    metrics = df.groupBy("category_id") \
        .agg(
            count("*").alias("transaction_count"),
            sum("total_amount").alias("total_revenue")
        )

    # Write to metrics store
    metrics.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/analytics") \
        .option("dbtable", "real_time_metrics") \
        .mode("append") \
        .save()
```

### API Integration

The consumers can be integrated with the REST API for real-time data access:

```python
# Real-time query endpoint
@app.get("/api/v1/streaming/status")
def get_streaming_status():
    return manager.get_all_consumer_status()

@app.post("/api/v1/streaming/consumers/{name}/restart")
def restart_consumer(name: str):
    manager.stop_consumer(name)
    manager.start_consumer(name)
    return {"status": "restarted"}
```

This comprehensive streaming consumer implementation provides a robust foundation for real-time data processing in the e-commerce analytics platform, with enterprise-grade reliability, monitoring, and operational capabilities.

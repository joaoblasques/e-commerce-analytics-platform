# Kafka Setup and Management Guide
## E-Commerce Analytics Platform

### Overview

This guide covers the Kafka setup, topic configuration, and management for the E-Commerce Analytics Platform. The platform uses Apache Kafka as the central message streaming system for real-time data ingestion and processing.

### Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│   Kafka Topics  │───▶│  Spark Streaming│
│                 │    │                 │    │                 │
│ • Web Events    │    │ • transactions  │    │ • Real-time     │
│ • Transactions  │    │ • user-events   │    │   Processing    │
│ • Product Upd.  │    │ • product-upd.  │    │ • Analytics     │
│ • Fraud Alerts  │    │ • fraud-alerts  │    │ • Aggregations  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Topic Configuration

The platform uses 5 main Kafka topics, each optimized for specific use cases:

#### 1. `transactions` Topic
- **Purpose**: E-commerce transaction events with high throughput requirements
- **Partitions**: 6 (for parallel processing)
- **Replication Factor**: 1 (single-node development)
- **Retention**: 7 days
- **Compression**: LZ4 (fast compression/decompression)
- **Max Message Size**: 1MB

**Use Cases**:
- Real-time transaction processing
- Revenue analytics
- Fraud detection
- Customer behavior analysis

**Expected Volume**: 10,000-50,000 events/hour

#### 2. `user-events` Topic
- **Purpose**: User behavior events (page views, clicks, searches) with very high volume
- **Partitions**: 12 (highest throughput requirements)
- **Replication Factor**: 1
- **Retention**: 3 days (high volume, shorter retention)
- **Compression**: LZ4
- **Max Message Size**: 512KB

**Use Cases**:
- User behavior tracking
- Recommendation engines
- A/B testing
- Conversion funnel analysis

**Expected Volume**: 100,000-500,000 events/hour

#### 3. `product-updates` Topic
- **Purpose**: Product catalog updates with log compaction for latest state
- **Partitions**: 3 (moderate volume)
- **Replication Factor**: 1
- **Retention**: 30 days
- **Cleanup Policy**: Compaction (keeps latest state)
- **Compression**: GZIP (better compression for larger messages)
- **Max Message Size**: 2MB

**Use Cases**:
- Product catalog synchronization
- Price updates
- Inventory management
- Search index updates

**Expected Volume**: 1,000-10,000 events/hour

#### 4. `fraud-alerts` Topic
- **Purpose**: Fraud detection alerts requiring longer retention
- **Partitions**: 2 (lower volume, high importance)
- **Replication Factor**: 1
- **Retention**: 90 days (compliance and investigation)
- **Compression**: GZIP
- **Max Message Size**: 1MB

**Use Cases**:
- Fraud alert notifications
- Risk scoring results
- Investigation workflows
- Compliance reporting

**Expected Volume**: 100-1,000 events/hour

#### 5. `analytics-results` Topic
- **Purpose**: Processed analytics results from Spark jobs
- **Partitions**: 4 (moderate volume)
- **Replication Factor**: 1
- **Retention**: 14 days
- **Compression**: GZIP
- **Max Message Size**: 10MB (larger aggregated results)

**Use Cases**:
- Dashboard data feeds
- Report generation
- ML model results
- Business intelligence

**Expected Volume**: 500-5,000 events/hour

### Partitioning Strategy

The partitioning strategy is designed to optimize throughput and ensure even distribution:

1. **High-volume topics** (`user-events`): 12 partitions for maximum parallelism
2. **Medium-volume topics** (`transactions`, `analytics-results`): 4-6 partitions
3. **Low-volume topics** (`product-updates`, `fraud-alerts`): 2-3 partitions

**Partition Key Strategy**:
- `transactions`: `user_id` (ensures user transactions are processed in order)
- `user-events`: `session_id` (groups events by user session)
- `product-updates`: `product_id` (ensures product updates are processed in order)
- `fraud-alerts`: `user_id` (groups alerts by user)
- `analytics-results`: `metric_type` (groups by analytics type)

### Setup Instructions

#### Prerequisites

1. **Docker Environment**: Ensure Docker and Docker Compose are installed
2. **Python Dependencies**: Install required Python packages
   ```bash
   pip install kafka-python click
   ```

#### Starting the Environment

1. **Start Development Environment**:
   ```bash
   ./scripts/start-dev-env.sh
   ```

2. **Wait for Kafka to be Ready**:
   ```bash
   # Check if Kafka is responding
   docker exec ecap-kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

3. **Create Topics**:
   ```bash
   # Using the management script
   ./scripts/kafka-topics.sh create-topics
   
   # Or using Python directly
   python3 scripts/manage_kafka.py create-topics
   ```

#### Verification

1. **List Topics**:
   ```bash
   ./scripts/kafka-topics.sh list-topics
   ```

2. **Describe Specific Topic**:
   ```bash
   ./scripts/kafka-topics.sh describe-topic transactions
   ```

3. **Health Check**:
   ```bash
   ./scripts/kafka-topics.sh health-check
   ```

### Management Operations

#### Topic Management

```bash
# Create all topics
./scripts/kafka-topics.sh create-topics

# List all topics
./scripts/kafka-topics.sh list-topics

# Describe a specific topic
./scripts/kafka-topics.sh describe-topic transactions

# Delete a topic (be careful!)
./scripts/kafka-topics.sh delete-topic old-topic

# Reset all topics (DANGEROUS - deletes all data)
./scripts/kafka-topics.sh reset-topics
```

#### Testing and Monitoring

```bash
# Send test message
./scripts/kafka-topics.sh test-produce transactions '{"user_id": "test", "amount": 100}'

# Check consumer groups
./scripts/kafka-topics.sh consumer-groups

# Perform health check
./scripts/kafka-topics.sh health-check

# Monitor topic in real-time
./scripts/kafka-topics.sh kafka-cli kafka-console-consumer --topic transactions --from-beginning
```

#### Advanced Operations

```bash
# Direct Kafka CLI access
./scripts/kafka-topics.sh kafka-cli kafka-topics --list

# Change topic configuration
./scripts/kafka-topics.sh kafka-cli kafka-configs --entity-type topics --entity-name transactions --alter --add-config retention.ms=604800000

# Monitor consumer lag
./scripts/kafka-topics.sh kafka-cli kafka-consumer-groups --describe --group my-consumer-group
```

### Monitoring and Observability

#### Metrics Collection

The platform includes comprehensive monitoring through:

1. **JMX Metrics**: Kafka exposes JMX metrics for monitoring
2. **Prometheus Integration**: JMX metrics are exposed via Prometheus
3. **Grafana Dashboards**: Pre-built dashboards for visualization

#### Key Metrics to Monitor

1. **Topic Metrics**:
   - Messages per second
   - Bytes per second
   - Consumer lag
   - Partition distribution

2. **Broker Metrics**:
   - CPU utilization
   - Memory usage
   - Disk I/O
   - Network throughput

3. **Consumer Metrics**:
   - Consumer lag
   - Processing rate
   - Error rate
   - Rebalance frequency

#### Accessing Monitoring

1. **Grafana Dashboard**: http://localhost:3000
   - Username: admin
   - Password: admin
   - Navigate to "Kafka Overview" dashboard

2. **Prometheus Metrics**: http://localhost:9090
   - Query: `kafka_server_brokertopicmetrics_messagesinpersec`

3. **Kafka UI** (if enabled): http://localhost:8080

### Troubleshooting

#### Common Issues

1. **Topics Not Created**:
   ```bash
   # Check if Kafka is running
   docker ps | grep kafka
   
   # Check Kafka logs
   docker logs ecap-kafka
   
   # Verify connection
   ./scripts/kafka-topics.sh health-check
   ```

2. **Consumer Lag**:
   ```bash
   # Check consumer group status
   ./scripts/kafka-topics.sh consumer-groups
   
   # Describe specific consumer group
   ./scripts/kafka-topics.sh kafka-cli kafka-consumer-groups --describe --group my-group
   ```

3. **Disk Space Issues**:
   ```bash
   # Check topic retention settings
   ./scripts/kafka-topics.sh describe-topic transactions
   
   # Clean up old segments
   ./scripts/kafka-topics.sh kafka-cli kafka-log-dirs --describe --bootstrap-server localhost:9092
   ```

4. **Performance Issues**:
   ```bash
   # Check partition distribution
   ./scripts/kafka-topics.sh describe-topic user-events
   
   # Monitor broker metrics
   curl http://localhost:9090/metrics | grep kafka
   ```

#### Log Locations

- **Kafka Logs**: `docker logs ecap-kafka`
- **Management Script Logs**: `./kafka_management.log`
- **Docker Compose Logs**: `docker-compose logs kafka`

### Best Practices

#### Production Considerations

1. **Replication**: Increase replication factor to 3 for production
2. **Monitoring**: Set up alerts for consumer lag and broker health
3. **Backup**: Implement regular backup strategies
4. **Security**: Enable SSL/TLS and authentication
5. **Capacity Planning**: Monitor disk usage and plan for growth

#### Development Guidelines

1. **Topic Naming**: Use descriptive names with environment prefixes
2. **Message Size**: Keep messages under 1MB for optimal performance
3. **Partitioning**: Choose partition keys that ensure even distribution
4. **Testing**: Always test with realistic data volumes
5. **Cleanup**: Regularly clean up test topics and consumer groups

#### Performance Optimization

1. **Batch Size**: Tune producer batch size for throughput
2. **Compression**: Use appropriate compression for your use case
3. **Partition Count**: Balance between parallelism and overhead
4. **Consumer Groups**: Use appropriate number of consumers per partition
5. **Retention**: Set appropriate retention policies to manage disk usage

### Integration with Spark

The Kafka topics are designed to work seamlessly with Spark Structured Streaming:

```python
# Example Spark streaming configuration
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()
```

### Schema Management

For production deployments, consider implementing schema registry:

1. **Confluent Schema Registry**: For Avro schema management
2. **JSON Schema Validation**: For JSON message validation
3. **Schema Evolution**: Plan for backward/forward compatibility

### Security Considerations

1. **Authentication**: Implement SASL/PLAIN or SASL/SCRAM
2. **Authorization**: Use Kafka ACLs for fine-grained access control
3. **Encryption**: Enable SSL/TLS for data in transit
4. **Network Security**: Implement proper firewall rules

### Disaster Recovery

1. **Backup Strategy**: Regular backups of Kafka logs and metadata
2. **Replication**: Cross-datacenter replication for critical topics
3. **Recovery Procedures**: Documented recovery procedures
4. **Testing**: Regular disaster recovery testing

This comprehensive setup provides a robust foundation for the e-commerce analytics platform's real-time data processing needs.
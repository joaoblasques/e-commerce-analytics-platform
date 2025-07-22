# User Behavior Event Producer

The User Behavior Event Producer is a sophisticated data generator that simulates realistic website user interactions for e-commerce analytics. It generates session-based events that follow realistic user journey patterns.

## Features

### 🎯 Core Capabilities
- **Session-based Event Correlation**: Events are grouped into realistic user sessions
- **User Journey Patterns**: Intelligent event transitions based on user behavior
- **Device and Location Simulation**: Realistic device fingerprinting and geo-location
- **Time-based Patterns**: Activity varies by business hours, peak times, and weekends
- **User Segmentation**: Different user types with varying behavior patterns

### 📊 Event Types
The producer generates 14 different event types with realistic frequency distributions:

| Event Type | Weight | Description |
|------------|--------|-------------|
| `page_view` | 40% | General page views |
| `product_view` | 25% | Product detail page views |
| `search` | 10% | Search queries |
| `add_to_cart` | 8% | Add product to cart |
| `begin_checkout` | 5% | Start checkout process |
| `login` | 4% | User login |
| `wishlist_add` | 3% | Add to wishlist |
| `complete_checkout` | 2% | Complete purchase |
| `remove_from_cart` | 2% | Remove from cart |
| `newsletter_signup` | 1.5% | Newsletter subscription |
| `account_create` | 1.5% | Account creation |
| `logout` | 1% | User logout |
| `review_write` | 1% | Product review |
| `contact_support` | 1% | Support contact |

### 🎭 User Segments
Users are classified into segments with different behavior patterns:

| Segment | Weight | Session Length | Behavior |
|---------|--------|----------------|----------|
| `new_visitor` | 35% | 0.6x shorter | Exploratory, low conversion |
| `returning_visitor` | 40% | 1.0x normal | Familiar navigation |
| `loyal_customer` | 20% | 1.5x longer | Engaged, higher conversion |
| `power_user` | 5% | 2.0x longer | Expert users, high engagement |

### 🕐 Time Patterns
Activity varies throughout the day and week:

- **Business Hours (9-17)**: 1.2x multiplier
- **Peak Hours (19-22)**: 2.0x multiplier (evening shopping)
- **Weekends**: 1.8x multiplier
- **Late Night (23-6)**: 0.2x multiplier

### 🎯 User Journey Intelligence
The producer uses transition probabilities to create realistic user journeys:

```
page_view → product_view (25%) → add_to_cart (20%) → begin_checkout (25%) → complete_checkout (60%)
```

### 📱 Device Simulation
Realistic device distribution:
- **Mobile**: 65% (35% iOS, 30% Android)
- **Desktop**: 30% (20% Windows, 10% macOS)
- **Tablet**: 5% (iPadOS)

### 🌍 Geographic Distribution
Events include realistic geographic data:
- US (San Francisco, New York, Austin)
- Canada (Toronto)
- UK (London)
- Germany (Berlin)
- Australia (Sydney)

## Usage

### Command Line Interface

```bash
# Basic usage
python -m src.data_ingestion.producers.user_behavior_cli

# Custom configuration
python -m src.data_ingestion.producers.user_behavior_cli \
    --bootstrap-servers localhost:9092 \
    --topic user-events \
    --rate 10000 \
    --duration 300
```

### Shell Script

```bash
# Run with default settings
./scripts/run_user_behavior_producer.sh

# Custom configuration
./scripts/run_user_behavior_producer.sh \
    --rate 10000 \
    --duration 300 \
    --min-session 30 \
    --max-session 900
```

### Python API

```python
from src.data_ingestion.producers.user_behavior_producer import UserBehaviorProducer

# Create producer
producer = UserBehaviorProducer(
    bootstrap_servers="localhost:9092",
    topic="user-events",
    generation_rate=5000.0,
    session_duration_range=(60, 1800)
)

# Generate single event
event = producer.generate_message()
print(event)

# Run continuously
producer.run_continuous(duration_seconds=300)
```

## Configuration Options

### Producer Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `bootstrap_servers` | str | `localhost:9092` | Kafka bootstrap servers |
| `topic` | str | `user-events` | Kafka topic name |
| `generation_rate` | float | `5000.0` | Events per hour |
| `session_duration_range` | tuple | `(60, 1800)` | Min/max session duration (seconds) |

### CLI Arguments

```bash
--bootstrap-servers SERVERS  # Kafka servers
--topic TOPIC                # Kafka topic
--rate RATE                  # Events per hour
--duration DURATION          # Runtime in seconds
--min-session-duration MIN   # Min session duration
--max-session-duration MAX   # Max session duration
--verbose                    # Enable verbose logging
--quiet                      # Suppress output
```

## Message Format

### Event Structure

```json
{
  "event_id": "evt_1703123456_123456",
  "event_type": "product_view",
  "timestamp": "2024-01-01T12:00:00Z",
  "session_id": "sess_1703123456_user_123456789",
  "user_id": "user_123456789",
  "user_segment": "returning_visitor",
  "page_info": {
    "page_url": "/product/prod_electronics_1234",
    "page_title": "MacBook Pro 14-inch - Product",
    "page_category": "product",
    "product_id": "prod_electronics_1234",
    "product_category": "electronics"
  },
  "event_data": {
    "product_id": "prod_electronics_1234",
    "product_category": "electronics",
    "quantity": 1,
    "price": 1899.99
  },
  "device_info": {
    "type": "mobile",
    "os": "iOS",
    "browser": "Safari",
    "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "screen_resolution": "375x667",
    "viewport_size": "375x667"
  },
  "location": {
    "country": "US",
    "state": "CA",
    "city": "San Francisco",
    "timezone": "America/Los_Angeles",
    "latitude": 37.7749,
    "longitude": -122.4194
  },
  "referrer": {
    "type": "organic",
    "source": "google.com",
    "utm_source": null,
    "utm_medium": null,
    "utm_campaign": null
  },
  "session_info": {
    "session_start": "2024-01-01T11:45:00Z",
    "events_in_session": 5,
    "session_duration_so_far": 900,
    "is_new_session": false
  },
  "technical_info": {
    "ip_address": "192.168.1.100",
    "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "page_load_time_ms": 1250,
    "time_on_page_ms": 45000
  }
}
```

### Event-Specific Data

Different event types include specific data:

#### Search Events
```json
"event_data": {
  "search_term": "wireless headphones",
  "search_results_count": 245,
  "search_duration_ms": 2500
}
```

#### Add to Cart Events
```json
"event_data": {
  "product_id": "prod_electronics_1234",
  "product_category": "electronics",
  "quantity": 2,
  "price": 299.99
}
```

#### Checkout Events
```json
"event_data": {
  "order_id": "ord_1703123456_1234",
  "order_value": 599.98,
  "payment_method": "credit_card"
}
```

## Performance Characteristics

### Generation Rates
- **Default**: 5,000 events/hour
- **Recommended Range**: 1,000 - 50,000 events/hour
- **Maximum Tested**: 100,000 events/hour

### Session Management
- **Active Sessions**: Automatically managed and cleaned up
- **Session Cleanup**: Every 5 minutes
- **Session Reuse**: 70% chance of reusing existing session

### Memory Usage
- **Baseline**: ~50 MB
- **Per 1,000 Active Sessions**: ~5 MB additional
- **Typical Usage**: 100-500 MB for normal workloads

## Integration with Analytics Platform

### Kafka Topic Configuration
The producer works with the `user-events` topic configured in the platform:
- **Partitions**: 12 (for high throughput)
- **Replication Factor**: 3
- **Retention**: 3 days
- **Compression**: LZ4

### Partitioning Strategy
Events are partitioned by `session_id` to ensure:
- All events from a session go to the same partition
- Consistent ordering within sessions
- Balanced load across partitions

### Downstream Processing
The events are consumed by:
1. **Real-time Analytics**: Spark Streaming for immediate insights
2. **Data Lake**: Batch processing for historical analysis
3. **Fraud Detection**: Real-time anomaly detection
4. **User Journey Analytics**: Session-based analysis

## Monitoring and Metrics

### Producer Metrics
- **Messages Sent**: Total successful events
- **Messages Failed**: Failed event count
- **Bytes Sent**: Total data volume
- **Success Rate**: Percentage of successful sends
- **Active Sessions**: Current session count

### Performance Monitoring
```python
# Get current metrics
metrics = producer.get_metrics()
print(f"Success Rate: {metrics['success_rate']:.2f}%")
print(f"Active Sessions: {len(producer.active_sessions)}")
```

### Logging Levels
- **DEBUG**: Detailed event information
- **INFO**: General operation status
- **WARNING**: Potential issues
- **ERROR**: Critical failures

## Testing

### Unit Tests
```bash
# Run all tests
pytest tests/test_user_behavior_producer.py

# Run specific test categories
pytest tests/test_user_behavior_producer.py::TestUserBehaviorProducer::test_session_correlation
```

### Integration Tests
```bash
# Test with real Kafka cluster
pytest tests/test_user_behavior_producer.py --integration
```

### Performance Tests
```bash
# Test high-throughput scenarios
pytest tests/test_user_behavior_producer.py --performance
```

## Troubleshooting

### Common Issues

#### Connection Errors
```
Error: Failed to connect to Kafka
Solution: Verify Kafka is running and bootstrap servers are correct
```

#### Memory Issues
```
Error: Out of memory with many active sessions
Solution: Increase session cleanup frequency or reduce session duration
```

#### Rate Limiting
```
Warning: Cannot achieve target rate
Solution: Increase Kafka producer batch size or reduce rate
```

### Debug Mode
```bash
# Enable debug logging
python -m src.data_ingestion.producers.user_behavior_cli --verbose

# Check active sessions
# Producer logs session count every 5 minutes
```

### Performance Tuning
```python
# Optimize producer configuration
producer_config = {
    "acks": "1",           # Balance between speed and durability
    "batch_size": 32768,   # Larger batches for higher throughput
    "linger_ms": 20,       # Wait time for batching
    "compression_type": "lz4",  # Fast compression
    "buffer_memory": 67108864,  # 64MB buffer
}
```

## Best Practices

### Production Deployment
1. **Monitor Memory**: Keep active sessions under 10,000
2. **Adjust Rate**: Start with 1,000-5,000 events/hour
3. **Use Compression**: Enable LZ4 compression for efficiency
4. **Monitor Metrics**: Track success rates and active sessions
5. **Graceful Shutdown**: Always handle SIGTERM properly

### Development
1. **Test Locally**: Use small rates for development
2. **Validate Messages**: Check JSON serialization
3. **Session Logic**: Verify session correlation
4. **Time Patterns**: Test different time multipliers

### Scaling
1. **Horizontal**: Run multiple producer instances
2. **Vertical**: Increase generation rate per instance
3. **Partition**: Ensure adequate Kafka partitions
4. **Buffer**: Increase Kafka producer buffer sizes

## Examples

### Basic Usage
```python
# Simple event generation
producer = UserBehaviorProducer()
for _ in range(100):
    event = producer.generate_message()
    print(f"Generated: {event['event_type']}")
```

### Session Analysis
```python
# Analyze session patterns
producer = UserBehaviorProducer()
sessions = {}

for _ in range(1000):
    event = producer.generate_message()
    session_id = event['session_id']
    if session_id not in sessions:
        sessions[session_id] = []
    sessions[session_id].append(event)

print(f"Generated {len(sessions)} sessions")
for session_id, events in sessions.items():
    print(f"Session {session_id}: {len(events)} events")
```

### Custom Journey Patterns
```python
# Modify journey transitions
producer = UserBehaviorProducer()
producer.journey_transitions["page_view"]["add_to_cart"] = 0.15  # Increase conversion
```

This documentation provides comprehensive guidance for using the User Behavior Event Producer effectively in the e-commerce analytics platform.

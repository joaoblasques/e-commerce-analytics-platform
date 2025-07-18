# Data Generation Framework
## E-Commerce Analytics Platform

### Overview

The data generation framework provides comprehensive capabilities for creating realistic e-commerce data for development, testing, and demonstration purposes. The framework generates temporally accurate, geographically distributed, and behaviorally realistic data that mimics real-world e-commerce patterns.

### Architecture

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Configuration     │    │   Pattern Engine    │    │   Data Generator    │
│                     │    │                     │    │                     │
│ • User Demographics │───▶│ • Temporal Patterns │───▶│ • Transactions      │
│ • Product Catalog   │    │ • Geographic Dist.  │    │ • User Events       │
│ • Business Rules    │    │ • Fraud Injection   │    │ • Product Updates   │
│ • Seasonal Events   │    │ • Seasonal Trends   │    │ • Fraud Alerts      │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
                                      │
                                      ▼
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Kafka Producer    │    │   CLI Interface     │    │   Test Framework    │
│                     │    │                     │    │                     │
│ • Real-time Stream  │    │ • Batch Generation  │    │ • Unit Tests        │
│ • Topic Management  │    │ • Scenario Modes    │    │ • Integration Tests │
│ • Rate Control      │    │ • Health Checks     │    │ • Validation        │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

### Core Components

#### 1. Data Generation Configuration (`DataGenerationConfig`)

Comprehensive configuration system that controls all aspects of data generation:

```python
from src.data_generation import DataGenerationConfig

config = DataGenerationConfig(
    total_users=10000,
    total_products=1000,
    fraud_rate=0.001,
    base_transaction_rate=10000,  # per hour
    base_event_rate=100000        # per hour
)
```

**Key Configuration Areas**:
- **User Demographics**: Age groups, geographic distribution, user types
- **Product Catalog**: Categories, pricing ranges, seasonal items
- **Transaction Patterns**: Payment methods, business hours, peak times
- **Fraud Patterns**: Anomaly types, injection rates, detection scenarios
- **Seasonal Events**: Holiday spikes, sales periods, trending categories

#### 2. Pattern Engines

**Temporal Patterns** (`TemporalPatterns`):
- Business hours and weekend variations
- Holiday and seasonal spikes
- Realistic session durations
- Time-zone aware generation

**Geographic Patterns** (`GeographicPatterns`):
- Regional distribution (North America, Europe, Asia, Others)
- City-level location simulation
- Distance calculations for anomaly detection
- Timezone-aware timestamps

**Fraud Patterns** (`FraudPatterns`):
- Velocity attacks (multiple transactions)
- Location anomalies (unusual geography)
- Amount anomalies (unusual transaction sizes)
- Time anomalies (off-hours transactions)
- Device anomalies (new/suspicious devices)
- Pattern anomalies (unusual purchase categories)

**Seasonal Patterns** (`SeasonalPatterns`):
- Holiday-driven traffic spikes
- Category-specific seasonal trends
- Back-to-school, Black Friday, Christmas patterns
- Weather and calendar-based variations

#### 3. Core Data Generator (`ECommerceDataGenerator`)

The main data generation engine that orchestrates all patterns:

```python
from src.data_generation import ECommerceDataGenerator

generator = ECommerceDataGenerator(config)

# Generate realistic data
transactions = generator.generate_transactions(1000)
user_events = generator.generate_user_events(5000)
product_updates = generator.generate_product_updates(100)
```

**Generated Data Types**:

**Transactions**:
```json
{
  "transaction_id": "txn_001234567",
  "user_id": "user_000123",
  "timestamp": "2024-07-18T14:30:45.123Z",
  "products": [
    {
      "product_id": "prod_001234",
      "name": "Premium Professional Monitor",
      "category": "Electronics",
      "price": 299.99,
      "quantity": 1
    }
  ],
  "total_amount": 323.99,
  "payment_method": "credit_card",
  "location": {
    "country": "US",
    "state": "CA",
    "city": "San Francisco",
    "latitude": 37.7749,
    "longitude": -122.4194
  },
  "device_info": {
    "device_type": "desktop",
    "os": "macOS",
    "browser": "Chrome"
  },
  "is_fraud": false,
  "fraud_score": 0.12
}
```

**User Events**:
```json
{
  "event_id": "evt_987654321",
  "user_id": "user_000123",
  "session_id": "sess_abc123def456",
  "event_type": "product_view",
  "timestamp": "2024-07-18T14:28:15.456Z",
  "product_id": "prod_001234",
  "category": "Electronics",
  "page_url": "/products/electronics",
  "duration_seconds": 45,
  "device_info": {
    "device_type": "desktop",
    "screen_resolution": "1920x1080"
  }
}
```

#### 4. Kafka Integration (`KafkaDataProducer`)

Real-time streaming capabilities for continuous data generation:

```python
from src.data_generation import KafkaDataProducer

producer = KafkaDataProducer(generator)

# Start streaming to all topics
producer.start_streaming(
    topics=['transactions', 'user-events', 'product-updates'],
    duration_hours=2.0,
    rate_multiplier=1.5
)

# Send batch data
transactions = generator.generate_transactions(100)
producer.send_batch('transactions', transactions)
```

**Topic Configuration**:
- **transactions**: 10,000 events/hour, user_id partitioning
- **user-events**: 100,000 events/hour, session_id partitioning  
- **product-updates**: 1,000 events/hour, product_id partitioning
- **fraud-alerts**: 100 events/hour, transaction_id partitioning
- **analytics-results**: 500 events/hour, metric_type partitioning

#### 5. Orchestration (`DataGenerationOrchestrator`)

High-level orchestration for complex scenarios:

```python
from src.data_generation import DataGenerationOrchestrator

orchestrator = DataGenerationOrchestrator(config)

# Run predefined scenarios
orchestrator.run_scenario('peak_traffic', duration_hours=1.0)
orchestrator.run_scenario('fraud_testing', duration_hours=0.5)

# Generate historical data
results = orchestrator.generate_historical_data(days=30)
```

**Predefined Scenarios**:
- **normal_traffic**: Standard business operations
- **peak_traffic**: High-volume periods (3x rate multiplier)
- **fraud_testing**: Enhanced fraud patterns (10% fraud rate)
- **maintenance_mode**: Reduced traffic (0.1x rate multiplier)

### Usage Examples

#### Command Line Interface

The framework includes a comprehensive CLI for easy data generation:

```bash
# Generate data locally
python scripts/generate_stream_data.py generate --type transactions --count 1000

# Stream data to Kafka
python scripts/generate_stream_data.py stream --scenario peak_traffic --duration 2.0

# Generate historical data
python scripts/generate_stream_data.py historical --days 30

# Health check
python scripts/generate_stream_data.py health

# Show statistics
python scripts/generate_stream_data.py stats
```

#### Programmatic Usage

```python
from src.data_generation import (
    ECommerceDataGenerator,
    DataGenerationConfig,
    KafkaDataProducer
)

# 1. Configure generation
config = DataGenerationConfig()
config.total_users = 5000
config.fraud_rate = 0.005  # 0.5% fraud rate

# 2. Initialize generator
generator = ECommerceDataGenerator(config)

# 3. Generate data
transactions = generator.generate_transactions(
    count=1000,
    start_time=datetime.now() - timedelta(days=7),
    end_time=datetime.now()
)

# 4. Stream to Kafka
producer = KafkaDataProducer(generator)
producer.send_batch('transactions', transactions)

# 5. Get statistics
stats = generator.get_statistics()
print(f"Generated {stats['total_users']} users and {stats['total_products']} products")
```

#### Custom Configuration

```python
# Custom configuration for specific scenarios
config = DataGenerationConfig()

# Black Friday scenario
config.seasonal_events['Black Friday']['multiplier'] = 10.0
config.fraud_rate = 0.01  # Higher fraud during sales

# Geographic focus
config.regions = {
    'North America': {'weight': 0.8, 'time_zone_offset': -5},
    'Europe': {'weight': 0.2, 'time_zone_offset': 1}
}

# Custom categories
config.categories = ['Electronics', 'Fashion', 'Home']
config.price_ranges = {
    'Electronics': (100.0, 2000.0),
    'Fashion': (20.0, 500.0),
    'Home': (30.0, 800.0)
}
```

### Data Realism Features

#### Temporal Realism
- **Business Hours**: Higher activity during 9 AM - 9 PM
- **Peak Hours**: Traffic spikes at 10 AM, 2 PM, 7-9 PM
- **Weekend Patterns**: 50% increase in weekend activity
- **Holiday Spikes**: 4-8x multipliers during major holidays
- **Seasonal Trends**: Back-to-school, holiday shopping patterns

#### Geographic Realism
- **Regional Distribution**: 45% North America, 30% Europe, 20% Asia, 5% Others
- **City-Level Accuracy**: Real coordinates for major cities
- **Time Zone Awareness**: Activity patterns follow local time zones
- **Distance Calculations**: Haversine formula for fraud detection

#### Behavioral Realism
- **User Types**: Regular (70%), VIP (5%), New (25%) with different patterns
- **Session Behavior**: Realistic session durations and event sequences
- **Purchase Patterns**: Category preferences based on demographics
- **Device Distribution**: 50% mobile, 35% desktop, 15% tablet

#### Fraud Realism
- **Velocity Attacks**: 5-15 transactions within 5 minutes
- **Geographic Anomalies**: Transactions from different continents
- **Amount Anomalies**: 5-20x normal amounts or micro-transactions
- **Time Anomalies**: Transactions at 2-4 AM local time
- **Device Anomalies**: Unknown or suspicious device signatures

### Performance and Scalability

#### Generation Performance
- **Transactions**: ~1,000 records/second
- **User Events**: ~5,000 records/second
- **Memory Usage**: ~100MB for 10K users, 1K products
- **Streaming Rate**: Up to 50,000 events/hour per topic

#### Optimization Features
- **Batch Generation**: Generate large datasets efficiently
- **Lazy Loading**: Generate data on-demand
- **Memory Management**: Efficient data structures
- **Parallel Processing**: Multi-threaded streaming

#### Scalability Considerations
- **Horizontal Scaling**: Multiple generator instances
- **Kafka Partitioning**: Optimal partition distribution
- **Rate Control**: Configurable generation rates
- **Resource Management**: Memory and CPU optimization

### Testing and Validation

#### Unit Testing
```bash
# Run data generation tests
python -m pytest tests/test_data_generation.py -v

# Test specific components
python -m pytest tests/test_data_generation.py::TestECommerceDataGenerator -v
```

#### Integration Testing
```bash
# Test with Kafka integration
python -m pytest tests/test_data_generation.py::TestDataGenerationIntegration -v
```

#### Data Quality Validation
- **Schema Validation**: Ensure all required fields present
- **Referential Integrity**: Valid user/product references
- **Temporal Consistency**: Chronological event ordering
- **Geographic Accuracy**: Valid coordinates and locations
- **Business Logic**: Realistic patterns and relationships

### Configuration Reference

#### Core Settings
```python
class DataGenerationConfig:
    # User base
    total_users: int = 10000
    active_user_ratio: float = 0.7
    vip_user_ratio: float = 0.05
    
    # Product catalog
    total_products: int = 1000
    categories: List[str] = [...]
    
    # Transaction patterns
    base_transaction_rate: int = 10000  # per hour
    peak_hour_multiplier: float = 3.0
    weekend_multiplier: float = 1.5
    
    # Fraud settings
    fraud_rate: float = 0.001
    fraud_patterns: Dict[str, float] = {...}
    
    # Geographic distribution
    regions: Dict[str, Dict[str, float]] = {...}
```

#### Seasonal Events
```python
seasonal_events = {
    'Black Friday': {
        'date': '2024-11-29',
        'multiplier': 8.0,
        'duration_hours': 24,
        'categories': ['Electronics', 'Clothing']
    },
    'Christmas': {
        'date': '2024-12-25',
        'multiplier': 6.0,
        'duration_hours': 72,
        'categories': ['Toys & Games', 'Electronics']
    }
}
```

### Troubleshooting

#### Common Issues

**Low Data Quality**:
```bash
# Check configuration validation
python scripts/generate_stream_data.py config

# Verify data statistics
python scripts/generate_stream_data.py stats
```

**Kafka Connection Issues**:
```bash
# Test Kafka connectivity
python scripts/generate_stream_data.py health

# Check topic availability
./scripts/kafka-topics.sh list-topics
```

**Performance Issues**:
```python
# Reduce batch sizes
config.total_users = 1000  # Instead of 10000
config.total_products = 100  # Instead of 1000

# Use streaming mode for large datasets
orchestrator.run_scenario('normal_traffic', duration_hours=0.1)
```

**Memory Issues**:
```python
# Generate data in batches
for i in range(10):
    transactions = generator.generate_transactions(100)
    producer.send_batch('transactions', transactions)
```

#### Debug Mode
```bash
# Enable verbose logging
python scripts/generate_stream_data.py --verbose generate --type transactions --count 10

# Check generator statistics
python scripts/generate_stream_data.py stats
```

### Best Practices

#### Configuration Management
1. **Use Configuration Files**: Store settings in JSON/YAML
2. **Environment-Specific Configs**: Different settings per environment
3. **Validation**: Always validate configuration before use
4. **Documentation**: Document custom configuration changes

#### Data Generation
1. **Start Small**: Begin with small datasets for testing
2. **Incremental Scaling**: Gradually increase data volume
3. **Monitor Resources**: Watch memory and CPU usage
4. **Batch Processing**: Use batches for large datasets

#### Streaming
1. **Rate Control**: Monitor and adjust streaming rates
2. **Topic Management**: Ensure topics exist before streaming
3. **Error Handling**: Implement proper error recovery
4. **Health Monitoring**: Regular health checks

#### Testing
1. **Unit Tests**: Test individual components
2. **Integration Tests**: Test with real Kafka
3. **Data Validation**: Verify generated data quality
4. **Performance Tests**: Measure generation rates

### Integration with Analytics Platform

The data generation framework is tightly integrated with the e-commerce analytics platform:

#### Spark Integration
- Generated data matches Spark DataFrame schemas
- Optimized for Spark Structured Streaming
- Compatible with Delta Lake format
- Supports schema evolution

#### Kafka Integration
- Pre-configured topic settings
- Optimal partitioning strategies
- Compression and serialization
- Monitoring and health checks

#### Dashboard Integration
- Real-time data feeds for dashboards
- Historical data for trend analysis
- Configurable data refresh rates
- Quality metrics and monitoring

This comprehensive data generation framework provides the foundation for realistic e-commerce analytics development, testing, and demonstration scenarios.
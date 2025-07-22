# Data Lake Storage Layer

The Data Lake Storage Layer provides comprehensive data lake functionality for the E-Commerce Analytics Platform, including optimal partitioning strategies, automated ingestion, compaction, and metadata management.

## Overview

The data lake architecture is designed to efficiently store and manage large volumes of e-commerce data in Parquet format with optimal partitioning strategies for different data access patterns.

### Key Components

1. **DataLakeStorage** - Core storage functionality with partitioning strategies
2. **DataLakeIngester** - Automated ingestion from various sources
3. **DataCompactor** - File optimization and compaction operations
4. **MetadataManager** - Catalog and metadata management

## Architecture

```
Data Lake Architecture
├── Storage Layer (Parquet files with optimal partitioning)
│   ├── Transactions (time-based partitioning)
│   ├── User Events (time + event type partitioning)
│   ├── Product Updates (category + time partitioning)
│   └── Analytics Results (result type + time partitioning)
├── Ingestion Layer (Batch and streaming ingestion)
│   ├── Kafka Integration
│   ├── Database Integration
│   └── File System Integration
├── Optimization Layer (Compaction and maintenance)
│   ├── File Size Optimization
│   ├── Partition Optimization
│   └── Storage Cost Optimization
└── Catalog Layer (Metadata management)
    ├── Schema Tracking
    ├── Statistics Management
    └── Lineage Tracking
```

## Partitioning Strategies

The system provides optimized partitioning strategies for different data types:

### Transaction Data
- **Partitions**: year/month/day
- **Sort Order**: transaction_id
- **Use Case**: Time-series analysis, daily reporting
- **Benefits**: Efficient time-range queries, optimal for daily batch processing

### User Events
- **Partitions**: year/month/day/event_type
- **Sort Order**: session_id, event_timestamp
- **Use Case**: User behavior analysis, funnel analysis
- **Benefits**: Event type filtering, session-based analysis

### Product Updates
- **Partitions**: category/year/month
- **Sort Order**: product_id
- **Use Case**: Product catalog management, category analysis
- **Benefits**: Category-based analysis, product lifecycle tracking

### Analytics Results
- **Partitions**: result_type/year/month
- **Sort Order**: computation_id
- **Use Case**: Analytics output storage, report generation
- **Benefits**: Result type organization, temporal analysis

### Customer Profiles
- **Partitions**: customer_segment/region
- **Bucketing**: 10 buckets by customer_id
- **Sort Order**: customer_id
- **Use Case**: Customer analytics, segmentation analysis
- **Benefits**: Segment-based queries, regional analysis

## Usage Examples

### Basic Storage Operations

```python
from src.data_lake import DataLakeStorage

# Initialize storage
storage = DataLakeStorage(base_path="s3a://data-lake")

# Write data with automatic partitioning
output_path = storage.write_partitioned_data(
    df=transaction_df,
    table_name="transactions",
    data_type="transactions",
    mode="append"
)

# Read data with partition filtering
df = storage.read_partitioned_data(
    table_name="transactions",
    partition_filters={"year": 2023, "month": [10, 11, 12]},
    columns=["transaction_id", "amount", "customer_id"]
)

# Get table information
info = storage.get_table_info("transactions")
print(f"Table has {info['record_count']} records")
```

### Data Ingestion

```python
from src.data_lake import DataLakeIngester

# Initialize ingester
ingester = DataLakeIngester(storage=storage)

# Batch ingestion from Kafka
result = ingester.ingest_from_kafka_batch(
    topic="transactions",
    table_name="transactions",
    data_type="transactions",
    batch_size=1000,
    max_batches=10
)

# Streaming ingestion from Kafka
stream_id = ingester.ingest_from_kafka_streaming(
    topic="user-events",
    table_name="user_events",
    data_type="user_events",
    trigger_interval="30 seconds"
)

# Database ingestion
result = ingester.ingest_from_database(
    connection_config={
        "url": "jdbc:postgresql://postgres:5432/ecommerce",
        "user": "user",
        "password": "password"
    },
    query="SELECT * FROM products WHERE updated_at > '2023-01-01'",
    table_name="products",
    data_type="product_updates"
)
```

### Data Compaction

```python
from src.data_lake import DataCompactor

# Initialize compactor
compactor = DataCompactor(storage=storage)

# Analyze table for optimization needs
analysis = compactor.analyze_table_files("transactions")
if analysis["needs_compaction"]:
    print(f"Compaction recommended: {analysis['compaction_reason']}")

# Compact a specific table
result = compactor.compact_table(
    table_name="transactions",
    target_file_size_mb=128,
    backup=True
)

# Optimize all tables that need it
results = compactor.optimize_all_tables(
    min_file_count=10,
    target_file_size_mb=128,
    parallel=True
)
```

### Metadata Management

```python
from src.data_lake import MetadataManager

# Initialize metadata manager
catalog = MetadataManager(catalog_path="s3a://data-lake/catalog")

# Register a new table
metadata = catalog.register_table(
    table_name="transactions",
    table_path="s3a://data-lake/transactions",
    description="E-commerce transaction data",
    owner="data-team",
    tags=["transactions", "ecommerce", "financial"]
)

# Update table statistics
metadata = catalog.update_table_statistics(
    table_name="transactions",
    compute_column_stats=True,
    sample_size=100000
)

# Search tables
results = catalog.search_tables(
    search_term="transaction",
    search_fields=["table_name", "description", "tags"]
)

# Add lineage information
catalog.add_table_lineage(
    table_name="customer_analytics",
    upstream_tables=["transactions", "user_events"],
    downstream_tables=["dashboard_metrics"]
)
```

## Command Line Interface

The data lake provides a comprehensive CLI for all operations:

### Storage Operations
```bash
# Write data to data lake
ecap data-lake storage write transactions \
  --data-type transactions \
  --source-path /data/transactions.parquet \
  --mode append

# Read data from data lake
ecap data-lake storage read transactions \
  --partition-filter year=2023 \
  --partition-filter month=12 \
  --columns transaction_id,amount \
  --limit 100

# Get table information
ecap data-lake storage info transactions

# List all tables
ecap data-lake storage list-tables
```

### Ingestion Operations
```bash
# Kafka batch ingestion
ecap data-lake ingestion kafka-batch transactions transactions \
  --data-type transactions \
  --batch-size 1000 \
  --max-batches 10

# Kafka streaming ingestion
ecap data-lake ingestion kafka-stream user-events user_events \
  --data-type user_events \
  --trigger-interval "30 seconds"

# Check streaming status
ecap data-lake ingestion stream-status
```

### Compaction Operations
```bash
# Analyze table for optimization
ecap data-lake compaction analyze transactions

# Compact a table
ecap data-lake compaction compact transactions \
  --target-file-size 128 \
  --backup

# Optimize all tables
ecap data-lake compaction optimize-all \
  --min-file-count 10 \
  --parallel
```

### Catalog Operations
```bash
# Register a table
ecap data-lake catalog register transactions \
  s3a://data-lake/transactions \
  --description "Transaction data" \
  --owner data-team \
  --tag transactions \
  --tag ecommerce

# Update table statistics
ecap data-lake catalog update-stats transactions \
  --compute-column-stats \
  --sample-size 100000

# List tables
ecap data-lake catalog list-tables \
  --database data_lake \
  --status active

# Search tables
ecap data-lake catalog search transaction \
  --field table_name \
  --field description
```

## Performance Optimization

### File Size Optimization
- **Target File Size**: 128-256MB per file
- **Small File Problem**: Automatically detected and resolved
- **Compaction Strategy**: Configurable target sizes and parallel processing

### Query Performance
- **Partition Pruning**: Efficient partition elimination
- **Columnar Storage**: Parquet format for analytical workloads
- **Predicate Pushdown**: Column and row-level filtering
- **Statistics**: Comprehensive column statistics for query optimization

### Storage Efficiency
- **Compression**: Snappy compression by default
- **Schema Evolution**: Support for schema changes over time
- **Data Deduplication**: Configurable deduplication strategies
- **Lifecycle Management**: Automated retention and archival

## Monitoring and Observability

### Storage Metrics
- File count and sizes per table
- Partition distribution
- Compression ratios
- Storage cost analysis

### Ingestion Metrics
- Records processed per second
- Error rates and retry statistics
- Streaming query health
- Backlog monitoring

### Query Metrics
- Query execution times
- Data scan volumes
- Partition pruning effectiveness
- Cache hit rates

## Security and Governance

### Access Control
- Table-level access permissions
- Column-level security (future enhancement)
- Audit logging for all operations
- Integration with enterprise identity systems

### Data Governance
- Comprehensive metadata catalog
- Data lineage tracking
- Schema versioning and evolution
- Data quality monitoring

### Compliance
- Data retention policies
- Geographic data residency
- Encryption at rest and in transit
- Audit trail maintenance

## Best Practices

### Partitioning
1. Choose partition columns with moderate cardinality (100-10,000 partitions)
2. Use time-based partitioning for time-series data
3. Consider query patterns when designing partition strategy
4. Avoid over-partitioning (too many small partitions)

### File Management
1. Target 128-256MB files for optimal performance
2. Regular compaction to prevent small file accumulation
3. Monitor and optimize partition skew
4. Use appropriate compression algorithms

### Schema Design
1. Use consistent naming conventions
2. Design for schema evolution
3. Include metadata columns (ingestion timestamp, source, etc.)
4. Consider nested structures for complex data

### Performance Tuning
1. Use partition pruning in queries
2. Implement proper indexing strategies
3. Monitor query execution plans
4. Optimize file layouts for access patterns

## Troubleshooting

### Common Issues

#### Small File Problem
**Symptoms**: Many small files, poor query performance
**Solution**: Run compaction jobs regularly
```bash
ecap data-lake compaction analyze table_name
ecap data-lake compaction compact table_name
```

#### Partition Skew
**Symptoms**: Uneven partition sizes, slow queries
**Solution**: Review partitioning strategy, consider bucketing
```bash
ecap data-lake storage info table_name
# Review partition distribution
```

#### Schema Evolution Issues
**Symptoms**: Schema mismatch errors during reads
**Solution**: Use schema evolution features
```python
# Enable schema merging
storage.read_partitioned_data(
    table_name="table_name",
    options={"mergeSchema": "true"}
)
```

#### Streaming Ingestion Failures
**Symptoms**: Streaming queries fail or lag behind
**Solution**: Monitor and restart streams
```bash
ecap data-lake ingestion stream-status
# Restart failed streams
```

### Performance Issues

#### Slow Queries
1. Check partition pruning effectiveness
2. Analyze file sizes and distribution
3. Review query execution plans
4. Consider data locality optimization

#### High Storage Costs
1. Run compaction jobs to reduce file count
2. Implement data lifecycle policies
3. Review compression settings
4. Archive old data to cheaper storage tiers

### Debugging

#### Enable Debug Logging
```python
import logging
logging.getLogger('src.data_lake').setLevel(logging.DEBUG)
```

#### Check Spark UI
- Monitor job execution in Spark UI
- Analyze task distribution and execution times
- Review storage I/O patterns

#### Validate Data Integrity
```bash
# Check table record counts
ecap data-lake catalog update-stats table_name

# Validate partition structure
ecap data-lake storage info table_name
```

## Future Enhancements

### Planned Features
1. **Delta Lake Integration** - ACID transactions and time travel
2. **Advanced Indexing** - Bloom filters and Z-ordering
3. **Data Quality Framework** - Automated data validation
4. **Cost Optimization** - Intelligent tiering and archival
5. **Machine Learning Integration** - Feature store capabilities

### Roadmap
- **Phase 1**: Core storage and ingestion ✅
- **Phase 2**: Advanced optimization and catalog
- **Phase 3**: Delta Lake and ACID transactions
- **Phase 4**: ML and advanced analytics integration
- **Phase 5**: Enterprise governance and security

This data lake implementation provides a solid foundation for scalable, efficient data storage and management in the e-commerce analytics platform.

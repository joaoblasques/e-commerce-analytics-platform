# Data Lifecycle Management

## Overview

The Data Lifecycle Management system provides comprehensive automated data retention policies, archiving strategies, data lineage tracking, and cost optimization for the e-commerce analytics platform. This system integrates seamlessly with Delta Lake to ensure ACID compliance while managing data throughout its entire lifecycle.

## Table of Contents

1. [Architecture](#architecture)
2. [Key Features](#key-features)
3. [Getting Started](#getting-started)
4. [Configuration](#configuration)
5. [Usage Examples](#usage-examples)
6. [CLI Reference](#cli-reference)
7. [API Reference](#api-reference)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                   Data Lifecycle Management                  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │   Retention     │  │    Archiving    │  │   Lineage   │  │
│  │   Policies      │  │   Strategies    │  │   Tracking  │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │ Cost Optimization│  │  Configuration  │  │ CLI/API     │  │
│  │                 │  │   Management    │  │ Interface   │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                      Delta Lake Manager                     │
├─────────────────────────────────────────────────────────────┤
│                        Delta Tables                         │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Raw Data → Hot Storage → Warm Storage → Cold Storage → Archive → Deletion
   ↓           ↓           ↓            ↓         ↓        ↓
  0-30d      30-90d     90-365d      1-3 years  3-7 years  >7 years
   ↓           ↓           ↓            ↓         ↓        ↓
 Standard   Standard   Infrequent    Archive   Deep Archive  N/A
  Tier       Tier       Access       Storage    Storage
```

## Key Features

### 1. Automated Retention Policies

- **Hot Data (0-30 days)**: Frequently accessed data stored in high-performance tier
- **Warm Data (30-90 days)**: Occasionally accessed data with moderate performance
- **Cold Data (90-365 days)**: Rarely accessed data with lower performance requirements
- **Archive Data (1+ years)**: Long-term storage with retrieval delays
- **Automated Deletion**: Configurable deletion policies for compliance

### 2. Intelligent Archiving Strategies

- **Format Optimization**: Automatic conversion to optimal storage formats (Parquet, ORC)
- **Compression**: Multiple compression algorithms (GZIP, Snappy, LZ4, ZSTD)
- **Partitioning**: Intelligent partitioning for cost-effective storage and retrieval
- **Tiered Storage**: Automatic migration between storage tiers based on access patterns

### 3. Comprehensive Lineage Tracking

- **Operation Tracking**: Track all CREATE, UPDATE, DELETE, MERGE, OPTIMIZE operations
- **Source Dependencies**: Automatic discovery of upstream data dependencies
- **Impact Analysis**: Understand downstream effects of data changes
- **Audit Trail**: Complete audit trail for compliance and debugging

### 4. Cost Optimization

- **Usage Analytics**: Analyze access patterns and storage costs
- **Tier Recommendations**: Automatic recommendations for optimal storage tiers
- **Cost Forecasting**: Predict monthly storage costs
- **Resource Optimization**: Optimize storage allocation and access patterns

## Getting Started

### Installation

The data lifecycle management system is included with the main analytics platform. No additional installation is required.

### Basic Setup

```python
from src.data_lake.delta import DeltaLakeManager
from src.data_lake.lifecycle_manager import DataLifecycleManager

# Initialize Delta Lake Manager
delta_manager = DeltaLakeManager(base_path="s3a://your-data-lake/delta")

# Initialize Lifecycle Manager
lifecycle_manager = DataLifecycleManager(delta_manager)

# Enable automatic lifecycle tracking
delta_manager.enable_lifecycle_tracking(lifecycle_manager)
```

### Quick Start Example

```python
from src.data_lake.lifecycle_config import LifecycleConfigManager

# Load default configurations
config_manager = LifecycleConfigManager()

# Apply configurations to lifecycle manager
for table_name, rule in config_manager.retention_rules.items():
    lifecycle_manager.add_retention_rule(rule)

for table_name, policy in config_manager.archive_policies.items():
    lifecycle_manager.add_archive_policy(policy)

# Apply retention policies (dry run first)
results = lifecycle_manager.apply_retention_policies(dry_run=True)
print("Retention Policy Results:", results)

# Generate comprehensive report
report = lifecycle_manager.generate_lifecycle_report()
print("Lifecycle Report:", report)
```

## Configuration

### Retention Rules

Configure data retention policies for each table:

```python
from src.data_lake.lifecycle_manager import RetentionRule

retention_rule = RetentionRule(
    table_name="transactions",
    hot_days=30,        # Keep data hot for 30 days
    warm_days=90,       # Move to warm storage after 90 days
    cold_days=365,      # Move to cold storage after 1 year
    archive_after_days=2190,  # Archive after 6 years
    delete_after_days=2555,   # Delete after 7 years
    partition_column="transaction_date",
    enabled=True
)

lifecycle_manager.add_retention_rule(retention_rule)
```

### Archive Policies

Configure archiving strategies:

```python
from src.data_lake.lifecycle_manager import ArchivePolicy

archive_policy = ArchivePolicy(
    table_name="transactions",
    archive_path="s3a://archive-bucket/transactions",
    compression="gzip",
    format="parquet",
    partition_by=["year", "month"],
    enabled=True
)

lifecycle_manager.add_archive_policy(archive_policy)
```

### Configuration Files

You can also use JSON configuration files:

```json
{
  "retention_rules": [
    {
      "table_name": "user_events",
      "hot_days": 7,
      "warm_days": 30,
      "cold_days": 90,
      "archive_after_days": 365,
      "delete_after_days": 1095,
      "partition_column": "event_timestamp",
      "enabled": true
    }
  ],
  "archive_policies": [
    {
      "table_name": "user_events",
      "archive_path": "s3a://archive/user_events",
      "compression": "gzip",
      "format": "parquet",
      "partition_by": ["event_date"],
      "enabled": true
    }
  ]
}
```

Load configuration:

```python
lifecycle_manager.load_configuration("lifecycle_config.json")
```

## Usage Examples

### 1. Creating Tables with Lifecycle Management

```python
# Create a new table with automatic lifecycle setup
df = spark.createDataFrame(data, schema)

# Create table with default lifecycle policies
success = delta_manager.create_delta_table_with_lifecycle(
    df=df,
    table_name="customer_events",
    partition_columns=["event_date"],
    enable_retention=True
)

# Create table with custom retention configuration
success = delta_manager.create_delta_table_with_lifecycle(
    df=df,
    table_name="sensitive_data",
    retention_config={
        "hot_days": 7,
        "warm_days": 30,
        "cold_days": 90,
        "delete_after_days": 365  # Delete after 1 year for compliance
    }
)
```

### 2. Writing Data with Lineage Tracking

```python
# Write data with automatic lineage tracking
transformed_df = source_df.transform(some_transformation)

delta_manager.write_to_delta_with_lifecycle(
    df=transformed_df,
    table_name="processed_events",
    mode="append",
    source_tables=["raw_events", "customer_profiles"]
)
```

### 3. Applying Retention Policies

```python
# Dry run to see what would happen
dry_run_results = lifecycle_manager.apply_retention_policies(dry_run=True)

for table_name, result in dry_run_results.items():
    print(f"Table: {table_name}")
    print(f"  Hot records: {result['hot_records']:,}")
    print(f"  Would archive: {result['archived_records']:,}")
    print(f"  Would delete: {result['deleted_records']:,}")

# Apply retention policies for real
actual_results = lifecycle_manager.apply_retention_policies(dry_run=False)
```

### 4. Lineage Analysis

```python
# Get lineage graph for a table
lineage = lifecycle_manager.get_lineage_graph("customer_analytics", depth=3)

print(f"Upstream dependencies: {lineage['upstream']}")
print(f"Downstream consumers: {lineage['downstream']}")
print(f"Recent operations: {len(lineage['operations'])}")

# Analyze impact of changes
for operation in lineage['operations'][-5:]:
    print(f"Operation: {operation['operation']} at {operation['timestamp']}")
    print(f"Records affected: {operation['record_count']:,}")
```

### 5. Cost Optimization

```python
# Analyze storage costs
storage_metrics = lifecycle_manager.optimize_storage_costs()

total_cost = sum(m.storage_cost_usd for m in storage_metrics.values())
total_size_gb = sum(m.size_bytes for m in storage_metrics.values()) / (1024**3)

print(f"Total monthly storage cost: ${total_cost:.2f}")
print(f"Total storage size: {total_size_gb:.2f} GB")

# Get optimization recommendations
report = lifecycle_manager.generate_lifecycle_report()
for recommendation in report['recommendations']:
    print(f"💡 {recommendation}")
```

### 6. Comprehensive Reporting

```python
# Generate full lifecycle report
report = lifecycle_manager.generate_lifecycle_report()

# Display summary
print(f"Report generated: {report['timestamp']}")
print(f"Tables managed: {report['storage_summary']['total_tables']}")
print(f"Total size: {report['storage_summary']['total_size_gb']:.2f} GB")
print(f"Monthly cost: ${report['storage_summary']['total_monthly_cost_usd']:.2f}")

# Save detailed report
with open('lifecycle_report.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
```

## CLI Reference

The lifecycle management system includes a comprehensive CLI for operations:

### Basic Commands

```bash
# Apply retention policies (dry run)
python -m src.data_lake.lifecycle_cli retention --dry-run

# Apply retention to specific tables
python -m src.data_lake.lifecycle_cli retention --tables transactions user_events

# Show lineage for a table
python -m src.data_lake.lifecycle_cli lineage --table transactions --depth 2

# Optimize storage costs
python -m src.data_lake.lifecycle_cli optimize --output storage_analysis.json

# Generate comprehensive report
python -m src.data_lake.lifecycle_cli report --output lifecycle_report.json

# Configure table policies
python -m src.data_lake.lifecycle_cli config --table transactions \
  --hot-days 30 --warm-days 90 --cold-days 365

# List all configured tables
python -m src.data_lake.lifecycle_cli list --detailed
```

### Advanced Usage

```bash
# Apply retention with custom configuration
python -m src.data_lake.lifecycle_cli retention \
  --config lifecycle_config.json \
  --output retention_results.json

# Generate lineage graph with output
python -m src.data_lake.lifecycle_cli lineage \
  --table customer_analytics \
  --depth 3 \
  --output lineage_graph.json

# Optimize with specific strategy
python -m src.data_lake.lifecycle_cli optimize \
  --strategy performance_optimized \
  --output optimization_results.json
```

## API Reference

### DataLifecycleManager

#### Methods

**`__init__(delta_manager, config_path=None, metadata_table="lifecycle_metadata")`**
- Initialize the lifecycle manager with Delta Lake integration

**`add_retention_rule(rule: RetentionRule)`**
- Add or update a retention rule for a table

**`add_archive_policy(policy: ArchivePolicy)`**
- Add or update an archive policy for a table

**`track_lineage(lineage_record: LineageRecord)`**
- Track a data lineage record for an operation

**`apply_retention_policies(dry_run: bool = True) -> Dict[str, Dict[str, int]]`**
- Apply retention policies to configured tables

**`optimize_storage_costs() -> Dict[str, StorageMetrics]`**
- Analyze and optimize storage costs across all tables

**`get_lineage_graph(table_name: str, depth: int = 3) -> Dict[str, Any]`**
- Get complete lineage graph for a table

**`generate_lifecycle_report() -> Dict[str, Any]`**
- Generate comprehensive lifecycle management report

### DeltaLakeManager Extensions

#### Lifecycle-Enabled Methods

**`enable_lifecycle_tracking(lifecycle_manager: DataLifecycleManager)`**
- Enable automatic lineage tracking for all Delta operations

**`create_delta_table_with_lifecycle(..., enable_retention=True, retention_config=None)`**
- Create Delta table with automatic lifecycle setup

**`write_to_delta_with_lifecycle(..., source_tables=None, track_lineage=True)`**
- Write to Delta table with automatic lineage tracking

**`optimize_with_lifecycle(table_name, z_order_columns=None, track_lineage=True)`**
- Optimize Delta table and track the operation

**`apply_retention_policies(dry_run=True)`**
- Apply retention policies to all managed tables

**`get_table_lineage(table_name, depth=3)`**
- Get lineage information for a specific table

### Configuration Classes

#### RetentionRule
```python
@dataclass
class RetentionRule:
    table_name: str
    hot_days: int = 30
    warm_days: int = 90
    cold_days: int = 365
    archive_after_days: Optional[int] = None
    delete_after_days: Optional[int] = None
    partition_column: str = "created_at"
    enabled: bool = True
```

#### ArchivePolicy
```python
@dataclass
class ArchivePolicy:
    table_name: str
    archive_path: str
    compression: str = "gzip"
    format: str = "parquet"
    partition_by: List[str] = None
    enabled: bool = True
```

## Best Practices

### 1. Retention Policy Design

- **Start Conservative**: Begin with longer retention periods and reduce as needed
- **Business Alignment**: Align retention periods with business and compliance requirements
- **Partition Strategy**: Use appropriate partition columns for time-based retention
- **Regular Review**: Review and adjust retention policies quarterly

### 2. Archiving Strategy

- **Format Selection**: Use Parquet for analytical workloads, ORC for transactional data
- **Compression Choice**: GZIP for archival, Snappy for frequent access
- **Partitioning**: Partition archived data by year/month for cost-effective retrieval
- **Access Patterns**: Consider how archived data will be accessed when designing structure

### 3. Cost Optimization

- **Monitor Regularly**: Review storage costs and access patterns monthly
- **Tier Appropriately**: Move data to appropriate storage tiers based on access frequency
- **Lifecycle Automation**: Implement automated policies to reduce manual intervention
- **Forecast Planning**: Use cost forecasting for budget planning

### 4. Lineage Management

- **Track Operations**: Enable lineage tracking for all critical data operations
- **Document Transformations**: Include meaningful metadata in lineage records
- **Regular Cleanup**: Clean up old lineage records to manage metadata size
- **Impact Analysis**: Use lineage for impact analysis before making changes

### 5. Compliance and Governance

- **Data Classification**: Classify data based on sensitivity and compliance requirements
- **Retention Compliance**: Ensure retention policies meet legal and regulatory requirements
- **Audit Trail**: Maintain comprehensive audit trails for all lifecycle operations
- **Access Control**: Implement appropriate access controls for lifecycle management

## Troubleshooting

### Common Issues

#### 1. Retention Policies Not Applied
```
Error: No retention rules configured for table
```
**Solution**: Add retention rules using `add_retention_rule()` or load from configuration file.

#### 2. Lineage Tracking Disabled
```
Warning: Lifecycle manager not enabled
```
**Solution**: Enable lifecycle tracking with `delta_manager.enable_lifecycle_tracking(lifecycle_manager)`.

#### 3. Archive Path Access Issues
```
Error: Failed to archive data - access denied
```
**Solution**: Verify archive path permissions and S3 credentials.

#### 4. Large Table Performance
```
Warning: Retention policy taking too long
```
**Solution**: 
- Optimize table partitioning
- Use appropriate cluster resources
- Consider breaking large operations into smaller batches

#### 5. Configuration Validation Errors
```
Error: hot_days should be less than warm_days
```
**Solution**: Review and fix configuration using `validate_configuration()` method.

### Debugging Tips

1. **Enable Debug Logging**:
```python
import logging
logging.getLogger('src.data_lake.lifecycle_manager').setLevel(logging.DEBUG)
```

2. **Use Dry Run Mode**:
```python
# Always test with dry_run=True first
results = lifecycle_manager.apply_retention_policies(dry_run=True)
```

3. **Check Table Status**:
```python
# Verify table exists and is accessible
tables = delta_manager.list_tables()
print(f"Available tables: {[t['name'] for t in tables]}")
```

4. **Monitor Resource Usage**:
```python
# Check Spark UI for job progress and resource usage
print(f"Spark UI: {delta_manager.spark.sparkContext.uiWebUrl}")
```

### Performance Tuning

#### For Large Tables
- Increase Spark executor memory and cores
- Use appropriate partitioning strategy
- Enable adaptive query execution
- Consider using Z-ordering for frequently queried columns

#### For Many Small Tables
- Batch process multiple tables together
- Use appropriate parallelism settings
- Consider consolidating very small tables

#### For Archive Operations
- Use efficient compression algorithms
- Optimize partition structure for retrieval patterns
- Consider using columnar formats for analytical queries

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Storage Growth**: Monitor data lake storage growth trends
2. **Cost Trends**: Track monthly storage costs and identify anomalies
3. **Retention Compliance**: Ensure retention policies are being applied correctly
4. **Archive Success Rate**: Monitor success rate of archival operations
5. **Lineage Coverage**: Track percentage of operations with lineage data

### Recommended Alerts

1. **Storage Cost Spike**: Alert when monthly storage costs increase >20%
2. **Retention Policy Failures**: Alert on failed retention policy applications
3. **Large Table Growth**: Alert when individual tables grow >50% in size
4. **Archive Failures**: Alert on failed archival operations
5. **Lineage Gaps**: Alert when lineage tracking fails for critical tables

### Integration with Monitoring Systems

```python
# Example integration with Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Define metrics
retention_operations = Counter('lifecycle_retention_operations_total')
storage_cost_gauge = Gauge('lifecycle_storage_cost_usd')
archive_duration = Histogram('lifecycle_archive_duration_seconds')

# Update metrics during operations
def apply_retention_with_metrics():
    with archive_duration.time():
        results = lifecycle_manager.apply_retention_policies()
    
    retention_operations.inc()
    
    # Update storage cost gauge
    storage_metrics = lifecycle_manager.optimize_storage_costs()
    total_cost = sum(m.storage_cost_usd for m in storage_metrics.values())
    storage_cost_gauge.set(total_cost)
```

---

For more detailed information, refer to the source code documentation and examples in the `examples/` directory.
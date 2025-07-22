# RFM Customer Segmentation

## Overview

The RFM (Recency, Frequency, Monetary) Customer Segmentation system provides comprehensive customer analytics capabilities for e-commerce businesses. It analyzes customer transaction patterns to segment customers based on their purchasing behavior and enables data-driven marketing strategies.

## What is RFM Analysis?

RFM analysis is a customer segmentation technique that evaluates customers based on three key dimensions:

- **Recency (R)**: How recently a customer made a purchase
- **Frequency (F)**: How often a customer makes purchases
- **Monetary (M)**: How much money a customer spends

By scoring customers on these three dimensions, businesses can identify their most valuable customers, predict future behavior, and tailor marketing strategies accordingly.

## Key Features

### 🎯 Customer Segmentation
- **11 distinct customer segments** based on industry-standard RFM methodology
- **Automated scoring** using statistical quintiles or quartiles
- **Behavioral insights** for each segment with actionable recommendations

### 📊 Advanced Analytics
- **Customer lifetime value** indicators and trends
- **Churn risk identification** for proactive retention
- **High-value customer detection** for VIP programs
- **Purchase pattern analysis** and seasonal behavior insights

### 🚀 Scalable Processing
- **Apache Spark integration** for big data processing
- **Distributed computing** capabilities for large customer bases
- **Optimized algorithms** for efficient large-scale analysis

### 📈 Business Intelligence
- **Segment performance metrics** and revenue attribution
- **Customer value tiers** (High, Medium, Low Value)
- **Engagement levels** (Highly Engaged, Moderately Engaged, etc.)
- **Marketing action recommendations** for each segment

## Customer Segments

### 🏆 Champions
**Profile**: Best customers - recent purchases, high frequency, high monetary value
- **RFM Scores**: 555, 554, 544, 545, 454, 455, 445
- **Characteristics**: Top 5-10% of customers, highly engaged, brand loyal
- **Strategy**: Reward them, early product access, brand ambassadors
- **Revenue Impact**: Typically 25-40% of total revenue

### 💎 Loyal Customers
**Profile**: Consistent customers with good purchase history
- **RFM Scores**: 543, 444, 435, 355, 354, 345, 344, 335
- **Characteristics**: Regular purchasers, moderate to high value
- **Strategy**: Upsell premium products, loyalty programs, referral incentives
- **Revenue Impact**: 15-25% of total revenue

### 🌱 Potential Loyalists
**Profile**: Recent customers with good engagement potential
- **RFM Scores**: 512, 511, 422, 421, 412, 411, 311
- **Characteristics**: Recent activity, moderate frequency
- **Strategy**: Membership programs, personalized recommendations
- **Revenue Impact**: 10-15% of total revenue

### ✨ New Customers
**Profile**: Recently acquired customers with limited history
- **RFM Scores**: 551, 552, 541, 542, 533-515, 425, 413-315
- **Characteristics**: High recency, low frequency
- **Strategy**: Onboarding campaigns, early success programs
- **Revenue Impact**: 5-10% of total revenue

### 💡 Promising
**Profile**: Recent customers with growth potential
- **RFM Scores**: Various combinations with high recency, low frequency
- **Characteristics**: New to brand, testing purchases
- **Strategy**: Free trials, brand awareness, social proof
- **Revenue Impact**: 3-8% of total revenue

### ⚠️ Need Attention
**Profile**: Below-average customers requiring intervention
- **RFM Scores**: 323, 322, 231, 241, 251, 233, 232
- **Characteristics**: Declining engagement, moderate value
- **Strategy**: Limited-time offers, reactivation campaigns
- **Revenue Impact**: 5-10% of total revenue

### 😴 About to Sleep
**Profile**: Customers showing early signs of disengagement
- **RFM Scores**: 331, 321, 312, 221, 213, 222, 132
- **Characteristics**: Decreasing recency, historically active
- **Strategy**: Win-back campaigns, valuable content, product recommendations
- **Revenue Impact**: 5-12% of total revenue

### 🚨 At Risk
**Profile**: Previously valuable customers with declining activity
- **RFM Scores**: 155, 154, 144, 214, 215, 115, 114, 113
- **Characteristics**: Low recent activity, historically high value
- **Strategy**: Personalized outreach, special offers, customer service
- **Revenue Impact**: 8-15% of total revenue

### 🆘 Cannot Lose Them
**Profile**: High-value customers with very low recent activity
- **RFM Scores**: 144, 234, 134, 124, 123
- **Characteristics**: Highest historical value, concerning recency drop
- **Strategy**: Immediate intervention, executive outreach, exclusive offers
- **Revenue Impact**: 10-20% of total revenue at risk

### 🛌 Hibernating
**Profile**: Previously active customers now dormant
- **RFM Scores**: Various low combinations across all dimensions
- **Characteristics**: Extended periods of inactivity
- **Strategy**: Broad reactivation campaigns, new product categories
- **Revenue Impact**: 2-5% of total revenue

### ❌ Lost
**Profile**: Customers with minimal engagement across all metrics
- **RFM Scores**: 111, 112, 121, 131, 141, 151
- **Characteristics**: Lowest scores in all dimensions
- **Strategy**: Minimal investment unless cost-effective, focus on retention of others
- **Revenue Impact**: 1-3% of total revenue

## Technical Architecture

### Core Components

#### `RFMSegmentationEngine`
The main class that orchestrates the entire RFM analysis process:

```python
from src.analytics.rfm_segmentation import RFMSegmentationEngine

# Initialize engine
engine = RFMSegmentationEngine(
    spark_session=spark,
    reference_date=datetime(2023, 12, 31),
    quintiles=True
)

# Generate customer profiles
profiles = engine.generate_customer_profiles(transactions_df)
```

#### Key Methods

- `calculate_rfm_metrics()`: Computes R, F, M values from transaction data
- `assign_rfm_scores()`: Statistical scoring using quintiles/quartiles
- `assign_customer_segments()`: Maps scores to business segments
- `generate_customer_profiles()`: Creates comprehensive customer profiles
- `get_segment_summary()`: Provides segment-level analytics
- `identify_high_value_customers()`: Identifies top percentile customers
- `recommend_actions()`: Generates segment-based marketing recommendations

### Data Pipeline Integration

#### Input Requirements
```sql
-- Transaction table structure
CREATE TABLE transactions (
    customer_id STRING,
    transaction_date DATE,
    amount DOUBLE,
    -- Additional columns as needed
);
```

#### Processing Flow
1. **Data Ingestion**: Load transaction data from data lake/warehouse
2. **RFM Calculation**: Compute recency, frequency, monetary metrics
3. **Statistical Scoring**: Assign 1-5 scores using data distribution
4. **Segment Assignment**: Map scores to business segments using industry rules
5. **Profile Enrichment**: Add derived metrics and classifications
6. **Action Recommendations**: Generate marketing strategies per segment

## Usage Examples

### Basic RFM Analysis

```python
from pyspark.sql import SparkSession
from src.analytics.rfm_segmentation import RFMSegmentationEngine

# Setup
spark = SparkSession.builder.appName("RFM-Analysis").getOrCreate()
transactions_df = spark.read.parquet("path/to/transactions")

# Initialize engine
rfm_engine = RFMSegmentationEngine(
    spark_session=spark,
    reference_date=datetime(2023, 12, 31),
    quintiles=True
)

# Run analysis
customer_profiles = rfm_engine.generate_customer_profiles(transactions_df)
customer_profiles.show()
```

### CLI Usage

```bash
# Basic analysis
python -m src.analytics.rfm_cli analyze \
    --input /data/transactions.parquet \
    --output /results/rfm_analysis

# Custom configuration
python -m src.analytics.rfm_cli analyze \
    --input /data/sales.csv \
    --output /results/customer_segments \
    --format csv \
    --customer-id user_id \
    --transaction-date purchase_date \
    --amount total_spent \
    --reference-date 2023-12-31 \
    --quartiles

# Generate detailed report
python -m src.analytics.rfm_cli report \
    --input /results/rfm_analysis/customer_profiles \
    --output /reports/segment_analysis.csv
```

### Advanced Analytics

```python
# Segment analysis
segment_summary = rfm_engine.get_segment_summary(customer_profiles)
segment_summary.orderBy("total_revenue", ascending=False).show()

# High-value customer identification
high_value = rfm_engine.identify_high_value_customers(
    customer_profiles, top_percentage=0.2
)

# Action recommendations
profiles_with_actions = rfm_engine.recommend_actions(customer_profiles)
```

## Configuration Options

### Scoring Methods

#### Quintile Scoring (Default)
- **Range**: 1-5 for each dimension
- **Distribution**: Statistical quintiles (20% buckets)
- **Precision**: Higher granularity for detailed segmentation
- **Best For**: Large customer bases (>1000 customers)

#### Quartile Scoring
- **Range**: 1-4 for each dimension
- **Distribution**: Statistical quartiles (25% buckets)
- **Precision**: Simplified scoring for smaller datasets
- **Best For**: Smaller customer bases or simplified analysis

### Custom Column Mapping
```python
# Support for different schema structures
profiles = engine.generate_customer_profiles(
    transactions_df,
    customer_id_col="user_id",
    transaction_date_col="order_date",
    amount_col="revenue"
)
```

### Reference Date Configuration
```python
# Analyze customer behavior as of specific date
engine = RFMSegmentationEngine(
    spark_session=spark,
    reference_date=datetime(2023, 12, 31)  # End of year analysis
)
```

## Performance Considerations

### Optimization Strategies

#### Data Partitioning
```python
# Partition by customer for better performance
transactions_df.repartition("customer_id")
```

#### Caching Strategy
```python
# Cache intermediate results for iterative analysis
rfm_metrics = engine.calculate_rfm_metrics(transactions_df)
rfm_metrics.cache()
```

#### Resource Configuration
```python
# Optimize Spark configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Scalability Guidelines

| Customer Count | Memory Requirement | Processing Time | Recommended Configuration |
|----------------|-------------------|-----------------|---------------------------|
| < 10K | 2GB | < 5 minutes | Local mode, 2 cores |
| 10K - 100K | 4GB | 5-15 minutes | Local[4], 4GB driver |
| 100K - 1M | 8GB | 15-30 minutes | Small cluster, 3 executors |
| 1M+ | 16GB+ | 30+ minutes | Production cluster |

## Business Applications

### Marketing Campaign Optimization

#### Champions & Loyal Customers
```python
# Identify VIP customers for exclusive campaigns
vip_customers = customer_profiles.filter(
    col("segment").isin(["Champions", "Loyal Customers"]) &
    (col("monetary") > customer_profiles.approxQuantile("monetary", [0.8], 0.1)[0])
)
```

#### Win-Back Campaigns
```python
# Target at-risk high-value customers
winback_targets = customer_profiles.filter(
    col("segment").isin(["At Risk", "Cannot Lose Them"]) &
    (col("customer_value_tier") == "High Value")
)
```

#### New Customer Onboarding
```python
# Identify recent customers for onboarding
new_customers = customer_profiles.filter(
    col("segment") == "New Customers"
).orderBy(desc("last_purchase_date"))
```

### Revenue Analysis

#### Segment Revenue Contribution
```python
# Calculate revenue percentage by segment
revenue_analysis = segment_summary.select(
    "segment",
    "customer_count",
    "total_revenue",
    "revenue_percentage"
).orderBy(desc("revenue_percentage"))
```

#### Customer Lifetime Value Insights
```python
# Analyze CLV patterns by segment
clv_analysis = customer_profiles.groupBy("segment").agg(
    avg("monetary").alias("avg_clv"),
    avg("customer_lifetime_days").alias("avg_lifetime_days"),
    avg("purchase_frequency_per_month").alias("avg_monthly_frequency")
)
```

## Integration Patterns

### Data Lake Integration
```python
# Delta Lake integration
from src.data_lake.delta import DeltaLakeManager

delta_manager = DeltaLakeManager()
transactions_df = delta_manager.time_travel_read("transactions")

# Run RFM analysis on historical data
rfm_engine = RFMSegmentationEngine(spark, reference_date=datetime(2023, 6, 30))
profiles = rfm_engine.generate_customer_profiles(transactions_df)

# Store results in Delta Lake
delta_manager.write_to_delta(profiles, "customer_segments", mode="overwrite")
```

### Streaming Analytics
```python
# Real-time segment updates
streaming_transactions = spark.readStream.format("kafka")...

def process_batch(batch_df, batch_id):
    # Update RFM scores for affected customers
    updated_profiles = rfm_engine.generate_customer_profiles(batch_df)
    delta_manager.write_to_delta(updated_profiles, "customer_segments", mode="merge")

streaming_transactions.foreachBatch(process_batch).start()
```

### API Integration
```python
# RESTful API for real-time segment lookup
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/customer/<customer_id>/segment')
def get_customer_segment(customer_id):
    segment_info = customer_profiles.filter(
        col("customer_id") == customer_id
    ).select("segment", "customer_value_tier", "recommended_action").collect()

    return jsonify(segment_info[0].asDict() if segment_info else {})
```

## Monitoring and Maintenance

### Segment Drift Detection
```python
# Monitor segment distribution changes over time
def detect_segment_drift(current_profiles, historical_profiles):
    current_dist = current_profiles.groupBy("segment").count()
    historical_dist = historical_profiles.groupBy("segment").count()

    # Compare distributions and alert on significant changes
    drift_analysis = current_dist.join(historical_dist, "segment", "outer")
    return drift_analysis
```

### Performance Monitoring
```python
# Track analysis performance metrics
import time

start_time = time.time()
customer_profiles = rfm_engine.generate_customer_profiles(transactions_df)
processing_time = time.time() - start_time

logger.info(f"RFM analysis completed in {processing_time:.2f} seconds")
logger.info(f"Processed {customer_profiles.count()} customers")
```

### Data Quality Checks
```python
# Validate RFM analysis results
def validate_rfm_results(profiles_df):
    # Check for null scores
    null_scores = profiles_df.filter(
        col("recency_score").isNull() |
        col("frequency_score").isNull() |
        col("monetary_score").isNull()
    ).count()

    assert null_scores == 0, "Found null RFM scores"

    # Verify score ranges
    score_ranges = profiles_df.agg(
        min("recency_score").alias("min_r"),
        max("recency_score").alias("max_r"),
        min("frequency_score").alias("min_f"),
        max("frequency_score").alias("max_f"),
        min("monetary_score").alias("min_m"),
        max("monetary_score").alias("max_m")
    ).collect()[0]

    assert 1 <= score_ranges.min_r <= score_ranges.max_r <= 5
    assert 1 <= score_ranges.min_f <= score_ranges.max_f <= 5
    assert 1 <= score_ranges.min_m <= score_ranges.max_m <= 5
```

## Best Practices

### 📅 Analysis Frequency
- **Monthly**: Standard business reporting and campaign planning
- **Weekly**: Dynamic retail environments with fast-changing behavior
- **Daily**: High-frequency e-commerce with real-time personalization needs
- **Quarterly**: Strategic planning and annual budget allocation

### 📊 Reference Date Selection
- **End of Month/Quarter**: Consistent business reporting periods
- **Campaign Launch Dates**: Pre-campaign customer segmentation
- **Seasonal Analysis**: Peak season vs. off-season comparisons
- **Current Date**: Real-time operational decision making

### 🎯 Segment Customization
- **Industry Adaptation**: Adjust segment thresholds for industry norms
- **Business Model Alignment**: B2B vs. B2C segment definitions
- **Geographic Considerations**: Regional purchasing pattern differences
- **Product Category**: Segment rules for different product types

### 💡 Action Implementation
- **Personalization**: Use segments for targeted content delivery
- **Campaign Automation**: Trigger campaigns based on segment changes
- **Resource Allocation**: Budget distribution across segment priorities
- **Retention Focus**: Prioritize at-risk high-value customer interventions

## Troubleshooting

### Common Issues

#### Empty Segment Results
```python
# Check data availability and date ranges
transactions_df.select(
    min("transaction_date").alias("earliest_date"),
    max("transaction_date").alias("latest_date"),
    count("*").alias("total_transactions")
).show()
```

#### Performance Issues
```python
# Optimize partitioning and caching
transactions_df = transactions_df.repartition(200, "customer_id").cache()
```

#### Segment Mapping Failures
```python
# Handle unmapped RFM score combinations
unmapped_scores = scored_df.join(segment_mapping, "rfm_score", "left_anti")
unmapped_scores.show()  # Debug unmapped combinations
```

## Future Enhancements

- **Machine Learning Integration**: Predictive RFM scoring
- **Real-time Streaming**: Live segment updates
- **Multi-dimensional Analysis**: Extended RFM with additional dimensions
- **Automated Campaign Triggers**: ML-driven marketing automation
- **Advanced Visualization**: Interactive segment exploration dashboards

---

*This documentation is maintained by the E-Commerce Analytics Platform team. For questions or contributions, please see the project repository.*

# Customer Lifetime Value (CLV) Analytics

## Overview

The Customer Lifetime Value (CLV) analytics module provides comprehensive customer value analysis capabilities for e-commerce businesses. It combines historical CLV calculations, predictive modeling using machine learning, cohort analysis, and seamless integration with customer segmentation systems.

## Features

### 🔍 Historical CLV Analysis
- **Revenue-based CLV**: Calculate actual customer lifetime value from transaction history
- **Customer Metrics**: Track purchase frequency, average order value, customer lifespan
- **Profit Margin Integration**: Apply configurable profit margins for accurate value calculations
- **Behavioral Analysis**: Analyze customer purchase patterns and consistency

### 🔮 Predictive CLV Modeling
- **Machine Learning Models**: Support for Linear Regression, Random Forest, and Gradient Boosting
- **Feature Engineering**: Automated feature creation from transaction data
- **Model Training**: Automated model training with performance evaluation
- **Confidence Scoring**: Prediction confidence metrics for business decision-making
- **Flexible Horizons**: Configurable prediction time horizons (3, 6, 12, 24 months)

### 👥 Cohort Analysis
- **Temporal Grouping**: Monthly, quarterly, and yearly cohort analysis
- **Retention Metrics**: Track customer behavior over time
- **Revenue Trends**: Analyze cohort revenue patterns and performance
- **Comparative Analysis**: Compare cohort performance across time periods

### 🎯 Customer Segment Integration
- **RFM Integration**: Seamless integration with RFM customer segmentation
- **Segment Alignment**: Identify misalignments between CLV and segment classifications
- **Retention Prioritization**: Priority scoring for customer retention efforts
- **Business Recommendations**: Automated action recommendations based on CLV and segments

### 📊 Business Intelligence
- **Summary Statistics**: Comprehensive CLV distribution and summary metrics
- **Top Customer Identification**: Automated high-value customer identification
- **Business Impact Analysis**: Revenue forecasting and business value metrics
- **Action Recommendations**: Data-driven customer engagement strategies

## Architecture

### Core Components

```
CLVModelEngine
├── Historical CLV Calculator
├── Predictive Model Trainer
├── Cohort Analysis Engine
├── RFM Integration Module
└── Business Intelligence Layer
```

### Data Flow

```
Transaction Data → Feature Engineering → Model Training → Predictions → Business Insights
                ↓                    ↓                 ↓              ↓
            Historical CLV      Model Evaluation    Cohort Analysis   Recommendations
```

### Model Types

1. **Linear Regression**: Fast, interpretable baseline model
2. **Random Forest**: Robust ensemble method with feature importance
3. **Gradient Boosting**: High-performance gradient-based ensemble
4. **Ensemble** (Future): Combination of multiple models for optimal performance

## Installation

The CLV module is part of the E-Commerce Analytics Platform and requires:

```bash
# Core dependencies
pip install pyspark>=3.4.0
pip install pandas>=1.5.0
pip install numpy>=1.21.0

# For visualization (optional)
pip install matplotlib>=3.5.0
pip install seaborn>=0.11.0
```

## Quick Start

### Basic CLV Analysis

```python
from pyspark.sql import SparkSession
from src.analytics.clv_model import CLVModelEngine

# Initialize Spark session
spark = SparkSession.builder.appName("CLV-Analysis").getOrCreate()

# Load your transaction data
transactions_df = spark.read.parquet("path/to/transactions.parquet")

# Initialize CLV engine
clv_engine = CLVModelEngine(
    spark_session=spark,
    prediction_horizon_months=12,
    profit_margin=0.25
)

# Generate comprehensive CLV insights
clv_insights = clv_engine.generate_clv_insights(transactions_df)

# Display results
clv_insights.show()
```

### CLI Usage

```bash
# Historical CLV analysis
python src/analytics/clv_cli.py historical --data-size medium --profit-margin 0.25

# Predictive CLV analysis
python src/analytics/clv_cli.py predictive --model-type random_forest --prediction-horizon 12

# Cohort analysis
python src/analytics/clv_cli.py cohort --cohort-period monthly

# Complete integrated analysis
python src/analytics/clv_cli.py integrated --data-size large --rfm-segments-path /path/to/rfm_segments.parquet
```

## API Reference

### CLVModelEngine

#### Core Methods

##### `__init__(spark_session, prediction_horizon_months=12, profit_margin=0.2, model_type=CLVModelType.RANDOM_FOREST)`

Initialize the CLV model engine.

**Parameters:**
- `spark_session` (SparkSession): Active Spark session
- `prediction_horizon_months` (int): Months to predict CLV forward (default: 12)
- `profit_margin` (float): Profit margin for CLV calculations (default: 0.2)
- `model_type` (CLVModelType): ML model type for predictions (default: RANDOM_FOREST)

##### `calculate_historical_clv(transactions_df, customer_id_col="customer_id", transaction_date_col="transaction_date", amount_col="amount", profit_margin=None)`

Calculate historical customer lifetime value from transaction data.

**Parameters:**
- `transactions_df` (DataFrame): Transaction data with customer_id, date, amount
- `customer_id_col` (str): Customer identifier column name
- `transaction_date_col` (str): Transaction date column name
- `amount_col` (str): Transaction amount column name
- `profit_margin` (float, optional): Profit margin override

**Returns:**
- DataFrame with historical CLV metrics including total_revenue, historical_clv, customer_lifespan_days, purchase_frequency

**Example:**
```python
historical_clv = clv_engine.calculate_historical_clv(
    transactions_df,
    profit_margin=0.3  # 30% profit margin
)
```

##### `train_clv_prediction_model(training_data, target_col="historical_clv", test_split=0.2)`

Train machine learning model for CLV prediction.

**Parameters:**
- `training_data` (DataFrame): Training dataset with features and target
- `target_col` (str): Target variable column name
- `test_split` (float): Proportion of data for testing

**Returns:**
- Dictionary with trained model and evaluation metrics (rmse, mae, r2, feature_importance)

**Example:**
```python
model_results = clv_engine.train_clv_prediction_model(features_df)
print(f"Model R² Score: {model_results['r2']:.3f}")
print(f"RMSE: {model_results['rmse']:.2f}")
```

##### `predict_clv(customer_features_df, model=None)`

Generate CLV predictions using trained model.

**Parameters:**
- `customer_features_df` (DataFrame): Customer features for prediction
- `model` (Pipeline, optional): Trained model pipeline

**Returns:**
- DataFrame with predicted_clv and clv_confidence columns

**Example:**
```python
predictions = clv_engine.predict_clv(customer_features_df)
high_value_customers = predictions.filter(col("predicted_clv") > 1000)
```

##### `calculate_cohort_analysis(transactions_df, cohort_period=CohortPeriod.MONTHLY, customer_id_col="customer_id", transaction_date_col="transaction_date", amount_col="amount")`

Perform cohort analysis to track customer behavior over time.

**Parameters:**
- `transactions_df` (DataFrame): Transaction data
- `cohort_period` (CohortPeriod): Grouping period (MONTHLY, QUARTERLY, YEARLY)
- `customer_id_col` (str): Customer identifier column
- `transaction_date_col` (str): Transaction date column
- `amount_col` (str): Transaction amount column

**Returns:**
- DataFrame with cohort metrics including cohort_size, total_revenue, avg_clv, avg_orders_per_customer

**Example:**
```python
from src.analytics.clv_model import CohortPeriod

monthly_cohorts = clv_engine.calculate_cohort_analysis(
    transactions_df,
    cohort_period=CohortPeriod.MONTHLY
)
```

##### `integrate_with_rfm_segments(clv_df, rfm_segments_df, customer_id_col="customer_id")`

Integrate CLV predictions with RFM customer segments.

**Parameters:**
- `clv_df` (DataFrame): CLV predictions DataFrame
- `rfm_segments_df` (DataFrame): RFM segments DataFrame
- `customer_id_col` (str): Customer identifier column

**Returns:**
- Combined DataFrame with CLV and RFM insights, segment alignment analysis, retention priority

**Example:**
```python
integrated_insights = clv_engine.integrate_with_rfm_segments(
    clv_predictions,
    rfm_segments_df
)

# Find segment misalignments
misaligned = integrated_insights.filter(
    col("clv_segment_alignment").contains("Mismatch")
)
```

##### `generate_clv_insights(transactions_df, rfm_segments_df=None, customer_id_col="customer_id", transaction_date_col="transaction_date", amount_col="amount")`

Generate comprehensive CLV insights combining all analysis methods.

**Parameters:**
- `transactions_df` (DataFrame): Transaction data
- `rfm_segments_df` (DataFrame, optional): RFM segments for integration
- `customer_id_col` (str): Customer identifier column
- `transaction_date_col` (str): Transaction date column
- `amount_col` (str): Transaction amount column

**Returns:**
- Comprehensive CLV insights DataFrame with historical CLV, predictions, recommendations

**Example:**
```python
comprehensive_insights = clv_engine.generate_clv_insights(
    transactions_df,
    rfm_segments_df=rfm_segments  # Optional RFM integration
)

# Business recommendations
recommendations = comprehensive_insights.groupBy("recommended_action").count()
```

#### Utility Methods

##### `get_clv_summary_statistics(clv_df)`

Generate summary statistics for CLV analysis.

**Returns:**
- DataFrame with total_customers, avg_historical_clv, avg_predicted_clv, total_business_value

##### `prepare_features_for_prediction(customer_metrics_df, reference_date=None)`

Prepare feature set for CLV prediction modeling with automated feature engineering.

### Data Structures

#### CLVMetrics

```python
@dataclass
class CLVMetrics:
    customer_id: str
    historical_clv: float
    predicted_clv: float
    clv_confidence: float
    customer_lifespan_days: int
    avg_order_value: float
    purchase_frequency: float
    profit_margin: float
    acquisition_date: datetime
    last_purchase_date: datetime
    churn_probability: float
    segment: Optional[str] = None
    cohort: Optional[str] = None
```

#### CohortMetrics

```python
@dataclass
class CohortMetrics:
    cohort_period: str
    cohort_size: int
    total_revenue: float
    avg_clv: float
    retention_rate: float
    churn_rate: float
    avg_orders_per_customer: float
    avg_order_value: float
    months_tracked: int
```

## Business Use Cases

### 1. Customer Acquisition Strategy

**Scenario**: Optimize customer acquisition cost (CAC) based on predicted CLV

```python
# Calculate acquisition efficiency
clv_insights = clv_engine.generate_clv_insights(transactions_df)

# Identify high-value acquisition targets
high_clv_segments = clv_insights.filter(col("predicted_clv") > 1000)
acquisition_targets = high_clv_segments.groupBy("acquisition_channel").agg(
    avg("predicted_clv").alias("avg_clv"),
    count("*").alias("customer_count")
)

# Optimize CAC: Spend more on channels with higher predicted CLV
acquisition_targets.orderBy(desc("avg_clv")).show()
```

### 2. Customer Retention Campaigns

**Scenario**: Prioritize retention efforts based on CLV and churn risk

```python
# Identify retention priorities
retention_candidates = clv_insights.filter(
    (col("predicted_clv") > 500) &
    (col("recency_days") > 90)  # Haven't purchased recently
)

# Segment by retention priority
retention_priority = retention_candidates.groupBy("retention_priority").agg(
    count("*").alias("customer_count"),
    avg("predicted_clv").alias("avg_clv"),
    sum("predicted_clv").alias("total_at_risk_value")
)
```

### 3. Product and Pricing Strategy

**Scenario**: Optimize product recommendations and pricing based on CLV

```python
# Analyze CLV by product category
product_clv = transactions_df.join(clv_insights, "customer_id").groupBy("category").agg(
    avg("predicted_clv").alias("avg_customer_clv"),
    avg("amount").alias("avg_order_value"),
    count("customer_id").alias("customer_count")
)

# Identify high-CLV product categories for upselling
high_clv_products = product_clv.filter(col("avg_customer_clv") > 800)
```

### 4. Marketing Budget Allocation

**Scenario**: Allocate marketing budget based on customer value tiers

```python
# Create CLV-based customer tiers
clv_tiers = clv_insights.withColumn(
    "clv_tier",
    when(col("predicted_clv") > 2000, "Platinum")
    .when(col("predicted_clv") > 1000, "Gold")
    .when(col("predicted_clv") > 500, "Silver")
    .otherwise("Bronze")
)

# Calculate tier distribution and value
tier_analysis = clv_tiers.groupBy("clv_tier").agg(
    count("*").alias("customer_count"),
    avg("predicted_clv").alias("avg_clv"),
    sum("predicted_clv").alias("total_tier_value")
)

# Allocate marketing budget proportional to tier value
tier_analysis.orderBy(desc("total_tier_value")).show()
```

## Advanced Features

### Custom Model Training

```python
# Train with custom parameters
from src.analytics.clv_model import CLVModelType

# Initialize with specific model type
clv_engine = CLVModelEngine(
    spark_session=spark,
    model_type=CLVModelType.GRADIENT_BOOSTING,
    prediction_horizon_months=24  # 2-year horizon
)

# Custom feature engineering
features_df = clv_engine.prepare_features_for_prediction(historical_clv)

# Train with custom test split
model_results = clv_engine.train_clv_prediction_model(
    features_df,
    test_split=0.3  # 30% for testing
)

# Analyze feature importance
print("Feature Importance:")
for feature, importance in model_results['feature_importance'].items():
    print(f"{feature}: {importance:.3f}")
```

### Cohort Trend Analysis

```python
# Multi-period cohort analysis
cohort_trends = {}

for period in [CohortPeriod.MONTHLY, CohortPeriod.QUARTERLY]:
    cohort_trends[period.value] = clv_engine.calculate_cohort_analysis(
        transactions_df,
        cohort_period=period
    )

# Compare cohort performance
monthly_cohorts = cohort_trends['monthly']
quarterly_cohorts = cohort_trends['quarterly']

# Identify best-performing cohorts
top_monthly = monthly_cohorts.orderBy(desc("avg_clv")).limit(5)
top_quarterly = quarterly_cohorts.orderBy(desc("avg_clv")).limit(3)
```

### Custom Profit Margin Analysis

```python
# Analyze CLV sensitivity to profit margins
profit_scenarios = [0.15, 0.20, 0.25, 0.30, 0.35]
clv_scenarios = {}

for margin in profit_scenarios:
    clv_scenarios[margin] = clv_engine.calculate_historical_clv(
        transactions_df,
        profit_margin=margin
    )

# Compare total business value across scenarios
for margin, clv_df in clv_scenarios.items():
    total_value = clv_df.agg(sum("historical_clv")).collect()[0][0]
    print(f"Margin {margin:.0%}: Total CLV = ${total_value:,.2f}")
```

## Performance Optimization

### Data Partitioning

```python
# Optimize for large datasets
transactions_df = transactions_df.repartition("customer_id")

# Cache frequently accessed data
historical_clv = clv_engine.calculate_historical_clv(transactions_df)
historical_clv.cache()

# Persist intermediate results
features_df = clv_engine.prepare_features_for_prediction(historical_clv)
features_df.persist()
```

### Incremental Processing

```python
# Process new customers incrementally
new_customers_df = transactions_df.filter(
    col("transaction_date") >= "2023-12-01"
).select("customer_id").distinct()

# Generate CLV for new customers only
new_customer_clv = clv_engine.generate_clv_insights(
    transactions_df.join(new_customers_df, "customer_id")
)
```

## Monitoring and Maintenance

### Model Performance Monitoring

```python
# Track model performance over time
def evaluate_model_drift(current_predictions, historical_predictions):
    # Compare prediction distributions
    current_avg = current_predictions.agg(avg("predicted_clv")).collect()[0][0]
    historical_avg = historical_predictions.agg(avg("predicted_clv")).collect()[0][0]

    drift_percentage = abs(current_avg - historical_avg) / historical_avg * 100

    if drift_percentage > 10:  # 10% threshold
        print(f"⚠️ Model drift detected: {drift_percentage:.1f}% change in average CLV")
        return True
    return False

# Retrain model if drift detected
if evaluate_model_drift(new_predictions, baseline_predictions):
    print("🔄 Retraining CLV model...")
    model_results = clv_engine.train_clv_prediction_model(updated_features_df)
```

### Data Quality Checks

```python
# Validate data quality
def validate_clv_data(clv_df):
    checks = {
        "negative_clv": clv_df.filter(col("predicted_clv") < 0).count(),
        "null_predictions": clv_df.filter(col("predicted_clv").isNull()).count(),
        "extreme_values": clv_df.filter(col("predicted_clv") > 50000).count()
    }

    for check, count in checks.items():
        if count > 0:
            print(f"⚠️ Data quality issue: {check} = {count} records")

    return all(count == 0 for count in checks.values())

# Run validation
if validate_clv_data(clv_insights):
    print("✅ Data quality validation passed")
```

## Troubleshooting

### Common Issues

#### Memory Issues with Large Datasets

```python
# Solution: Increase Spark memory and optimize partitioning
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

# Reduce DataFrame size by selecting only necessary columns
minimal_transactions = transactions_df.select(
    "customer_id", "transaction_date", "amount"
)
```

#### Model Training Failures

```python
# Solution: Handle data quality issues
def clean_training_data(features_df):
    # Remove outliers
    cleaned_df = features_df.filter(
        (col("monetary") < 100000) &  # Remove extreme values
        (col("frequency") < 1000) &
        (col("recency_days") >= 0)
    )

    # Fill missing values
    cleaned_df = cleaned_df.fillna(0, subset=["days_between_orders"])

    return cleaned_df

# Use cleaned data for training
cleaned_features = clean_training_data(features_df)
model_results = clv_engine.train_clv_prediction_model(cleaned_features)
```

#### Poor Prediction Quality

```python
# Solution: Feature engineering and model tuning
# Add more sophisticated features
enhanced_features = features_df.withColumn(
    "seasonality_factor",
    when(month(col("last_purchase_date")).isin([11, 12]), 1.2)  # Holiday boost
    .otherwise(1.0)
).withColumn(
    "customer_tenure_months",
    months_between(current_date(), col("first_purchase_date"))
).withColumn(
    "purchase_acceleration",
    col("frequency") / col("customer_tenure_months")
)

# Try ensemble approach
ensemble_predictions = []
for model_type in [CLVModelType.RANDOM_FOREST, CLVModelType.GRADIENT_BOOSTING]:
    engine = CLVModelEngine(spark_session=spark, model_type=model_type)
    engine.train_clv_prediction_model(enhanced_features)
    predictions = engine.predict_clv(enhanced_features)
    ensemble_predictions.append(predictions)

# Average predictions from multiple models
final_predictions = ensemble_predictions[0].join(
    ensemble_predictions[1].select("customer_id", "predicted_clv").withColumnRenamed("predicted_clv", "predicted_clv_2"),
    "customer_id"
).withColumn(
    "ensemble_predicted_clv",
    (col("predicted_clv") + col("predicted_clv_2")) / 2
)
```

## Integration Examples

### Airflow DAG Integration

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def run_clv_analysis(**context):
    from src.analytics.clv_model import CLVModelEngine

    spark = SparkSession.builder.appName("CLV-Daily-Analysis").getOrCreate()
    clv_engine = CLVModelEngine(spark_session=spark)

    # Load yesterday's transactions
    transactions_df = spark.read.parquet(f"s3://data-lake/transactions/{context['ds']}/")

    # Generate insights
    clv_insights = clv_engine.generate_clv_insights(transactions_df)

    # Save results
    clv_insights.write.mode("overwrite").parquet(f"s3://analytics/clv/{context['ds']}/")

dag = DAG(
    'clv_daily_analysis',
    default_args={'retries': 2},
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1)
)

clv_task = PythonOperator(
    task_id='run_clv_analysis',
    python_callable=run_clv_analysis,
    dag=dag
)
```

### Real-time Streaming Integration

```python
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import window

def process_clv_streaming(spark):
    # Read streaming transactions
    streaming_df = spark.readStream.format("kafka").load()

    # Parse transaction data
    transactions = streaming_df.select(
        get_json_object(col("value").cast("string"), "$.customer_id").alias("customer_id"),
        get_json_object(col("value").cast("string"), "$.amount").cast("double").alias("amount"),
        get_json_object(col("value").cast("string"), "$.timestamp").cast("timestamp").alias("transaction_date")
    )

    # Aggregate by customer in 1-hour windows
    customer_aggregates = transactions.groupBy(
        "customer_id",
        window(col("transaction_date"), "1 hour")
    ).agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
    )

    # Update CLV calculations
    def update_clv(batch_df, batch_id):
        if batch_df.count() > 0:
            clv_engine = CLVModelEngine(spark_session=spark)
            # Recalculate CLV for affected customers
            updated_clv = clv_engine.calculate_historical_clv(batch_df)
            updated_clv.write.mode("append").saveAsTable("clv_updates")

    # Start streaming query
    query = customer_aggregates.writeStream.foreachBatch(update_clv).start()

    return query
```

## Best Practices

### 1. Data Preparation
- **Clean Data**: Remove outliers and handle missing values before CLV analysis
- **Feature Engineering**: Create meaningful features that capture customer behavior patterns
- **Data Validation**: Implement comprehensive data quality checks

### 2. Model Selection
- **Start Simple**: Begin with Linear Regression for baseline performance
- **Iterate**: Progress to Random Forest or Gradient Boosting for better accuracy
- **Ensemble**: Combine multiple models for optimal performance in production

### 3. Business Integration
- **Stakeholder Alignment**: Ensure CLV definitions align with business objectives
- **Action Orientation**: Design CLV insights to drive specific business actions
- **Regular Updates**: Retrain models monthly or quarterly to maintain accuracy

### 4. Performance Optimization
- **Data Partitioning**: Partition large datasets by customer_id for optimal performance
- **Caching**: Cache frequently accessed DataFrames during analysis
- **Resource Management**: Monitor Spark cluster resources and adjust as needed

## Conclusion

The CLV analytics module provides a comprehensive framework for understanding and predicting customer value. By combining historical analysis, predictive modeling, cohort analysis, and business intelligence, it enables data-driven customer management strategies that maximize business value and customer satisfaction.

For additional support or questions, please refer to the project documentation or contact the development team.

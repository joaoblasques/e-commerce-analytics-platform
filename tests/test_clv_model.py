"""
Tests for Customer Lifetime Value (CLV) Model

Comprehensive tests for CLV analysis, predictive modeling, and cohort analysis.
"""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.analytics.clv_model import (
    CLVMetrics,
    CLVModelEngine,
    CLVModelType,
    CohortMetrics,
    CohortPeriod,
)


class TestCLVModelEngine:
    """Test suite for CLV Model Engine."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for tests."""
        spark = (
            SparkSession.builder.appName("CLV-Tests")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        yield spark
        spark.stop()

    @pytest.fixture
    def sample_transactions_data(self):
        """Generate sample transaction data for tests."""
        return [
            # High-value customer with consistent purchases
            ("customer_001", "2023-01-15", 500.00),
            ("customer_001", "2023-02-20", 600.00),
            ("customer_001", "2023-03-25", 550.00),
            ("customer_001", "2023-05-10", 700.00),
            ("customer_001", "2023-07-15", 650.00),
            ("customer_001", "2023-09-20", 750.00),
            ("customer_001", "2023-11-25", 800.00),
            # Medium-value customer with declining purchases
            ("customer_002", "2023-01-10", 200.00),
            ("customer_002", "2023-03-15", 180.00),
            ("customer_002", "2023-06-20", 150.00),
            ("customer_002", "2023-10-25", 120.00),
            # Low-value customer with few purchases
            ("customer_003", "2023-02-01", 50.00),
            ("customer_003", "2023-08-15", 75.00),
            # New customer with recent activity
            ("customer_004", "2023-11-01", 300.00),
            ("customer_004", "2023-11-15", 250.00),
            ("customer_004", "2023-12-01", 400.00),
            # Churned customer (old purchases only)
            ("customer_005", "2022-06-01", 100.00),
            ("customer_005", "2022-08-15", 150.00),
            ("customer_005", "2022-12-20", 200.00),
            # Single purchase customer
            ("customer_006", "2023-05-15", 1000.00),
            # Frequent small purchases
            ("customer_007", "2023-01-05", 25.00),
            ("customer_007", "2023-01-20", 30.00),
            ("customer_007", "2023-02-10", 20.00),
            ("customer_007", "2023-02-25", 35.00),
            ("customer_007", "2023-03-15", 40.00),
            ("customer_007", "2023-04-05", 28.00),
            ("customer_007", "2023-04-20", 32.00),
            ("customer_007", "2023-05-10", 38.00),
        ]

    @pytest.fixture
    def transactions_schema(self):
        """Schema for transaction data."""
        return StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", DateType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )

    @pytest.fixture
    def sample_transactions_df(
        self, spark_session, sample_transactions_data, transactions_schema
    ):
        """Create sample transactions DataFrame."""
        return spark_session.createDataFrame(
            sample_transactions_data, transactions_schema
        )

    @pytest.fixture
    def clv_engine(self, spark_session):
        """Create CLV engine with test configuration."""
        return CLVModelEngine(
            spark_session=spark_session,
            prediction_horizon_months=12,
            profit_margin=0.25,
            model_type=CLVModelType.RANDOM_FOREST,
        )

    @pytest.fixture
    def sample_rfm_segments(self, spark_session):
        """Create sample RFM segments for integration testing."""
        rfm_data = [
            ("customer_001", "Champions", "555", "High Value", "Highly Engaged"),
            ("customer_002", "At Risk", "155", "Medium Value", "Low Engagement"),
            ("customer_003", "Lost", "111", "Low Value", "Lost"),
            ("customer_004", "New Customers", "511", "High Value", "New"),
            ("customer_005", "Hibernating", "122", "Low Value", "Inactive"),
            ("customer_006", "Promising", "311", "Medium Value", "Moderate Engagement"),
            ("customer_007", "Need Attention", "255", "Medium Value", "Declining"),
        ]

        rfm_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("segment", StringType(), True),
                StructField("rfm_score", StringType(), True),
                StructField("customer_value_tier", StringType(), True),
                StructField("engagement_level", StringType(), True),
            ]
        )

        return spark_session.createDataFrame(rfm_data, rfm_schema)

    def test_clv_engine_initialization(self, spark_session):
        """Test CLV engine initialization."""
        engine = CLVModelEngine(
            spark_session=spark_session,
            prediction_horizon_months=24,
            profit_margin=0.3,
            model_type=CLVModelType.LINEAR_REGRESSION,
        )

        assert engine.spark == spark_session
        assert engine.prediction_horizon_months == 24
        assert engine.profit_margin == 0.3
        assert engine.model_type == CLVModelType.LINEAR_REGRESSION
        assert engine.trained_model is None
        assert len(engine.feature_cols) > 0

    def test_clv_engine_initialization_without_spark_session(self):
        """Test CLV engine initialization without Spark session."""
        with pytest.raises(ValueError, match="No active Spark session found"):
            CLVModelEngine(spark_session=None)

    def test_calculate_historical_clv(self, clv_engine, sample_transactions_df):
        """Test historical CLV calculation."""
        historical_clv = clv_engine.calculate_historical_clv(sample_transactions_df)

        # Check that we get results for all customers
        customer_count = historical_clv.count()
        unique_customers = (
            sample_transactions_df.select("customer_id").distinct().count()
        )
        assert customer_count == unique_customers

        # Check required columns are present
        expected_columns = [
            "customer_id",
            "total_revenue",
            "total_orders",
            "avg_order_value",
            "first_purchase_date",
            "last_purchase_date",
            "historical_clv",
            "customer_lifespan_days",
            "purchase_frequency",
            "days_between_orders",
        ]
        assert all(col in historical_clv.columns for col in expected_columns)

        # Test specific customer calculations
        customer_001_data = historical_clv.filter(
            col("customer_id") == "customer_001"
        ).collect()[0]

        # Customer_001 has 7 transactions totaling 4550.00
        expected_revenue = 4550.00
        expected_clv = expected_revenue * clv_engine.profit_margin

        assert abs(customer_001_data.total_revenue - expected_revenue) < 0.01
        assert abs(customer_001_data.historical_clv - expected_clv) < 0.01
        assert customer_001_data.total_orders == 7

    def test_prepare_features_for_prediction(self, clv_engine, sample_transactions_df):
        """Test feature preparation for prediction."""
        historical_clv = clv_engine.calculate_historical_clv(sample_transactions_df)
        features_df = clv_engine.prepare_features_for_prediction(historical_clv)

        # Check that feature columns are added
        feature_columns = [
            "recency_days",
            "months_since_first_purchase",
            "frequency",
            "monetary",
            "purchase_velocity",
            "revenue_trend",
            "customer_maturity",
        ]
        assert all(col in features_df.columns for col in feature_columns)

        # Check that all required features for modeling are present
        for feature_col in clv_engine.feature_cols:
            if feature_col in features_df.columns:
                # Check that feature values are reasonable
                feature_stats = features_df.select(feature_col).describe().collect()
                assert len(feature_stats) > 0

    def test_train_clv_prediction_model(self, clv_engine, sample_transactions_df):
        """Test CLV prediction model training."""
        historical_clv = clv_engine.calculate_historical_clv(sample_transactions_df)
        features_df = clv_engine.prepare_features_for_prediction(historical_clv)

        # Train model
        model_results = clv_engine.train_clv_prediction_model(features_df)

        # Check that model training results are returned
        assert "model" in model_results
        assert "rmse" in model_results
        assert "mae" in model_results
        assert "r2" in model_results
        assert "train_count" in model_results
        assert "test_count" in model_results

        # Check that model is saved to engine
        assert clv_engine.trained_model is not None

        # Check that metrics are reasonable
        assert model_results["rmse"] >= 0
        assert model_results["mae"] >= 0
        assert model_results["train_count"] > 0
        assert model_results["test_count"] >= 0

    def test_predict_clv(self, clv_engine, sample_transactions_df):
        """Test CLV prediction."""
        historical_clv = clv_engine.calculate_historical_clv(sample_transactions_df)
        features_df = clv_engine.prepare_features_for_prediction(historical_clv)

        # Train model first
        clv_engine.train_clv_prediction_model(features_df)

        # Generate predictions
        predictions_df = clv_engine.predict_clv(features_df)

        # Check that prediction columns are added
        prediction_columns = ["predicted_clv", "clv_confidence"]
        assert all(col in predictions_df.columns for col in prediction_columns)

        # Check that all customers have predictions
        null_predictions = predictions_df.filter(col("predicted_clv").isNull()).count()
        assert null_predictions == 0

        # Check that predictions are non-negative
        negative_predictions = predictions_df.filter(col("predicted_clv") < 0).count()
        assert negative_predictions == 0

        # Check that confidence scores are between 0 and 1
        confidence_stats = predictions_df.select("clv_confidence").describe().collect()
        min_confidence = float(
            [row for row in confidence_stats if row.summary == "min"][0].clv_confidence
        )
        max_confidence = float(
            [row for row in confidence_stats if row.summary == "max"][0].clv_confidence
        )

        assert min_confidence >= 0
        assert max_confidence <= 1

    def test_calculate_cohort_analysis(self, clv_engine, sample_transactions_df):
        """Test cohort analysis calculation."""
        cohort_results = clv_engine.calculate_cohort_analysis(
            sample_transactions_df, cohort_period=CohortPeriod.MONTHLY
        )

        # Check that cohort columns are present
        expected_columns = [
            "cohort_period",
            "cohort_size",
            "total_revenue",
            "avg_order_value",
            "total_transactions",
            "avg_clv",
            "avg_orders_per_customer",
        ]
        assert all(col in cohort_results.columns for col in expected_columns)

        # Check that we have cohort data
        cohort_count = cohort_results.count()
        assert cohort_count > 0

        # Check that cohort metrics are reasonable
        cohorts = cohort_results.collect()
        for cohort in cohorts:
            assert cohort.cohort_size > 0
            assert cohort.total_revenue > 0
            assert cohort.avg_clv > 0
            assert cohort.avg_orders_per_customer > 0

    def test_integrate_with_rfm_segments(
        self, clv_engine, sample_transactions_df, sample_rfm_segments
    ):
        """Test integration with RFM segments."""
        historical_clv = clv_engine.calculate_historical_clv(sample_transactions_df)
        features_df = clv_engine.prepare_features_for_prediction(historical_clv)

        # Train model and predict
        clv_engine.train_clv_prediction_model(features_df)
        predictions_df = clv_engine.predict_clv(features_df)

        # Integrate with RFM segments
        integrated_df = clv_engine.integrate_with_rfm_segments(
            predictions_df, sample_rfm_segments
        )

        # Check that RFM columns are added
        rfm_columns = [
            "segment",
            "rfm_score",
            "customer_value_tier",
            "engagement_level",
            "clv_segment_alignment",
            "retention_priority",
        ]
        assert all(col in integrated_df.columns for col in rfm_columns)

        # Check that segment alignment is calculated
        alignment_values = (
            integrated_df.select("clv_segment_alignment").distinct().collect()
        )
        assert len(alignment_values) > 0

        # Check that retention priority is assigned
        priority_values = (
            integrated_df.select("retention_priority").distinct().collect()
        )
        assert len(priority_values) > 0

    def test_generate_clv_insights(self, clv_engine, sample_transactions_df):
        """Test comprehensive CLV insights generation."""
        clv_insights = clv_engine.generate_clv_insights(sample_transactions_df)

        # Check that all analysis components are included
        expected_columns = [
            "customer_id",
            "historical_clv",
            "predicted_clv",
            "clv_confidence",
            "total_revenue",
            "total_orders",
            "recommended_action",
        ]
        assert all(col in clv_insights.columns for col in expected_columns)

        # Check that all customers have insights
        insights_count = clv_insights.count()
        unique_customers = (
            sample_transactions_df.select("customer_id").distinct().count()
        )
        assert insights_count == unique_customers

        # Check that recommendations are provided
        null_recommendations = clv_insights.filter(
            col("recommended_action").isNull()
        ).count()
        assert null_recommendations == 0

        # Check that recommendations are meaningful
        recommendations = clv_insights.select("recommended_action").distinct().collect()
        assert len(recommendations) > 1  # Should have multiple recommendation types

    def test_get_clv_summary_statistics(self, clv_engine, sample_transactions_df):
        """Test CLV summary statistics generation."""
        clv_insights = clv_engine.generate_clv_insights(sample_transactions_df)
        summary_stats = clv_engine.get_clv_summary_statistics(clv_insights)

        # Check that summary columns are present
        expected_columns = [
            "total_customers",
            "avg_historical_clv",
            "avg_predicted_clv",
            "max_predicted_clv",
            "min_predicted_clv",
            "clv_stddev",
            "total_historical_value",
            "total_predicted_value",
        ]

        summary_data = summary_stats.collect()[0]
        for col_name in expected_columns:
            assert hasattr(summary_data, col_name)

        # Check that summary values are reasonable
        assert summary_data.total_customers > 0
        assert summary_data.avg_historical_clv > 0
        assert summary_data.avg_predicted_clv > 0
        assert summary_data.total_historical_value > 0
        assert summary_data.total_predicted_value > 0

    def test_different_model_types(self, spark_session, sample_transactions_df):
        """Test different ML model types for CLV prediction."""
        model_types = [
            CLVModelType.LINEAR_REGRESSION,
            CLVModelType.RANDOM_FOREST,
            CLVModelType.GRADIENT_BOOSTING,
        ]

        for model_type in model_types:
            engine = CLVModelEngine(spark_session=spark_session, model_type=model_type)

            historical_clv = engine.calculate_historical_clv(sample_transactions_df)
            features_df = engine.prepare_features_for_prediction(historical_clv)

            # Train model
            model_results = engine.train_clv_prediction_model(features_df)

            # Check that training succeeds for all model types
            assert "model" in model_results
            assert model_results["rmse"] >= 0
            assert engine.trained_model is not None

            # Generate predictions
            predictions_df = engine.predict_clv(features_df)
            assert predictions_df.count() > 0

    def test_cohort_period_types(self, clv_engine, sample_transactions_df):
        """Test different cohort period types."""
        cohort_periods = [
            CohortPeriod.MONTHLY,
            CohortPeriod.QUARTERLY,
            CohortPeriod.YEARLY,
        ]

        for period in cohort_periods:
            cohort_results = clv_engine.calculate_cohort_analysis(
                sample_transactions_df, cohort_period=period
            )

            # Check that analysis succeeds for all period types
            assert cohort_results.count() > 0
            assert "cohort_period" in cohort_results.columns
            assert "cohort_size" in cohort_results.columns

    def test_custom_column_names(self, clv_engine, spark_session):
        """Test CLV analysis with custom column names."""
        # Create data with different column names
        custom_data = [
            ("user_001", "2023-06-01", 200.00),
            ("user_001", "2023-07-15", 250.00),
            ("user_002", "2023-05-20", 150.00),
        ]

        custom_schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("order_date", DateType(), True),
                StructField("total", DoubleType(), True),
            ]
        )

        custom_df = spark_session.createDataFrame(custom_data, custom_schema)

        # Test with custom column names
        historical_clv = clv_engine.calculate_historical_clv(
            custom_df,
            customer_id_col="user_id",
            transaction_date_col="order_date",
            amount_col="total",
        )

        assert historical_clv.count() == 2
        assert "user_id" in historical_clv.columns

    def test_edge_cases(self, spark_session):
        """Test edge cases and error conditions."""
        engine = CLVModelEngine(spark_session=spark_session)

        # Test with empty DataFrame
        empty_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", DateType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )
        empty_df = spark_session.createDataFrame([], empty_schema)

        # Should handle empty data gracefully
        empty_clv = engine.calculate_historical_clv(empty_df)
        assert empty_clv.count() == 0

        # Test with single customer, single transaction
        single_data = [("customer_001", "2023-06-01", 100.00)]
        single_df = spark_session.createDataFrame(single_data, empty_schema)

        single_clv = engine.calculate_historical_clv(single_df)
        assert single_clv.count() == 1

        customer = single_clv.collect()[0]
        assert customer.customer_id == "customer_001"
        assert customer.total_orders == 1
        assert customer.total_revenue == 100.00

    def test_prediction_without_trained_model(self, clv_engine, sample_transactions_df):
        """Test prediction without a trained model."""
        historical_clv = clv_engine.calculate_historical_clv(sample_transactions_df)
        features_df = clv_engine.prepare_features_for_prediction(historical_clv)

        # Should raise error when trying to predict without trained model
        with pytest.raises(ValueError, match="No trained model available"):
            clv_engine.predict_clv(features_df)

    def test_data_quality_with_nulls(self, clv_engine, spark_session):
        """Test CLV analysis with null values in data."""
        # Test with null values
        null_data = [
            ("customer_001", "2023-06-01", 150.00),
            ("customer_002", None, 200.00),  # Null date
            ("customer_003", "2023-05-20", None),  # Null amount
            (None, "2023-07-15", 175.00),  # Null customer ID
        ]

        null_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", DateType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )

        null_df = spark_session.createDataFrame(null_data, null_schema)

        # Should handle nulls by filtering them out
        clv_results = clv_engine.calculate_historical_clv(null_df)

        # Should only process valid records
        valid_results = clv_results.filter(
            col("customer_id").isNotNull()
            & col("total_revenue").isNotNull()
            & col("total_orders").isNotNull()
        )

        assert valid_results.count() >= 1

    def test_profit_margin_override(self, clv_engine, sample_transactions_df):
        """Test CLV calculation with different profit margins."""
        # Test with default profit margin
        default_clv = clv_engine.calculate_historical_clv(sample_transactions_df)

        # Test with custom profit margin
        custom_margin = 0.35
        custom_clv = clv_engine.calculate_historical_clv(
            sample_transactions_df, profit_margin=custom_margin
        )

        # Check that CLV changes with different margin
        default_customer = default_clv.filter(
            col("customer_id") == "customer_001"
        ).collect()[0]
        custom_customer = custom_clv.filter(
            col("customer_id") == "customer_001"
        ).collect()[0]

        expected_ratio = custom_margin / clv_engine.profit_margin
        actual_ratio = custom_customer.historical_clv / default_customer.historical_clv

        assert abs(actual_ratio - expected_ratio) < 0.01


class TestCLVDataStructures:
    """Test CLV data structures and utilities."""

    def test_clv_model_type_enum(self):
        """Test CLVModelType enum."""
        # Check that all expected model types exist
        expected_types = [
            "LINEAR_REGRESSION",
            "RANDOM_FOREST",
            "GRADIENT_BOOSTING",
            "ENSEMBLE",
        ]

        for type_name in expected_types:
            assert hasattr(CLVModelType, type_name)
            model_type = getattr(CLVModelType, type_name)
            assert isinstance(model_type.value, str)
            assert len(model_type.value) > 0

    def test_cohort_period_enum(self):
        """Test CohortPeriod enum."""
        # Check that all expected periods exist
        expected_periods = ["MONTHLY", "QUARTERLY", "YEARLY"]

        for period_name in expected_periods:
            assert hasattr(CohortPeriod, period_name)
            period = getattr(CohortPeriod, period_name)
            assert isinstance(period.value, str)
            assert len(period.value) > 0

    def test_clv_metrics_dataclass(self):
        """Test CLVMetrics dataclass."""
        metrics = CLVMetrics(
            customer_id="test_customer",
            historical_clv=1500.00,
            predicted_clv=2000.00,
            clv_confidence=0.85,
            customer_lifespan_days=365,
            avg_order_value=200.00,
            purchase_frequency=0.5,
            profit_margin=0.25,
            acquisition_date=datetime(2023, 1, 1),
            last_purchase_date=datetime(2023, 12, 1),
            churn_probability=0.15,
            segment="Champions",
            cohort="2023-01",
        )

        assert metrics.customer_id == "test_customer"
        assert metrics.historical_clv == 1500.00
        assert metrics.predicted_clv == 2000.00
        assert metrics.segment == "Champions"
        assert metrics.cohort == "2023-01"

    def test_cohort_metrics_dataclass(self):
        """Test CohortMetrics dataclass."""
        cohort_metrics = CohortMetrics(
            cohort_period="2023-01",
            cohort_size=100,
            total_revenue=50000.00,
            avg_clv=500.00,
            retention_rate=0.80,
            churn_rate=0.20,
            avg_orders_per_customer=3.5,
            avg_order_value=142.86,
            months_tracked=12,
        )

        assert cohort_metrics.cohort_period == "2023-01"
        assert cohort_metrics.cohort_size == 100
        assert cohort_metrics.total_revenue == 50000.00
        assert cohort_metrics.retention_rate == 0.80
        assert cohort_metrics.churn_rate == 0.20


@pytest.mark.integration
class TestCLVIntegration:
    """Integration tests for CLV modeling with real-world scenarios."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for integration tests."""
        spark = (
            SparkSession.builder.appName("CLV-Integration-Tests")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .getOrCreate()
        )
        yield spark
        spark.stop()

    def test_end_to_end_clv_workflow(self, spark_session):
        """Test complete end-to-end CLV analysis workflow."""
        # Create realistic e-commerce transaction data
        ecommerce_data = [
            # High-value customers with consistent spending
            ("vip_001", "2023-01-15", 800.00),
            ("vip_001", "2023-02-20", 950.00),
            ("vip_001", "2023-03-25", 1200.00),
            ("vip_001", "2023-05-10", 1100.00),
            ("vip_001", "2023-07-15", 1050.00),
            ("vip_001", "2023-09-20", 1300.00),
            ("vip_001", "2023-11-25", 1400.00),
            # Regular customers with moderate spending
            ("regular_001", "2023-02-01", 150.00),
            ("regular_001", "2023-03-15", 200.00),
            ("regular_001", "2023-05-20", 180.00),
            ("regular_001", "2023-08-10", 220.00),
            ("regular_001", "2023-10-25", 190.00),
            # New customers with growing engagement
            ("new_001", "2023-10-01", 100.00),
            ("new_001", "2023-11-15", 150.00),
            ("new_001", "2023-12-01", 200.00),
            # Churned customers (old activity only)
            ("churned_001", "2022-08-01", 300.00),
            ("churned_001", "2022-10-15", 250.00),
            ("churned_001", "2023-01-20", 200.00),
            # One-time high-value customers
            ("onetime_001", "2023-06-15", 2000.00),
            # Frequent low-value customers
            ("frequent_001", "2023-01-05", 25.00),
            ("frequent_001", "2023-01-20", 30.00),
            ("frequent_001", "2023-02-10", 20.00),
            ("frequent_001", "2023-02-25", 35.00),
            ("frequent_001", "2023-03-15", 40.00),
            ("frequent_001", "2023-04-05", 28.00),
            ("frequent_001", "2023-04-20", 32.00),
            ("frequent_001", "2023-05-10", 38.00),
            ("frequent_001", "2023-06-05", 45.00),
            ("frequent_001", "2023-07-20", 42.00),
        ]

        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", DateType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )

        transactions_df = spark_session.createDataFrame(ecommerce_data, schema)

        # Initialize CLV engine
        engine = CLVModelEngine(
            spark_session=spark_session,
            prediction_horizon_months=12,
            profit_margin=0.25,
            model_type=CLVModelType.RANDOM_FOREST,
        )

        # Run complete analysis
        clv_insights = engine.generate_clv_insights(transactions_df)
        cohort_analysis = engine.calculate_cohort_analysis(transactions_df)
        summary_stats = engine.get_clv_summary_statistics(clv_insights)

        # Validate results
        total_customers = clv_insights.count()
        assert total_customers > 0

        # Check that all customers have both historical and predicted CLV
        complete_insights = clv_insights.filter(
            col("historical_clv").isNotNull() & col("predicted_clv").isNotNull()
        )
        assert complete_insights.count() == total_customers

        # Verify cohort analysis
        cohort_count = cohort_analysis.count()
        assert cohort_count > 0

        # Check summary statistics
        summary_data = summary_stats.collect()[0]
        assert summary_data.total_customers == total_customers
        assert summary_data.avg_historical_clv > 0
        assert summary_data.avg_predicted_clv > 0

        # Verify recommendations are generated
        null_recommendations = clv_insights.filter(
            col("recommended_action").isNull()
        ).count()
        assert null_recommendations == 0

    def test_clv_with_seasonal_patterns(self, spark_session):
        """Test CLV analysis with seasonal customer behavior."""
        # Simulate seasonal shopping patterns
        seasonal_data = []

        # Holiday shoppers - high activity in November/December
        for customer_id in range(1, 21):
            for month in [11, 12]:  # November and December
                for purchase in range(3):  # 3 purchases per month
                    day = 5 + purchase * 10
                    amount = 200 + (customer_id * 10) + (purchase * 50)
                    seasonal_data.append(
                        (
                            f"holiday_{customer_id:02d}",
                            f"2023-{month:02d}-{day:02d}",
                            amount,
                        )
                    )

        # Summer shoppers - active in June/July
        for customer_id in range(1, 11):
            for month in [6, 7]:  # June and July
                for purchase in range(2):  # 2 purchases per month
                    day = 10 + purchase * 15
                    amount = 150 + (customer_id * 15)
                    seasonal_data.append(
                        (
                            f"summer_{customer_id:02d}",
                            f"2023-{month:02d}-{day:02d}",
                            amount,
                        )
                    )

        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", StringType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )

        seasonal_df = spark_session.createDataFrame(seasonal_data, schema)
        seasonal_df = seasonal_df.withColumn(
            "transaction_date", col("transaction_date").cast("date")
        )

        # Analyze with December reference date
        engine = CLVModelEngine(
            spark_session=spark_session,
            prediction_horizon_months=6,
            model_type=CLVModelType.RANDOM_FOREST,
        )

        clv_insights = engine.generate_clv_insights(seasonal_df)

        # Holiday shoppers should have different CLV patterns than summer shoppers
        holiday_customers = clv_insights.filter(
            col("customer_id").startswith("holiday_")
        )
        summer_customers = clv_insights.filter(col("customer_id").startswith("summer_"))

        holiday_avg_clv = holiday_customers.agg({"predicted_clv": "avg"}).collect()[0][
            0
        ]
        summer_avg_clv = summer_customers.agg({"predicted_clv": "avg"}).collect()[0][0]

        # Holiday customers should generally have higher predicted CLV due to recency
        assert holiday_avg_clv > summer_avg_clv

    def test_clv_business_impact_scenarios(self, spark_session):
        """Test CLV analysis for different business impact scenarios."""
        # Create data representing different customer value scenarios
        business_data = [
            # Scenario 1: High-value declining customers (retention critical)
            ("declining_vip_001", "2023-01-15", 2000.00),
            ("declining_vip_001", "2023-03-20", 1800.00),
            ("declining_vip_001", "2023-06-25", 1500.00),
            ("declining_vip_001", "2023-08-10", 1200.00),
            # Scenario 2: Growing value customers (upsell opportunity)
            ("growing_001", "2023-02-01", 100.00),
            ("growing_001", "2023-04-15", 200.00),
            ("growing_001", "2023-07-20", 350.00),
            ("growing_001", "2023-10-25", 500.00),
            ("growing_001", "2023-12-01", 750.00),
            # Scenario 3: Consistent high-value customers (maintain)
            ("consistent_vip_001", "2023-01-10", 1000.00),
            ("consistent_vip_001", "2023-03-15", 1050.00),
            ("consistent_vip_001", "2023-05-20", 980.00),
            ("consistent_vip_001", "2023-07-25", 1020.00),
            ("consistent_vip_001", "2023-09-30", 1100.00),
            ("consistent_vip_001", "2023-11-15", 1060.00),
            # Scenario 4: Low-value but frequent customers (volume play)
            ("frequent_low_001", "2023-01-05", 25.00),
            ("frequent_low_001", "2023-01-20", 30.00),
            ("frequent_low_001", "2023-02-10", 20.00),
            ("frequent_low_001", "2023-02-25", 35.00),
            ("frequent_low_001", "2023-03-15", 40.00),
            ("frequent_low_001", "2023-04-05", 28.00),
            ("frequent_low_001", "2023-04-20", 32.00),
            ("frequent_low_001", "2023-05-10", 38.00),
            ("frequent_low_001", "2023-06-05", 45.00),
            ("frequent_low_001", "2023-07-20", 42.00),
            ("frequent_low_001", "2023-08-15", 48.00),
            ("frequent_low_001", "2023-09-10", 52.00),
        ]

        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", DateType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )

        business_df = spark_session.createDataFrame(business_data, schema)

        engine = CLVModelEngine(
            spark_session=spark_session,
            prediction_horizon_months=12,
            model_type=CLVModelType.RANDOM_FOREST,
        )

        clv_insights = engine.generate_clv_insights(business_df)

        # Analyze different customer scenarios
        scenarios = {
            "declining_vip": clv_insights.filter(
                col("customer_id").startswith("declining_vip")
            ),
            "growing": clv_insights.filter(col("customer_id").startswith("growing")),
            "consistent_vip": clv_insights.filter(
                col("customer_id").startswith("consistent_vip")
            ),
            "frequent_low": clv_insights.filter(
                col("customer_id").startswith("frequent_low")
            ),
        }

        # Validate that different scenarios have appropriate CLV patterns
        for scenario_name, scenario_data in scenarios.items():
            scenario_results = scenario_data.collect()
            assert (
                len(scenario_results) > 0
            ), f"No results for scenario: {scenario_name}"

            for customer in scenario_results:
                # All customers should have positive CLV
                assert customer.historical_clv > 0
                assert customer.predicted_clv > 0

                # All customers should have recommendations
                assert customer.recommended_action is not None
                assert len(customer.recommended_action) > 20

        # Validate business logic: consistent VIP should have highest predicted CLV
        consistent_vip_clv = scenarios["consistent_vip"].collect()[0].predicted_clv
        frequent_low_clv = scenarios["frequent_low"].collect()[0].predicted_clv

        assert consistent_vip_clv > frequent_low_clv

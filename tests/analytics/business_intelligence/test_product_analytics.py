"""
Tests for product analytics module.

This module tests the ProductAnalytics class which provides comprehensive
product performance analysis, recommendation engine, market basket analysis,
and product lifecycle analytics.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics.business_intelligence.product_analytics import ProductAnalytics


class TestProductAnalytics:
    """Test suite for ProductAnalytics class."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return (
            SparkSession.builder.appName("TestProductAnalytics")
            .master("local[2]")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .getOrCreate()
        )

    @pytest.fixture
    def product_analytics(self, spark):
        """Create ProductAnalytics instance."""
        return ProductAnalytics(spark)

    @pytest.fixture
    def sample_transactions(self, spark):
        """Create sample transaction data."""
        schema = StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_category", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )

        data = [
            (
                "txn_001",
                "user_001",
                "prod_001",
                "Electronics",
                "Apple",
                999.99,
                1,
                datetime(2024, 1, 15),
            ),
            (
                "txn_002",
                "user_002",
                "prod_002",
                "Clothing",
                "Nike",
                89.99,
                2,
                datetime(2024, 1, 16),
            ),
            (
                "txn_003",
                "user_001",
                "prod_001",
                "Electronics",
                "Apple",
                999.99,
                1,
                datetime(2024, 1, 17),
            ),
            (
                "txn_004",
                "user_003",
                "prod_003",
                "Books",
                "Penguin",
                19.99,
                1,
                datetime(2024, 1, 18),
            ),
            (
                "txn_005",
                "user_002",
                "prod_004",
                "Electronics",
                "Samsung",
                799.99,
                1,
                datetime(2024, 1, 19),
            ),
        ]

        return spark.createDataFrame(data, schema)

    @pytest.fixture
    def sample_interactions(self, spark):
        """Create sample interaction data for recommendations."""
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("price", DoubleType(), True),
            ]
        )

        data = [
            ("user_001", "prod_001", 2, 999.99),
            ("user_002", "prod_001", 1, 999.99),
            ("user_002", "prod_002", 3, 89.99),
            ("user_003", "prod_002", 1, 89.99),
            ("user_003", "prod_003", 2, 19.99),
            ("user_001", "prod_003", 1, 19.99),
        ]

        return spark.createDataFrame(data, schema)

    @pytest.fixture
    def sample_inventory(self, spark):
        """Create sample inventory data."""
        schema = StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("stock_quantity", IntegerType(), True),
                StructField("product_category", StringType(), True),
            ]
        )

        data = [
            ("prod_001", 50, "Electronics"),
            ("prod_002", 25, "Clothing"),
            ("prod_003", 100, "Books"),
            ("prod_004", 30, "Electronics"),
        ]

        return spark.createDataFrame(data, schema)

    def test_initialization_default_config(self, spark):
        """Test ProductAnalytics initialization with default config."""
        analytics = ProductAnalytics(spark)

        assert analytics.spark == spark
        assert "product_dimensions" in analytics.config
        assert "recommendation" in analytics.config
        assert "market_basket" in analytics.config
        assert "lifecycle_analysis" in analytics.config
        assert isinstance(analytics.metrics_cache, dict)

    def test_initialization_custom_config(self, spark):
        """Test ProductAnalytics initialization with custom config."""
        custom_config = {
            "product_dimensions": ["category", "brand"],
            "recommendation": {"als_rank": 5},
        }
        analytics = ProductAnalytics(spark, custom_config)

        assert analytics.config["product_dimensions"] == ["category", "brand"]
        assert analytics.config["recommendation"]["als_rank"] == 5

    def test_analyze_product_performance_basic(
        self, product_analytics, sample_transactions
    ):
        """Test basic product performance analysis."""
        result = product_analytics.analyze_product_performance(sample_transactions)

        assert isinstance(result, dict)
        assert "overall_performance" in result
        assert "sales_metrics" in result
        assert "velocity_analysis" in result
        assert "profitability" in result
        assert "category_performance" in result
        assert "brand_performance" in result

    def test_analyze_product_performance_with_inventory(
        self, product_analytics, sample_transactions, sample_inventory
    ):
        """Test product performance analysis with inventory data."""
        result = product_analytics.analyze_product_performance(
            sample_transactions, inventory_df=sample_inventory
        )

        assert "inventory_analysis" in result
        inventory_df = result["inventory_analysis"]
        assert inventory_df.count() > 0
        assert "inventory_turnover" in inventory_df.columns
        assert "stockout_risk" in inventory_df.columns

    def test_analyze_product_performance_with_date_filter(
        self, product_analytics, sample_transactions
    ):
        """Test product performance analysis with date filtering."""
        start_date = datetime(2024, 1, 16)
        end_date = datetime(2024, 1, 18)

        result = product_analytics.analyze_product_performance(
            sample_transactions, start_date=start_date, end_date=end_date
        )

        assert isinstance(result, dict)
        # Should have filtered data
        overall_df = result["overall_performance"]
        assert overall_df.count() > 0

    @patch("src.analytics.business_intelligence.product_analytics.ALS")
    def test_build_recommendation_engine(
        self, mock_als, product_analytics, sample_interactions
    ):
        """Test recommendation engine building."""
        # Mock ALS model
        mock_model = MagicMock()
        mock_model.summary.rmse = 0.5
        mock_model.summary.r2 = 0.8
        mock_model.transform.return_value = sample_interactions.withColumn(
            "prediction", sample_interactions.price * 0.9
        )
        mock_model.recommendForUserSubset.return_value = sample_interactions.limit(2)
        mock_model.itemFactors = (
            sample_interactions.select("product_id")
            .distinct()
            .withColumn("id", sample_interactions.product_id)
        )

        mock_als_instance = MagicMock()
        mock_als_instance.fit.return_value = mock_model
        mock_als.return_value = mock_als_instance

        result = product_analytics.build_recommendation_engine(sample_interactions)

        assert isinstance(result, dict)
        assert "model" in result
        assert "model_metrics" in result
        assert "user_recommendations" in result
        assert "training_data_stats" in result
        assert result["model_metrics"]["rmse"] == 0.5

    @patch("src.analytics.business_intelligence.product_analytics.FPGrowth")
    def test_perform_market_basket_analysis(
        self, mock_fpgrowth, product_analytics, sample_transactions
    ):
        """Test market basket analysis."""
        # Mock FPGrowth model
        mock_model = MagicMock()
        mock_frequent_itemsets = product_analytics.spark.createDataFrame(
            [(["prod_001", "prod_002"], 2), (["prod_002", "prod_003"], 1)],
            ["items", "freq"],
        )
        mock_association_rules = product_analytics.spark.createDataFrame(
            [(["prod_001"], ["prod_002"], 0.8, 1.2)],
            ["antecedent", "consequent", "confidence", "lift"],
        )

        mock_model.freqItemsets = mock_frequent_itemsets
        mock_model.associationRules = mock_association_rules

        mock_fpgrowth_instance = MagicMock()
        mock_fpgrowth_instance.fit.return_value = mock_model
        mock_fpgrowth.return_value = mock_fpgrowth_instance

        result = product_analytics.perform_market_basket_analysis(sample_transactions)

        assert isinstance(result, dict)
        assert "frequent_itemsets" in result
        assert "association_rules" in result
        assert "analysis_summary" in result

    def test_analyze_product_lifecycle(self, product_analytics, sample_transactions):
        """Test product lifecycle analysis."""
        result = product_analytics.analyze_product_lifecycle(sample_transactions)

        assert isinstance(result, dict)
        assert "lifecycle_stages" in result
        assert "stage_transitions" in result
        assert "lifecycle_metrics" in result
        assert "at_risk_products" in result
        assert "growth_opportunities" in result
        assert "stage_distribution" in result

    def test_filter_by_date_range(self, product_analytics, sample_transactions):
        """Test date range filtering."""
        start_date = datetime(2024, 1, 16)
        end_date = datetime(2024, 1, 18)

        filtered_df = product_analytics._filter_by_date_range(
            sample_transactions, start_date, end_date
        )

        # Should have 3 records (transactions from 16th, 17th, 18th)
        assert filtered_df.count() == 3

    def test_add_time_dimension_day(self, product_analytics, sample_transactions):
        """Test adding day time dimension."""
        result_df = product_analytics._add_time_dimension(sample_transactions, "day")

        assert "time_dimension" in result_df.columns
        # Check that time dimension is formatted correctly
        first_row = result_df.first()
        assert first_row.time_dimension == "2024-01-15"

    def test_add_time_dimension_week(self, product_analytics, sample_transactions):
        """Test adding week time dimension."""
        result_df = product_analytics._add_time_dimension(sample_transactions, "week")

        assert "time_dimension" in result_df.columns

    def test_add_time_dimension_month(self, product_analytics, sample_transactions):
        """Test adding month time dimension."""
        result_df = product_analytics._add_time_dimension(sample_transactions, "month")

        assert "time_dimension" in result_df.columns
        first_row = result_df.first()
        assert first_row.time_dimension == "2024-01-01"

    def test_add_time_dimension_invalid_period(
        self, product_analytics, sample_transactions
    ):
        """Test adding time dimension with invalid period."""
        with pytest.raises(ValueError, match="Unsupported time period"):
            product_analytics._add_time_dimension(sample_transactions, "invalid")

    def test_calculate_product_metrics(self, product_analytics, sample_transactions):
        """Test product metrics calculation."""
        result_df = product_analytics._calculate_product_metrics(sample_transactions)

        assert "revenue" in result_df.columns
        assert "transaction_count" in result_df.columns
        assert "units_sold" in result_df.columns

        # Verify revenue calculation (price * quantity)
        first_row = result_df.first()
        assert first_row.revenue == 999.99

    def test_analyze_overall_product_performance(
        self, product_analytics, sample_transactions
    ):
        """Test overall product performance analysis."""
        metrics_df = product_analytics._calculate_product_metrics(sample_transactions)
        time_df = product_analytics._add_time_dimension(metrics_df, "day")

        result_df = product_analytics._analyze_overall_product_performance(
            time_df, "day"
        )

        expected_columns = [
            "time_dimension",
            "total_revenue",
            "total_units_sold",
            "total_transactions",
            "unique_products_sold",
            "unique_customers",
            "avg_selling_price",
            "avg_revenue_per_product",
            "avg_units_per_transaction",
        ]

        for col in expected_columns:
            assert col in result_df.columns

    def test_analyze_product_sales_metrics(
        self, product_analytics, sample_transactions
    ):
        """Test product sales metrics analysis."""
        metrics_df = product_analytics._calculate_product_metrics(sample_transactions)

        result_df = product_analytics._analyze_product_sales_metrics(metrics_df)

        expected_columns = [
            "product_id",
            "total_revenue",
            "total_units_sold",
            "transaction_count",
            "unique_customers",
            "avg_selling_price",
            "avg_revenue_per_customer",
        ]

        for col in expected_columns:
            assert col in result_df.columns

        # Should have entries for each unique product
        assert result_df.count() == 4  # 4 unique products in sample data

    def test_analyze_category_performance(self, product_analytics, sample_transactions):
        """Test category performance analysis."""
        metrics_df = product_analytics._calculate_product_metrics(sample_transactions)
        time_df = product_analytics._add_time_dimension(metrics_df, "day")

        result_df = product_analytics._analyze_category_performance(time_df)

        expected_columns = [
            "product_category",
            "time_dimension",
            "category_revenue",
            "category_units_sold",
            "products_in_category",
            "avg_revenue_per_product",
        ]

        for col in expected_columns:
            assert col in result_df.columns

    def test_analyze_brand_performance(self, product_analytics, sample_transactions):
        """Test brand performance analysis."""
        metrics_df = product_analytics._calculate_product_metrics(sample_transactions)
        time_df = product_analytics._add_time_dimension(metrics_df, "day")

        result_df = product_analytics._analyze_brand_performance(time_df)

        expected_columns = [
            "brand",
            "time_dimension",
            "brand_revenue",
            "brand_units_sold",
            "products_in_brand",
            "avg_revenue_per_product",
        ]

        for col in expected_columns:
            assert col in result_df.columns

    def test_prepare_basket_data(self, product_analytics, sample_transactions):
        """Test basket data preparation."""
        result_df = product_analytics._prepare_basket_data(sample_transactions)

        assert "transaction_id" in result_df.columns
        assert "items" in result_df.columns

        # Check that items are collected as lists
        first_row = result_df.first()
        assert isinstance(first_row.items, list)

    def test_prepare_interaction_data(self, product_analytics, sample_interactions):
        """Test interaction data preparation."""
        result_df = product_analytics._prepare_interaction_data(sample_interactions)

        expected_columns = [
            "user_id",
            "product_id",
            "interaction_count",
            "total_quantity",
            "total_spent",
            "rating",
        ]

        for col in expected_columns:
            assert col in result_df.columns

    def test_prepare_product_time_series(self, product_analytics, sample_transactions):
        """Test product time series preparation."""
        result_df = product_analytics._prepare_product_time_series(sample_transactions)

        expected_columns = [
            "product_id",
            "date",
            "daily_units_sold",
            "daily_revenue",
            "daily_transactions",
        ]

        for col in expected_columns:
            assert col in result_df.columns

    def test_classify_lifecycle_stages(self, product_analytics):
        """Test lifecycle stage classification."""
        # Create mock trend data
        mock_trends = product_analytics.spark.createDataFrame(
            [
                ("prod_001", "2024-01-15", 5, 999.99, 1, 0.1, 5.0, 4.8),
                ("prod_002", "2024-01-16", 2, 179.98, 2, -0.2, 2.0, 2.2),
            ],
            [
                "product_id",
                "date",
                "daily_units_sold",
                "daily_revenue",
                "daily_transactions",
                "growth_rate",
                "units_ma_7d",
                "units_ma_30d",
            ],
        )

        result_df = product_analytics._classify_lifecycle_stages(mock_trends)

        expected_columns = [
            "product_id",
            "avg_growth_rate",
            "growth_volatility",
            "avg_daily_sales",
            "days_active",
            "lifecycle_stage",
        ]

        for col in expected_columns:
            assert col in result_df.columns

    def test_error_handling_invalid_transactions(self, product_analytics, spark):
        """Test error handling with invalid transaction data."""
        # Create empty dataframe
        empty_df = spark.createDataFrame([], "transaction_id STRING, user_id STRING")

        # Should handle gracefully without crashing
        try:
            result = product_analytics.analyze_product_performance(empty_df)
            # If it doesn't crash, that's already good
            assert isinstance(result, dict)
        except Exception as e:
            # Expected to handle errors gracefully
            assert "Failed to analyze product performance" in str(e)

    def test_error_handling_missing_columns(self, product_analytics, spark):
        """Test error handling with missing required columns."""
        # Create dataframe with missing columns
        incomplete_df = spark.createDataFrame(
            [("txn_001", "user_001")], ["transaction_id", "user_id"]
        )

        # Should handle missing columns gracefully
        with pytest.raises(Exception, match=".*"):  # Allow any exception
            product_analytics.analyze_product_performance(incomplete_df)

    def test_configuration_validation(self, spark):
        """Test configuration validation."""
        # Test with valid configuration
        valid_config = {
            "product_dimensions": ["category"],
            "recommendation": {"als_rank": 10},
            "market_basket": {"min_support": 0.01},
        }

        analytics = ProductAnalytics(spark, valid_config)
        assert analytics.config["product_dimensions"] == ["category"]
        assert analytics.config["recommendation"]["als_rank"] == 10

    def test_metrics_caching(self, product_analytics):
        """Test metrics caching functionality."""
        # Initially empty
        assert len(product_analytics.metrics_cache) == 0

        # Add some cached metrics
        product_analytics.metrics_cache["test_key"] = "test_value"
        assert product_analytics.metrics_cache["test_key"] == "test_value"

    def test_large_dataset_handling(self, product_analytics, spark):
        """Test handling of larger datasets."""
        # Create a larger sample dataset
        large_data = []
        for i in range(1000):
            large_data.append(
                (
                    f"txn_{i:04d}",
                    f"user_{i % 100:03d}",  # 100 unique users
                    f"prod_{i % 50:03d}",  # 50 unique products
                    "Electronics",
                    "Brand",
                    999.99,
                    1,
                    datetime(2024, 1, 15) + timedelta(days=i % 30),
                )
            )

        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_category", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )
        large_df = spark.createDataFrame(large_data, schema)

        # Should handle larger datasets without issues
        result = product_analytics.analyze_product_performance(large_df)
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_time_period_aggregation_consistency(
        self, product_analytics, sample_transactions
    ):
        """Test consistency across different time period aggregations."""
        # Test day aggregation
        daily_result = product_analytics.analyze_product_performance(
            sample_transactions, time_period="day"
        )

        # Test week aggregation
        weekly_result = product_analytics.analyze_product_performance(
            sample_transactions, time_period="week"
        )

        # Test month aggregation
        monthly_result = product_analytics.analyze_product_performance(
            sample_transactions, time_period="month"
        )

        # All should have the same structure
        assert (
            set(daily_result.keys())
            == set(weekly_result.keys())
            == set(monthly_result.keys())
        )

    def test_cross_dimensional_analysis(self, product_analytics, sample_transactions):
        """Test cross-dimensional analysis capabilities."""
        result = product_analytics.analyze_product_performance(sample_transactions)

        # Should include cross-dimensional analysis in category and brand performance
        assert "category_performance" in result
        assert "brand_performance" in result

        category_df = result["category_performance"]
        brand_df = result["brand_performance"]

        # Both should have meaningful data
        assert category_df.count() > 0
        assert brand_df.count() > 0

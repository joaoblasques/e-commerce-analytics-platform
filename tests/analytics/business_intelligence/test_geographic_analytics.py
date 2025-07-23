"""
Unit tests for Geographic Analytics module.

This test suite covers all functionality of the GeographicAnalytics class including:
- Geographic sales distribution analysis
- Seasonal trend identification
- Regional demand forecasting
- Geographic customer segmentation

Test scenarios include unit tests, integration tests, and error handling.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col, lit
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    DataFrame = Mock
    SparkSession = Mock

from src.analytics.business_intelligence.geographic_analytics import GeographicAnalytics


class TestGeographicAnalytics:
    """Test suite for GeographicAnalytics class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession for testing."""
        spark = Mock(spec=SparkSession)
        spark.createDataFrame = Mock()
        spark.sql = Mock()
        return spark

    @pytest.fixture
    def geographic_analytics(self, mock_spark):
        """Create GeographicAnalytics instance with mocked Spark."""
        with patch(
            "src.analytics.business_intelligence.geographic_analytics.SPARK_AVAILABLE",
            True,
        ):
            return GeographicAnalytics(spark=mock_spark)

    @pytest.fixture
    def sample_transaction_data(self):
        """Create sample transaction data for testing."""
        return [
            {
                "transaction_id": "txn_001",
                "customer_id": "cust_001",
                "product_id": "prod_001",
                "amount": 150.00,
                "timestamp": datetime(2024, 1, 15, 10, 30),
                "payment_method": "credit_card",
            },
            {
                "transaction_id": "txn_002",
                "customer_id": "cust_002",
                "product_id": "prod_002",
                "amount": 250.00,
                "timestamp": datetime(2024, 2, 20, 14, 45),
                "payment_method": "debit_card",
            },
            {
                "transaction_id": "txn_003",
                "customer_id": "cust_003",
                "product_id": "prod_003",
                "amount": 350.00,
                "timestamp": datetime(2024, 3, 25, 16, 20),
                "payment_method": "paypal",
            },
            {
                "transaction_id": "txn_004",
                "customer_id": "cust_001",
                "product_id": "prod_004",
                "amount": 450.00,
                "timestamp": datetime(2024, 6, 10, 11, 15),
                "payment_method": "credit_card",
            },
            {
                "transaction_id": "txn_005",
                "customer_id": "cust_002",
                "product_id": "prod_005",
                "amount": 550.00,
                "timestamp": datetime(2024, 7, 15, 13, 30),
                "payment_method": "bank_transfer",
            },
        ]

    @pytest.fixture
    def sample_customer_data(self):
        """Create sample customer data for testing."""
        return [
            {
                "customer_id": "cust_001",
                "country": "USA",
                "region": "California",
                "city": "San Francisco",
                "latitude": 37.7749,
                "longitude": -122.4194,
                "registration_date": datetime(2023, 1, 15),
            },
            {
                "customer_id": "cust_002",
                "country": "USA",
                "region": "New York",
                "city": "New York City",
                "latitude": 40.7128,
                "longitude": -74.0060,
                "registration_date": datetime(2023, 2, 20),
            },
            {
                "customer_id": "cust_003",
                "country": "Canada",
                "region": "Ontario",
                "city": "Toronto",
                "latitude": 43.6532,
                "longitude": -79.3832,
                "registration_date": datetime(2023, 3, 25),
            },
        ]

    @pytest.fixture
    def sample_product_data(self):
        """Create sample product data for testing."""
        return [
            {
                "product_id": "prod_001",
                "category": "Electronics",
                "subcategory": "Smartphones",
                "brand": "Apple",
                "price": 999.99,
            },
            {
                "product_id": "prod_002",
                "category": "Clothing",
                "subcategory": "Shirts",
                "brand": "Nike",
                "price": 49.99,
            },
            {
                "product_id": "prod_003",
                "category": "Home",
                "subcategory": "Furniture",
                "brand": "IKEA",
                "price": 299.99,
            },
            {
                "product_id": "prod_004",
                "category": "Electronics",
                "subcategory": "Laptops",
                "brand": "Dell",
                "price": 1299.99,
            },
            {
                "product_id": "prod_005",
                "category": "Books",
                "subcategory": "Fiction",
                "brand": "Penguin",
                "price": 19.99,
            },
        ]

    def test_geographic_analytics_initialization(self, mock_spark):
        """Test GeographicAnalytics initialization."""
        with patch(
            "src.analytics.business_intelligence.geographic_analytics.SPARK_AVAILABLE",
            True,
        ):
            geo_analytics = GeographicAnalytics(spark=mock_spark)
            assert geo_analytics.spark == mock_spark
            assert geo_analytics.logger is not None

    def test_geographic_analytics_initialization_without_spark(self):
        """Test GeographicAnalytics initialization without Spark session."""
        with patch(
            "src.analytics.business_intelligence.geographic_analytics.SPARK_AVAILABLE",
            True,
        ):
            with patch(
                "src.analytics.business_intelligence.geographic_analytics.SparkSession"
            ) as mock_session:
                mock_session.builder.appName.return_value.config.return_value.config.return_value.getOrCreate.return_value = (
                    Mock()
                )
                geo_analytics = GeographicAnalytics()
                assert geo_analytics.spark is not None

    def test_geographic_analytics_initialization_no_spark_available(self):
        """Test GeographicAnalytics initialization when Spark is not available."""
        with patch(
            "src.analytics.business_intelligence.geographic_analytics.SPARK_AVAILABLE",
            False,
        ):
            with pytest.raises(ImportError):
                GeographicAnalytics()

    def test_analyze_geographic_distribution(self, geographic_analytics, mock_spark):
        """Test geographic distribution analysis."""
        # Mock DataFrames
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_df = Mock(spec=DataFrame)
        mock_joined_df = Mock(spec=DataFrame)

        # Setup join mock
        mock_customer_df.select.return_value = Mock()
        mock_transaction_df.join.return_value = mock_joined_df
        mock_joined_df.withColumn.return_value = mock_joined_df

        # Mock the analysis methods
        geographic_analytics._analyze_country_distribution = Mock(return_value=Mock())
        geographic_analytics._analyze_region_distribution = Mock(return_value=Mock())
        geographic_analytics._analyze_city_distribution = Mock(return_value=Mock())
        geographic_analytics._calculate_geographic_performance = Mock(
            return_value=Mock()
        )
        geographic_analytics._analyze_geographic_concentration = Mock(
            return_value=Mock()
        )
        geographic_analytics._calculate_distribution_summary = Mock(return_value=Mock())

        # Test the method
        result = geographic_analytics.analyze_geographic_distribution(
            mock_transaction_df,
            mock_customer_df,
            time_period="month",
            min_transactions=5,
        )

        # Verify results structure
        assert isinstance(result, dict)
        assert "country_distribution" in result
        assert "region_distribution" in result
        assert "city_distribution" in result
        assert "geographic_performance" in result
        assert "concentration_analysis" in result
        assert "summary_stats" in result

    def test_analyze_seasonal_trends(self, geographic_analytics, mock_spark):
        """Test seasonal trends analysis."""
        # Mock DataFrame
        mock_transaction_df = Mock(spec=DataFrame)
        mock_product_df = Mock(spec=DataFrame)
        mock_seasonal_df = Mock(spec=DataFrame)

        # Setup mocks
        geographic_analytics._add_temporal_features = Mock(
            return_value=mock_seasonal_df
        )
        mock_seasonal_df.join.return_value = mock_seasonal_df
        mock_product_df.select.return_value = Mock()

        # Mock analysis methods
        geographic_analytics._analyze_monthly_trends = Mock(return_value=Mock())
        geographic_analytics._analyze_quarterly_trends = Mock(return_value=Mock())
        geographic_analytics._analyze_weekly_trends = Mock(return_value=Mock())
        geographic_analytics._analyze_daily_trends = Mock(return_value=Mock())
        geographic_analytics._analyze_holiday_patterns = Mock(return_value=Mock())
        geographic_analytics._decompose_seasonal_patterns = Mock(return_value=Mock())
        geographic_analytics._analyze_category_seasonality = Mock(return_value=Mock())

        # Test the method
        result = geographic_analytics.analyze_seasonal_trends(
            mock_transaction_df,
            mock_product_df,
            seasonality_periods=["monthly", "quarterly"],
        )

        # Verify results structure
        assert isinstance(result, dict)
        assert "monthly_trends" in result
        assert "quarterly_trends" in result
        assert "holiday_analysis" in result
        assert "seasonal_decomposition" in result

    def test_forecast_regional_demand(self, geographic_analytics, mock_spark):
        """Test regional demand forecasting."""
        # Mock DataFrames
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_df = Mock(spec=DataFrame)
        mock_product_df = Mock(spec=DataFrame)
        mock_regional_df = Mock(spec=DataFrame)

        # Setup mocks
        geographic_analytics._prepare_regional_forecast_data = Mock(
            return_value=mock_regional_df
        )
        geographic_analytics._forecast_country_demand = Mock(return_value=Mock())
        geographic_analytics._forecast_region_demand = Mock(return_value=Mock())
        geographic_analytics._forecast_product_region_demand = Mock(return_value=Mock())
        geographic_analytics._calculate_forecast_accuracy = Mock(return_value=Mock())
        geographic_analytics._create_forecast_summary = Mock(return_value=Mock())

        # Test the method
        result = geographic_analytics.forecast_regional_demand(
            mock_transaction_df,
            mock_customer_df,
            mock_product_df,
            forecast_horizon=30,
            confidence_levels=[0.80, 0.90, 0.95],
        )

        # Verify results structure
        assert isinstance(result, dict)
        assert "country_forecasts" in result
        assert "region_forecasts" in result
        assert "forecast_metrics" in result
        assert "forecast_summary" in result
        assert "product_region_forecasts" in result

    def test_segment_customers_geographically(self, geographic_analytics, mock_spark):
        """Test geographic customer segmentation."""
        # Mock DataFrames
        mock_customer_df = Mock(spec=DataFrame)
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_features = Mock(spec=DataFrame)

        # Mock clustering results
        mock_clustering_results = {
            "segmented_customers": Mock(),
            "cluster_centers": [[1, 2, 3], [4, 5, 6]],
            "metrics": {"silhouette_score": 0.75, "n_clusters": 5},
        }

        # Setup mocks
        geographic_analytics._prepare_customer_features = Mock(
            return_value=mock_customer_features
        )
        geographic_analytics._perform_geographic_clustering = Mock(
            return_value=mock_clustering_results
        )
        geographic_analytics._analyze_geographic_clusters = Mock(return_value=Mock())
        geographic_analytics._create_segment_profiles = Mock(return_value=Mock())
        geographic_analytics._compare_segment_performance = Mock(return_value=Mock())

        # Test the method
        result = geographic_analytics.segment_customers_geographically(
            mock_customer_df,
            mock_transaction_df,
            n_clusters=5,
            features=["total_revenue", "transaction_count"],
        )

        # Verify results structure
        assert isinstance(result, dict)
        assert "segmented_customers" in result
        assert "cluster_centers" in result
        assert "cluster_analysis" in result
        assert "segment_profiles" in result
        assert "segment_performance" in result
        assert "segmentation_metrics" in result

    def test_get_analytics_summary(self, geographic_analytics):
        """Test analytics summary method."""
        summary = geographic_analytics.get_analytics_summary()

        assert isinstance(summary, dict)
        assert "module" in summary
        assert "version" in summary
        assert "capabilities" in summary
        assert "supported_aggregations" in summary
        assert "forecasting_methods" in summary
        assert "clustering_algorithms" in summary

        assert summary["module"] == "GeographicAnalytics"
        assert isinstance(summary["capabilities"], list)
        assert len(summary["capabilities"]) == 4

    def test_error_handling_in_geographic_distribution(
        self, geographic_analytics, mock_spark
    ):
        """Test error handling in geographic distribution analysis."""
        # Mock DataFrame that raises exception
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_df = Mock(spec=DataFrame)
        mock_transaction_df.join.side_effect = Exception("Mock error")

        with pytest.raises(Exception):
            geographic_analytics.analyze_geographic_distribution(
                mock_transaction_df, mock_customer_df
            )

    def test_error_handling_in_seasonal_trends(self, geographic_analytics, mock_spark):
        """Test error handling in seasonal trends analysis."""
        # Mock DataFrame that raises exception
        mock_transaction_df = Mock(spec=DataFrame)
        geographic_analytics._add_temporal_features = Mock(
            side_effect=Exception("Mock error")
        )

        with pytest.raises(Exception):
            geographic_analytics.analyze_seasonal_trends(mock_transaction_df)

    def test_error_handling_in_demand_forecasting(
        self, geographic_analytics, mock_spark
    ):
        """Test error handling in demand forecasting."""
        # Mock DataFrames that raise exception
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_df = Mock(spec=DataFrame)
        geographic_analytics._prepare_regional_forecast_data = Mock(
            side_effect=Exception("Mock error")
        )

        with pytest.raises(Exception):
            geographic_analytics.forecast_regional_demand(
                mock_transaction_df, mock_customer_df
            )

    def test_error_handling_in_customer_segmentation(
        self, geographic_analytics, mock_spark
    ):
        """Test error handling in customer segmentation."""
        # Mock DataFrames that raise exception
        mock_customer_df = Mock(spec=DataFrame)
        mock_transaction_df = Mock(spec=DataFrame)
        geographic_analytics._prepare_customer_features = Mock(
            side_effect=Exception("Mock error")
        )

        with pytest.raises(Exception):
            geographic_analytics.segment_customers_geographically(
                mock_customer_df, mock_transaction_df
            )

    def test_analyze_country_distribution_method(
        self, geographic_analytics, mock_spark
    ):
        """Test the _analyze_country_distribution helper method."""
        # Mock DataFrame with required methods
        mock_geo_df = Mock(spec=DataFrame)
        mock_grouped_df = Mock()
        mock_aggregated_df = Mock()
        mock_filtered_df = Mock()

        # Setup method chain
        mock_geo_df.groupBy.return_value = mock_grouped_df
        mock_grouped_df.agg.return_value = mock_aggregated_df
        mock_aggregated_df.filter.return_value = mock_filtered_df
        mock_filtered_df.withColumn.return_value = mock_filtered_df
        mock_filtered_df.orderBy.return_value = mock_filtered_df

        result = geographic_analytics._analyze_country_distribution(mock_geo_df, 10)

        # Verify method was called
        mock_geo_df.groupBy.assert_called_once_with("country")
        assert result is not None

    def test_add_temporal_features_method(self, geographic_analytics, mock_spark):
        """Test the _add_temporal_features helper method."""
        # Mock DataFrame
        mock_transaction_df = Mock(spec=DataFrame)
        mock_result_df = Mock()

        # Setup withColumn to return a chained result
        mock_transaction_df.withColumn.return_value = mock_result_df
        mock_result_df.withColumn.return_value = mock_result_df

        result = geographic_analytics._add_temporal_features(mock_transaction_df)

        # Verify method calls
        assert mock_transaction_df.withColumn.called
        assert result == mock_result_df

    def test_prepare_customer_features_method(self, geographic_analytics, mock_spark):
        """Test the _prepare_customer_features helper method."""
        # Mock DataFrames
        mock_customer_df = Mock(spec=DataFrame)
        mock_transaction_df = Mock(spec=DataFrame)
        mock_grouped_df = Mock()
        mock_metrics_df = Mock()
        mock_joined_df = Mock()

        # Setup method chains
        mock_transaction_df.groupBy.return_value = mock_grouped_df
        mock_grouped_df.agg.return_value = mock_metrics_df
        mock_metrics_df.withColumn.return_value = mock_metrics_df
        mock_customer_df.join.return_value = mock_joined_df
        mock_joined_df.withColumn.return_value = mock_joined_df

        features = ["total_revenue", "transaction_count", "avg_order_value"]
        result = geographic_analytics._prepare_customer_features(
            mock_customer_df, mock_transaction_df, features
        )

        # Verify method was called
        mock_transaction_df.groupBy.assert_called_once_with("customer_id")
        assert result is not None

    def test_different_time_periods_in_geographic_distribution(
        self, geographic_analytics, mock_spark
    ):
        """Test geographic distribution with different time periods."""
        # Mock DataFrames
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_df = Mock(spec=DataFrame)
        mock_joined_df = Mock(spec=DataFrame)

        # Setup join mock
        mock_customer_df.select.return_value = Mock()
        mock_transaction_df.join.return_value = mock_joined_df
        mock_joined_df.withColumn.return_value = mock_joined_df

        # Mock the analysis methods
        geographic_analytics._analyze_country_distribution = Mock(return_value=Mock())
        geographic_analytics._analyze_region_distribution = Mock(return_value=Mock())
        geographic_analytics._analyze_city_distribution = Mock(return_value=Mock())
        geographic_analytics._calculate_geographic_performance = Mock(
            return_value=Mock()
        )
        geographic_analytics._analyze_geographic_concentration = Mock(
            return_value=Mock()
        )
        geographic_analytics._calculate_distribution_summary = Mock(return_value=Mock())

        # Test different time periods
        time_periods = ["day", "week", "month", "quarter"]

        for period in time_periods:
            result = geographic_analytics.analyze_geographic_distribution(
                mock_transaction_df, mock_customer_df, time_period=period
            )
            assert isinstance(result, dict)

    def test_default_seasonality_periods(self, geographic_analytics, mock_spark):
        """Test seasonal trends analysis with default seasonality periods."""
        # Mock DataFrame
        mock_transaction_df = Mock(spec=DataFrame)
        mock_seasonal_df = Mock(spec=DataFrame)

        # Setup mocks
        geographic_analytics._add_temporal_features = Mock(
            return_value=mock_seasonal_df
        )

        # Mock analysis methods for all default periods
        geographic_analytics._analyze_monthly_trends = Mock(return_value=Mock())
        geographic_analytics._analyze_quarterly_trends = Mock(return_value=Mock())
        geographic_analytics._analyze_weekly_trends = Mock(return_value=Mock())
        geographic_analytics._analyze_daily_trends = Mock(return_value=Mock())
        geographic_analytics._analyze_holiday_patterns = Mock(return_value=Mock())
        geographic_analytics._decompose_seasonal_patterns = Mock(return_value=Mock())

        # Test with default seasonality periods (None)
        result = geographic_analytics.analyze_seasonal_trends(mock_transaction_df)

        # Verify all default analysis methods were called
        geographic_analytics._analyze_monthly_trends.assert_called_once()
        geographic_analytics._analyze_quarterly_trends.assert_called_once()
        geographic_analytics._analyze_weekly_trends.assert_called_once()
        geographic_analytics._analyze_daily_trends.assert_called_once()

    def test_forecast_without_product_data(self, geographic_analytics, mock_spark):
        """Test regional demand forecasting without product data."""
        # Mock DataFrames
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_df = Mock(spec=DataFrame)
        mock_regional_df = Mock(spec=DataFrame)

        # Setup mocks
        geographic_analytics._prepare_regional_forecast_data = Mock(
            return_value=mock_regional_df
        )
        geographic_analytics._forecast_country_demand = Mock(return_value=Mock())
        geographic_analytics._forecast_region_demand = Mock(return_value=Mock())
        geographic_analytics._calculate_forecast_accuracy = Mock(return_value=Mock())
        geographic_analytics._create_forecast_summary = Mock(return_value=Mock())

        # Test without product data (None)
        result = geographic_analytics.forecast_regional_demand(
            mock_transaction_df,
            mock_customer_df,
            product_df=None,  # Explicitly pass None
        )

        # Verify results structure
        assert isinstance(result, dict)
        assert "country_forecasts" in result
        assert "region_forecasts" in result
        assert "forecast_metrics" in result
        assert "forecast_summary" in result
        # Should not contain product_region_forecasts when product_df is None
        assert "product_region_forecasts" not in result

    def test_default_confidence_levels_in_forecasting(
        self, geographic_analytics, mock_spark
    ):
        """Test regional demand forecasting with default confidence levels."""
        # Mock DataFrames
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_df = Mock(spec=DataFrame)
        mock_regional_df = Mock(spec=DataFrame)

        # Setup mocks to capture arguments
        geographic_analytics._prepare_regional_forecast_data = Mock(
            return_value=mock_regional_df
        )
        geographic_analytics._forecast_country_demand = Mock(return_value=Mock())
        geographic_analytics._forecast_region_demand = Mock(return_value=Mock())
        geographic_analytics._calculate_forecast_accuracy = Mock(return_value=Mock())
        geographic_analytics._create_forecast_summary = Mock(return_value=Mock())

        # Test with default confidence levels (None)
        result = geographic_analytics.forecast_regional_demand(
            mock_transaction_df, mock_customer_df, confidence_levels=None  # Use default
        )

        # Verify the forecasting methods were called with default confidence levels
        expected_confidence_levels = [0.80, 0.90, 0.95]
        geographic_analytics._forecast_country_demand.assert_called_once_with(
            mock_regional_df, 30, expected_confidence_levels
        )

    def test_default_features_in_customer_segmentation(
        self, geographic_analytics, mock_spark
    ):
        """Test customer segmentation with default features."""
        # Mock DataFrames
        mock_customer_df = Mock(spec=DataFrame)
        mock_transaction_df = Mock(spec=DataFrame)
        mock_customer_features = Mock(spec=DataFrame)

        # Mock clustering results
        mock_clustering_results = {
            "segmented_customers": Mock(),
            "cluster_centers": [[1, 2, 3], [4, 5, 6]],
            "metrics": {"silhouette_score": 0.75, "n_clusters": 5},
        }

        # Setup mocks to capture arguments
        geographic_analytics._prepare_customer_features = Mock(
            return_value=mock_customer_features
        )
        geographic_analytics._perform_geographic_clustering = Mock(
            return_value=mock_clustering_results
        )
        geographic_analytics._analyze_geographic_clusters = Mock(return_value=Mock())
        geographic_analytics._create_segment_profiles = Mock(return_value=Mock())
        geographic_analytics._compare_segment_performance = Mock(return_value=Mock())

        # Test with default features (None)
        result = geographic_analytics.segment_customers_geographically(
            mock_customer_df, mock_transaction_df, features=None  # Use default
        )

        # Verify the method was called with default features
        expected_features = [
            "total_revenue",
            "transaction_count",
            "avg_order_value",
            "days_since_last_purchase",
            "geographic_distance",
            "seasonal_variance",
            "category_diversity",
        ]
        geographic_analytics._prepare_customer_features.assert_called_once_with(
            mock_customer_df, mock_transaction_df, expected_features
        )

    def test_logging_behavior(self, geographic_analytics, mock_spark):
        """Test that appropriate log messages are generated."""
        with patch(
            "src.analytics.business_intelligence.geographic_analytics.logging"
        ) as mock_logging:
            mock_logger = Mock()
            mock_logging.getLogger.return_value = mock_logger

            # Create new instance to test logging
            with patch(
                "src.analytics.business_intelligence.geographic_analytics.SPARK_AVAILABLE",
                True,
            ):
                geo_analytics = GeographicAnalytics(spark=mock_spark)

                # Verify logger was configured
                mock_logging.getLogger.assert_called_with(
                    "src.analytics.business_intelligence.geographic_analytics"
                )


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])

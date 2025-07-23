"""
Tests for revenue analytics engine.

Comprehensive test suite covering multi-dimensional revenue analysis, forecasting,
profit margin analysis, and trend analysis capabilities.
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

from src.analytics.business_intelligence.revenue_analytics import RevenueAnalytics


class TestRevenueAnalytics:
    """Test cases for RevenueAnalytics."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = MagicMock()
        spark.createDataFrame.return_value = MagicMock()
        return spark

    @pytest.fixture
    def revenue_analytics(self, mock_spark):
        """Create revenue analytics instance."""
        return RevenueAnalytics(mock_spark)

    @pytest.fixture
    def sample_transactions_df(self, mock_spark):
        """Sample transaction DataFrame."""
        df = MagicMock()
        df.columns = [
            "transaction_id",
            "user_id",
            "product_id",
            "price",
            "quantity",
            "timestamp",
            "customer_segment",
            "product_category",
            "geographic_region",
            "sales_channel",
            "payment_method",
            "device_type",
        ]

        # Mock common DataFrame operations
        df.withColumn.return_value = df
        df.filter.return_value = df
        df.groupBy.return_value.agg.return_value = df
        df.orderBy.return_value = df
        df.limit.return_value = df
        df.count.return_value = 1000
        df.collect.return_value = [MagicMock()]

        return df

    def test_initialization(self, mock_spark):
        """Test RevenueAnalytics initialization."""
        analytics = RevenueAnalytics(mock_spark)

        assert analytics.spark == mock_spark
        assert analytics.config is not None
        assert "revenue_dimensions" in analytics.config
        assert "forecasting" in analytics.config
        assert "profit_margins" in analytics.config
        assert "trend_analysis" in analytics.config

    def test_default_config(self, revenue_analytics):
        """Test default configuration values."""
        config = revenue_analytics.config

        # Test revenue dimensions
        expected_dimensions = [
            "customer_segment",
            "product_category",
            "geographic_region",
            "sales_channel",
            "payment_method",
            "device_type",
        ]
        assert config["revenue_dimensions"] == expected_dimensions

        # Test forecasting config
        assert config["forecasting"]["default_horizon_days"] == 30
        assert config["forecasting"]["min_historical_days"] == 90
        assert len(config["forecasting"]["confidence_levels"]) == 3

        # Test profit margins config
        assert config["profit_margins"]["cost_of_goods_ratio"] == 0.65
        assert config["profit_margins"]["shipping_cost_ratio"] == 0.08

        # Test trend analysis config
        assert len(config["trend_analysis"]["moving_average_windows"]) == 4
        assert config["trend_analysis"]["anomaly_threshold_std"] == 2.0

    def test_multi_dimensional_revenue_analysis(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test multi-dimensional revenue analysis."""
        # Mock the internal methods
        with patch.object(
            revenue_analytics, "_filter_by_date_range"
        ) as mock_filter, patch.object(
            revenue_analytics, "_add_time_dimension"
        ) as mock_time_dim, patch.object(
            revenue_analytics, "_calculate_base_revenue_metrics"
        ) as mock_base, patch.object(
            revenue_analytics, "_analyze_overall_revenue"
        ) as mock_overall, patch.object(
            revenue_analytics, "_analyze_revenue_by_dimension"
        ) as mock_dimension, patch.object(
            revenue_analytics, "_analyze_cross_dimensional_revenue"
        ) as mock_cross, patch.object(
            revenue_analytics, "_analyze_revenue_concentration"
        ) as mock_concentration:
            mock_filter.return_value = sample_transactions_df
            mock_time_dim.return_value = sample_transactions_df
            mock_base.return_value = sample_transactions_df
            mock_overall.return_value = MagicMock()
            mock_dimension.return_value = MagicMock()
            mock_cross.return_value = MagicMock()
            mock_concentration.return_value = {}

            result = revenue_analytics.analyze_multi_dimensional_revenue(
                sample_transactions_df,
                dimensions=["customer_segment", "product_category"],
                time_period="day",
            )

            assert "overall" in result
            assert "customer_segment" in result
            assert "product_category" in result
            assert "cross_dimensional" in result
            assert "concentration" in result

            mock_base.assert_called_once()
            mock_overall.assert_called_once()
            assert mock_dimension.call_count == 2  # Called for each dimension

    def test_revenue_forecasting(self, revenue_analytics, sample_transactions_df):
        """Test revenue forecasting functionality."""
        with patch.object(
            revenue_analytics, "_prepare_time_series_data"
        ) as mock_prep, patch.object(
            revenue_analytics, "_engineer_forecasting_features"
        ) as mock_features, patch.object(
            revenue_analytics, "_train_forecasting_model"
        ) as mock_train, patch.object(
            revenue_analytics, "_generate_forecasts"
        ) as mock_forecast, patch.object(
            revenue_analytics, "_calculate_prediction_intervals"
        ) as mock_intervals, patch.object(
            revenue_analytics, "_detect_seasonality"
        ) as mock_seasonality:
            # Mock the time series data to have sufficient historical data
            mock_time_series = MagicMock()
            mock_time_series.count.return_value = 120  # 120 days of data
            mock_prep.return_value = mock_time_series

            mock_features.return_value = MagicMock()
            mock_train.return_value = (MagicMock(), {"rmse": 100.0, "r2": 0.85})
            mock_forecast.return_value = MagicMock()
            mock_intervals.return_value = MagicMock()
            mock_seasonality.return_value = {"seasonality_detected": True}

            result = revenue_analytics.forecast_revenue(
                sample_transactions_df, horizon_days=30, model_type="linear_regression"
            )

            assert "forecasts" in result
            assert "prediction_intervals" in result
            assert "model_metrics" in result
            assert "seasonality_patterns" in result
            assert result["model_type"] == "linear_regression"
            assert result["horizon_days"] == 30

            mock_prep.assert_called_once()
            mock_train.assert_called_once()

    def test_insufficient_historical_data_error(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test error handling for insufficient historical data."""
        with patch.object(revenue_analytics, "_prepare_time_series_data") as mock_prep:
            # Mock insufficient data
            mock_time_series = MagicMock()
            mock_time_series.count.return_value = 30  # Only 30 days
            mock_prep.return_value = mock_time_series

            with pytest.raises(ValueError, match="Insufficient historical data"):
                revenue_analytics.forecast_revenue(sample_transactions_df)

    def test_profit_margin_analysis(self, revenue_analytics, sample_transactions_df):
        """Test profit margin analysis."""
        with patch.object(
            revenue_analytics, "_calculate_base_revenue_metrics"
        ) as mock_base, patch.object(
            revenue_analytics, "_add_cost_information"
        ) as mock_costs, patch.object(
            revenue_analytics, "_add_operational_costs"
        ) as mock_ops, patch.object(
            revenue_analytics, "_add_marketing_costs"
        ) as mock_marketing, patch.object(
            revenue_analytics, "_calculate_margin_rankings"
        ) as mock_rankings, patch.object(
            revenue_analytics, "_categorize_profitability"
        ) as mock_categorize:
            # Setup mock chain
            mock_df = MagicMock()
            mock_df.withColumn.return_value = mock_df

            mock_base.return_value = mock_df
            mock_costs.return_value = mock_df
            mock_ops.return_value = mock_df
            mock_marketing.return_value = mock_df
            mock_rankings.return_value = mock_df
            mock_categorize.return_value = mock_df

            result = revenue_analytics.analyze_profit_margins(
                sample_transactions_df, include_marketing_costs=True
            )

            assert result == mock_df
            mock_base.assert_called_once()
            mock_costs.assert_called_once()
            mock_ops.assert_called_once()
            mock_marketing.assert_called_once()

    def test_profit_margin_analysis_without_marketing(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test profit margin analysis without marketing costs."""
        with patch.object(
            revenue_analytics, "_calculate_base_revenue_metrics"
        ) as mock_base, patch.object(
            revenue_analytics, "_add_cost_information"
        ) as mock_costs, patch.object(
            revenue_analytics, "_add_operational_costs"
        ) as mock_ops, patch.object(
            revenue_analytics, "_add_marketing_costs"
        ) as mock_marketing, patch.object(
            revenue_analytics, "_calculate_margin_rankings"
        ) as mock_rankings, patch.object(
            revenue_analytics, "_categorize_profitability"
        ) as mock_categorize:
            mock_df = MagicMock()
            mock_df.withColumn.return_value = mock_df

            mock_base.return_value = mock_df
            mock_costs.return_value = mock_df
            mock_ops.return_value = mock_df
            mock_rankings.return_value = mock_df
            mock_categorize.return_value = mock_df

            result = revenue_analytics.analyze_profit_margins(
                sample_transactions_df, include_marketing_costs=False
            )

            mock_marketing.assert_not_called()

    def test_revenue_trend_analysis(self, revenue_analytics, sample_transactions_df):
        """Test revenue trend analysis."""
        with patch.object(
            revenue_analytics, "_prepare_time_series_data"
        ) as mock_prep, patch.object(
            revenue_analytics, "_calculate_moving_averages"
        ) as mock_ma, patch.object(
            revenue_analytics, "_calculate_growth_rates"
        ) as mock_growth, patch.object(
            revenue_analytics, "_analyze_trend_direction"
        ) as mock_direction, patch.object(
            revenue_analytics, "_analyze_seasonal_patterns"
        ) as mock_seasonal, patch.object(
            revenue_analytics, "_detect_revenue_anomalies"
        ) as mock_anomalies, patch.object(
            revenue_analytics, "_calculate_trend_statistics"
        ) as mock_stats, patch.object(
            revenue_analytics, "forecast_revenue"
        ) as mock_forecast:
            mock_df = MagicMock()
            mock_prep.return_value = mock_df
            mock_ma.return_value = mock_df
            mock_growth.return_value = mock_df
            mock_direction.return_value = {"direction": "strong_growth"}
            mock_seasonal.return_value = {"seasonality_detected": True}
            mock_anomalies.return_value = mock_df
            mock_stats.return_value = {"avg_revenue": 1000.0}
            mock_forecast.return_value = {"forecasts": "mock_forecasts"}

            result = revenue_analytics.analyze_revenue_trends(
                sample_transactions_df,
                dimension="product_category",
                include_forecasts=True,
            )

            assert "trend_data" in result
            assert "trend_direction" in result
            assert "seasonal_patterns" in result
            assert "anomalies" in result
            assert "statistics" in result
            assert "forecasts" in result
            assert "analysis_date" in result

            mock_prep.assert_called_once()
            mock_forecast.assert_called_once()

    def test_revenue_trend_analysis_without_forecasts(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test revenue trend analysis without forecasts."""
        with patch.object(
            revenue_analytics, "_prepare_time_series_data"
        ) as mock_prep, patch.object(
            revenue_analytics, "_calculate_moving_averages"
        ) as mock_ma, patch.object(
            revenue_analytics, "_calculate_growth_rates"
        ) as mock_growth, patch.object(
            revenue_analytics, "_analyze_trend_direction"
        ) as mock_direction, patch.object(
            revenue_analytics, "_analyze_seasonal_patterns"
        ) as mock_seasonal, patch.object(
            revenue_analytics, "_detect_revenue_anomalies"
        ) as mock_anomalies, patch.object(
            revenue_analytics, "_calculate_trend_statistics"
        ) as mock_stats, patch.object(
            revenue_analytics, "forecast_revenue"
        ) as mock_forecast:
            mock_df = MagicMock()
            mock_prep.return_value = mock_df
            mock_ma.return_value = mock_df
            mock_growth.return_value = mock_df
            mock_direction.return_value = {"direction": "moderate_growth"}
            mock_seasonal.return_value = {"seasonality_detected": False}
            mock_anomalies.return_value = mock_df
            mock_stats.return_value = {"avg_revenue": 500.0}

            result = revenue_analytics.analyze_revenue_trends(
                sample_transactions_df, include_forecasts=False
            )

            assert "forecasts" not in result
            mock_forecast.assert_not_called()

    def test_filter_by_date_range(self, revenue_analytics, sample_transactions_df):
        """Test date range filtering."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)

        result = revenue_analytics._filter_by_date_range(
            sample_transactions_df, start_date, end_date
        )

        # Should call filter twice (for start and end date)
        assert sample_transactions_df.filter.call_count == 2

    def test_add_time_dimension(self, revenue_analytics, sample_transactions_df):
        """Test time dimension addition."""
        # Test different time periods
        time_periods = ["hour", "day", "week", "month"]

        for period in time_periods:
            result = revenue_analytics._add_time_dimension(
                sample_transactions_df, period
            )
            sample_transactions_df.withColumn.assert_called()

    def test_add_time_dimension_invalid_period(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test error handling for invalid time period."""
        with pytest.raises(ValueError, match="Unsupported time period"):
            revenue_analytics._add_time_dimension(sample_transactions_df, "invalid")

    def test_calculate_base_revenue_metrics(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test base revenue metrics calculation."""
        result = revenue_analytics._calculate_base_revenue_metrics(
            sample_transactions_df
        )

        # Should add revenue and transaction_count columns
        assert sample_transactions_df.withColumn.call_count >= 2

    def test_analyze_overall_revenue(self, revenue_analytics, sample_transactions_df):
        """Test overall revenue analysis."""
        result = revenue_analytics._analyze_overall_revenue(
            sample_transactions_df, "day"
        )

        sample_transactions_df.groupBy.assert_called_with("time_dimension")

    def test_analyze_revenue_by_dimension(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test revenue analysis by dimension."""
        result = revenue_analytics._analyze_revenue_by_dimension(
            sample_transactions_df, "customer_segment", "day"
        )

        sample_transactions_df.groupBy.assert_called_with(
            "time_dimension", "customer_segment"
        )

    def test_unsupported_model_type_error(self, revenue_analytics):
        """Test error for unsupported forecasting model type."""
        mock_df = MagicMock()

        with pytest.raises(ValueError, match="Unsupported model type"):
            revenue_analytics._train_forecasting_model(mock_df, "unsupported_model")

    def test_error_handling_in_multi_dimensional_analysis(self, revenue_analytics):
        """Test error handling in multi-dimensional analysis."""
        with patch.object(
            revenue_analytics, "_calculate_base_revenue_metrics"
        ) as mock_base:
            mock_base.side_effect = Exception("Test error")

            with pytest.raises(Exception, match="Test error"):
                revenue_analytics.analyze_multi_dimensional_revenue(MagicMock())

    def test_error_handling_in_forecasting(self, revenue_analytics):
        """Test error handling in revenue forecasting."""
        with patch.object(revenue_analytics, "_prepare_time_series_data") as mock_prep:
            mock_prep.side_effect = Exception("Forecasting error")

            with pytest.raises(Exception, match="Forecasting error"):
                revenue_analytics.forecast_revenue(MagicMock())

    def test_error_handling_in_profit_margins(self, revenue_analytics):
        """Test error handling in profit margin analysis."""
        with patch.object(
            revenue_analytics, "_calculate_base_revenue_metrics"
        ) as mock_base:
            mock_base.side_effect = Exception("Margin error")

            with pytest.raises(Exception, match="Margin error"):
                revenue_analytics.analyze_profit_margins(MagicMock())

    def test_error_handling_in_trend_analysis(self, revenue_analytics):
        """Test error handling in trend analysis."""
        with patch.object(revenue_analytics, "_prepare_time_series_data") as mock_prep:
            mock_prep.side_effect = Exception("Trend error")

            with pytest.raises(Exception, match="Trend error"):
                revenue_analytics.analyze_revenue_trends(MagicMock())

    def test_revenue_concentration_analysis(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test revenue concentration analysis."""
        # Mock the DataFrame operations for concentration analysis
        mock_grouped = MagicMock()
        mock_ordered = MagicMock()
        mock_windowed = MagicMock()

        sample_transactions_df.groupBy.return_value.agg.return_value = mock_grouped
        mock_grouped.orderBy.return_value = mock_ordered
        mock_ordered.withColumn.return_value = mock_windowed
        mock_windowed.withColumn.return_value = mock_windowed
        mock_windowed.agg.return_value.collect.return_value = [MagicMock()]
        mock_windowed.agg.return_value.collect.return_value[
            0
        ].__getitem__.return_value = 10000

        dimensions = ["customer_segment", "product_category"]
        result = revenue_analytics._analyze_revenue_concentration(
            sample_transactions_df, dimensions
        )

        assert isinstance(result, dict)
        # Should analyze each dimension that exists in the DataFrame
        for dimension in dimensions:
            if dimension in sample_transactions_df.columns:
                assert dimension in result

    def test_seasonality_detection(self, revenue_analytics):
        """Test seasonality detection."""
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.groupBy.return_value.agg.return_value.orderBy.return_value = MagicMock()

        result = revenue_analytics._detect_seasonality(mock_df)

        assert "day_of_week_patterns" in result
        assert "monthly_patterns" in result
        assert "seasonality_detected" in result

    def test_margin_calculations_with_cost_data(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test margin calculations with actual cost data."""
        cost_data_df = MagicMock()
        cost_data_df.columns = ["product_id", "actual_cost"]

        result = revenue_analytics._add_cost_information(
            sample_transactions_df, cost_data_df
        )

        # Should perform a join with cost data
        sample_transactions_df.join.assert_called_once_with(
            cost_data_df, "product_id", "left"
        )

    def test_margin_calculations_without_cost_data(
        self, revenue_analytics, sample_transactions_df
    ):
        """Test margin calculations without cost data (using defaults)."""
        result = revenue_analytics._add_cost_information(sample_transactions_df, None)

        # Should add cost_of_goods column using default ratio
        sample_transactions_df.withColumn.assert_called()


if __name__ == "__main__":
    pytest.main([__file__])

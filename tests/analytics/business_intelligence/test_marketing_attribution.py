"""
Test suite for marketing attribution engine.

This test suite validates the MarketingAttribution class functionality including
multi-touch attribution models, campaign performance tracking, customer acquisition
cost analysis, and marketing ROI calculations.
"""

from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics.business_intelligence.marketing_attribution import (
    MarketingAttribution,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return (
        SparkSession.builder.appName("MarketingAttributionTest")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def marketing_attribution(spark):
    """Create MarketingAttribution instance for testing."""
    config = {
        "attribution_models": {
            "first_touch_weight": 1.0,
            "last_touch_weight": 1.0,
            "linear_distribution": "equal",
            "time_decay_half_life_days": 7,
            "position_based_first_last_weight": 0.4,
            "position_based_middle_weight": 0.2,
        },
        "campaign_tracking": {
            "performance_metrics": [
                "impressions",
                "clicks",
                "conversions",
                "revenue",
                "cost",
                "ctr",
                "cvr",
                "cpc",
                "cpa",
                "roas",
            ],
            "attribution_window_days": 30,
            "budget_tracking": True,
        },
        "customer_acquisition": {
            "cac_calculation_methods": ["simple", "attributed", "cohort"],
            "ltv_calculation_enabled": True,
            "payback_period_enabled": True,
            "cohort_analysis_periods": [30, 60, 90, 180, 365],
        },
        "roi_analysis": {
            "attribution_required": True,
            "time_periods": ["day", "week", "month", "quarter"],
            "incremental_analysis": True,
            "budget_optimization": True,
        },
    }
    return MarketingAttribution(spark, config)


@pytest.fixture
def sample_touchpoint_data(spark):
    """Create sample touchpoint data for testing."""
    schema = StructType(
        [
            StructField("touchpoint_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("campaign_id", StringType(), False),
            StructField("campaign_name", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("cost", DecimalType(10, 2), True),
            StructField("touchpoint_type", StringType(), False),
        ]
    )

    data = [
        (
            "tp_001",
            "user_001",
            "paid_search",
            "camp_001",
            "Search Campaign A",
            datetime(2024, 1, 1, 10, 0),
            Decimal("5.50"),
            "click",
        ),
        (
            "tp_002",
            "user_001",
            "social",
            "camp_002",
            "Social Campaign B",
            datetime(2024, 1, 2, 14, 30),
            Decimal("3.25"),
            "click",
        ),
        (
            "tp_003",
            "user_001",
            "email",
            "camp_003",
            "Email Campaign C",
            datetime(2024, 1, 3, 9, 15),
            Decimal("0.75"),
            "click",
        ),
        (
            "tp_004",
            "user_002",
            "paid_search",
            "camp_001",
            "Search Campaign A",
            datetime(2024, 1, 1, 11, 30),
            Decimal("4.80"),
            "click",
        ),
        (
            "tp_005",
            "user_002",
            "display",
            "camp_004",
            "Display Campaign D",
            datetime(2024, 1, 4, 16, 45),
            Decimal("2.10"),
            "view",
        ),
        (
            "tp_006",
            "user_003",
            "organic",
            "camp_005",
            "SEO Campaign",
            datetime(2024, 1, 2, 8, 20),
            Decimal("0.00"),
            "click",
        ),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_conversion_data(spark):
    """Create sample conversion data for testing."""
    schema = StructType(
        [
            StructField("conversion_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("conversion_timestamp", TimestampType(), False),
            StructField("conversion_value", DecimalType(10, 2), False),
            StructField("conversion_type", StringType(), False),
            StructField("order_id", StringType(), True),
        ]
    )

    data = [
        (
            "conv_001",
            "user_001",
            datetime(2024, 1, 4, 12, 0),
            Decimal("150.00"),
            "purchase",
            "order_001",
        ),
        (
            "conv_002",
            "user_002",
            datetime(2024, 1, 5, 15, 30),
            Decimal("89.99"),
            "purchase",
            "order_002",
        ),
        (
            "conv_003",
            "user_003",
            datetime(2024, 1, 3, 11, 45),
            Decimal("75.50"),
            "purchase",
            "order_003",
        ),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_campaign_data(spark):
    """Create sample campaign data for testing."""
    schema = StructType(
        [
            StructField("campaign_id", StringType(), False),
            StructField("campaign_name", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("start_date", TimestampType(), False),
            StructField("end_date", TimestampType(), True),
            StructField("budget", DecimalType(10, 2), False),
            StructField("daily_budget", DecimalType(10, 2), True),
            StructField("target_audience", StringType(), True),
            StructField("campaign_objective", StringType(), False),
        ]
    )

    data = [
        (
            "camp_001",
            "Search Campaign A",
            "paid_search",
            datetime(2024, 1, 1),
            datetime(2024, 1, 31),
            Decimal("5000.00"),
            Decimal("161.29"),
            "broad",
            "conversions",
        ),
        (
            "camp_002",
            "Social Campaign B",
            "social",
            datetime(2024, 1, 1),
            datetime(2024, 1, 31),
            Decimal("3000.00"),
            Decimal("96.77"),
            "lookalike",
            "awareness",
        ),
        (
            "camp_003",
            "Email Campaign C",
            "email",
            datetime(2024, 1, 1),
            datetime(2024, 1, 31),
            Decimal("500.00"),
            Decimal("16.13"),
            "existing_customers",
            "retention",
        ),
        (
            "camp_004",
            "Display Campaign D",
            "display",
            datetime(2024, 1, 1),
            datetime(2024, 1, 31),
            Decimal("2000.00"),
            Decimal("64.52"),
            "retargeting",
            "conversions",
        ),
        (
            "camp_005",
            "SEO Campaign",
            "organic",
            datetime(2024, 1, 1),
            None,
            Decimal("0.00"),
            None,
            "organic",
            "awareness",
        ),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_marketing_spend_data(spark):
    """Create sample marketing spend data for testing."""
    schema = StructType(
        [
            StructField("spend_date", TimestampType(), False),
            StructField("campaign_id", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("spend_amount", DecimalType(10, 2), False),
            StructField("impressions", IntegerType(), True),
            StructField("clicks", IntegerType(), True),
            StructField("spend_type", StringType(), False),
        ]
    )

    data = [
        (
            datetime(2024, 1, 1),
            "camp_001",
            "paid_search",
            Decimal("200.00"),
            10000,
            500,
            "media",
        ),
        (
            datetime(2024, 1, 2),
            "camp_001",
            "paid_search",
            Decimal("180.50"),
            9500,
            475,
            "media",
        ),
        (
            datetime(2024, 1, 1),
            "camp_002",
            "social",
            Decimal("150.00"),
            20000,
            800,
            "media",
        ),
        (
            datetime(2024, 1, 2),
            "camp_002",
            "social",
            Decimal("165.75"),
            22000,
            880,
            "media",
        ),
        (
            datetime(2024, 1, 1),
            "camp_003",
            "email",
            Decimal("25.00"),
            5000,
            250,
            "media",
        ),
        (
            datetime(2024, 1, 2),
            "camp_003",
            "email",
            Decimal("22.50"),
            4800,
            240,
            "media",
        ),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_acquisition_data(spark):
    """Create sample customer acquisition data for testing."""
    schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("acquisition_date", TimestampType(), False),
            StructField("acquisition_channel", StringType(), False),
            StructField("acquisition_campaign", StringType(), True),
            StructField("first_purchase_value", DecimalType(10, 2), True),
            StructField("customer_segment", StringType(), True),
        ]
    )

    data = [
        (
            "user_001",
            datetime(2024, 1, 4),
            "paid_search",
            "camp_001",
            Decimal("150.00"),
            "high_value",
        ),
        (
            "user_002",
            datetime(2024, 1, 5),
            "paid_search",
            "camp_001",
            Decimal("89.99"),
            "medium_value",
        ),
        (
            "user_003",
            datetime(2024, 1, 3),
            "organic",
            "camp_005",
            Decimal("75.50"),
            "medium_value",
        ),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_revenue_data(spark):
    """Create sample revenue data for testing."""
    schema = StructType(
        [
            StructField("date", TimestampType(), False),
            StructField("user_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("revenue", DecimalType(10, 2), False),
            StructField("channel", StringType(), True),
            StructField("campaign_id", StringType(), True),
        ]
    )

    data = [
        (
            datetime(2024, 1, 4),
            "user_001",
            "order_001",
            Decimal("150.00"),
            "paid_search",
            "camp_001",
        ),
        (
            datetime(2024, 1, 5),
            "user_002",
            "order_002",
            Decimal("89.99"),
            "paid_search",
            "camp_001",
        ),
        (
            datetime(2024, 1, 3),
            "user_003",
            "order_003",
            Decimal("75.50"),
            "organic",
            "camp_005",
        ),
        (
            datetime(2024, 1, 8),
            "user_001",
            "order_004",
            Decimal("200.00"),
            "social",
            "camp_002",
        ),
    ]

    return spark.createDataFrame(data, schema)


class TestMarketingAttributionInitialization:
    """Test MarketingAttribution initialization and configuration."""

    def test_initialization_with_default_config(self, spark):
        """Test initialization with default configuration."""
        attribution = MarketingAttribution(spark)

        assert attribution.spark == spark
        assert attribution.config is not None
        assert "attribution_models" in attribution.config
        assert "campaign_tracking" in attribution.config
        assert "customer_acquisition" in attribution.config
        assert "roi_analysis" in attribution.config

    def test_initialization_with_custom_config(self, spark):
        """Test initialization with custom configuration."""
        custom_config = {
            "attribution_models": {
                "first_touch_weight": 0.8,
                "last_touch_weight": 0.8,
            }
        }

        attribution = MarketingAttribution(spark, custom_config)

        assert attribution.config == custom_config

    def test_logger_initialization(self, marketing_attribution):
        """Test that logger is properly initialized."""
        assert marketing_attribution.logger is not None
        assert (
            marketing_attribution.logger.name
            == "src.analytics.business_intelligence.marketing_attribution"
        )


class TestMultiTouchAttribution:
    """Test multi-touch attribution functionality."""

    def test_analyze_multi_touch_attribution_linear_model(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test linear attribution model analysis."""
        result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data, sample_conversion_data, attribution_model="linear"
        )

        assert isinstance(result, dict)
        assert "attribution_results" in result
        assert "model_summary" in result
        assert "attribution_paths" in result

        attribution_df = result["attribution_results"]
        assert attribution_df.count() > 0

        # Check that attribution credits sum to 1.0 for each conversion
        attribution_summary = (
            attribution_df.groupBy("user_id")
            .agg(F.sum("attribution_credit").alias("total_credit"))
            .collect()
        )

        for row in attribution_summary:
            assert (
                abs(row["total_credit"] - 1.0) < 0.001
            )  # Allow for floating point precision

    def test_analyze_multi_touch_attribution_first_touch_model(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test first touch attribution model analysis."""
        result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data,
            sample_conversion_data,
            attribution_model="first_touch",
        )

        attribution_df = result["attribution_results"]

        # In first touch model, only the first touchpoint should get credit
        user_001_attributions = attribution_df.filter(
            F.col("user_id") == "user_001"
        ).collect()
        first_touchpoint = min(
            user_001_attributions, key=lambda x: x["touchpoint_timestamp"]
        )

        for row in user_001_attributions:
            if row["touchpoint_id"] == first_touchpoint["touchpoint_id"]:
                assert row["attribution_credit"] == 1.0
            else:
                assert row["attribution_credit"] == 0.0

    def test_analyze_multi_touch_attribution_last_touch_model(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test last touch attribution model analysis."""
        result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data,
            sample_conversion_data,
            attribution_model="last_touch",
        )

        attribution_df = result["attribution_results"]

        # In last touch model, only the last touchpoint should get credit
        user_001_attributions = attribution_df.filter(
            F.col("user_id") == "user_001"
        ).collect()
        last_touchpoint = max(
            user_001_attributions, key=lambda x: x["touchpoint_timestamp"]
        )

        for row in user_001_attributions:
            if row["touchpoint_id"] == last_touchpoint["touchpoint_id"]:
                assert row["attribution_credit"] == 1.0
            else:
                assert row["attribution_credit"] == 0.0

    def test_analyze_multi_touch_attribution_time_decay_model(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test time decay attribution model analysis."""
        result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data,
            sample_conversion_data,
            attribution_model="time_decay",
        )

        attribution_df = result["attribution_results"]

        # In time decay model, more recent touchpoints should have higher weights
        user_001_attributions = (
            attribution_df.filter(F.col("user_id") == "user_001")
            .orderBy("touchpoint_timestamp")
            .collect()
        )

        # Check that weights are increasing over time (more recent = higher weight)
        for i in range(1, len(user_001_attributions)):
            assert (
                user_001_attributions[i]["attribution_credit"]
                >= user_001_attributions[i - 1]["attribution_credit"]
            )

    def test_analyze_multi_touch_attribution_position_based_model(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test position-based attribution model analysis."""
        result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data,
            sample_conversion_data,
            attribution_model="position_based",
        )

        attribution_df = result["attribution_results"]

        # In position-based model, first and last touchpoints should get more credit
        user_001_attributions = (
            attribution_df.filter(F.col("user_id") == "user_001")
            .orderBy("touchpoint_timestamp")
            .collect()
        )

        if len(user_001_attributions) >= 3:
            first_credit = user_001_attributions[0]["attribution_credit"]
            last_credit = user_001_attributions[-1]["attribution_credit"]
            middle_credit = user_001_attributions[1]["attribution_credit"]

            assert first_credit > middle_credit
            assert last_credit > middle_credit
            assert (
                abs(first_credit - last_credit) < 0.001
            )  # First and last should be equal

    def test_analyze_multi_touch_attribution_with_window(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test attribution analysis with attribution window."""
        result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data,
            sample_conversion_data,
            attribution_model="linear",
            attribution_window_days=2,
        )

        attribution_df = result["attribution_results"]

        # Check that only touchpoints within the window are included
        for row in attribution_df.collect():
            touchpoint_time = row["touchpoint_timestamp"]
            conversion_time = row["conversion_timestamp"]
            time_diff = (conversion_time - touchpoint_time).days
            assert time_diff <= 2

    def test_invalid_attribution_model(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test handling of invalid attribution model."""
        with pytest.raises(ValueError, match="Unsupported attribution model"):
            marketing_attribution.analyze_multi_touch_attribution(
                sample_touchpoint_data,
                sample_conversion_data,
                attribution_model="invalid_model",
            )


class TestCampaignPerformanceTracking:
    """Test campaign performance tracking functionality."""

    def test_track_campaign_performance_basic(
        self,
        marketing_attribution,
        sample_campaign_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test basic campaign performance tracking."""
        result = marketing_attribution.track_campaign_performance(
            sample_campaign_data, sample_touchpoint_data, sample_conversion_data
        )

        assert isinstance(result, dict)
        assert "campaign_metrics" in result
        assert "channel_performance" in result
        assert "time_series_performance" in result

        campaign_metrics = result["campaign_metrics"]
        assert campaign_metrics.count() > 0

        # Check that essential metrics are present
        columns = campaign_metrics.columns
        assert "campaign_id" in columns
        assert "total_conversions" in columns
        assert "total_revenue" in columns
        assert "conversion_rate" in columns

    def test_track_campaign_performance_monthly_aggregation(
        self,
        marketing_attribution,
        sample_campaign_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test campaign performance tracking with monthly aggregation."""
        result = marketing_attribution.track_campaign_performance(
            sample_campaign_data,
            sample_touchpoint_data,
            sample_conversion_data,
            time_period="month",
        )

        time_series_df = result["time_series_performance"]

        # Check that time dimension is monthly
        time_series_data = time_series_df.collect()
        for row in time_series_data:
            time_dim = row["time_dimension"]
            # Should be in format YYYY-MM-01 for monthly aggregation
            assert time_dim.day == 1

    def test_campaign_performance_metrics_calculation(
        self,
        marketing_attribution,
        sample_campaign_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test that campaign performance metrics are calculated correctly."""
        result = marketing_attribution.track_campaign_performance(
            sample_campaign_data, sample_touchpoint_data, sample_conversion_data
        )

        campaign_metrics = result["campaign_metrics"]

        for row in campaign_metrics.collect():
            # Test that rates are within valid ranges
            if row["conversion_rate"] is not None:
                assert 0 <= row["conversion_rate"] <= 100
            if row["click_through_rate"] is not None:
                assert 0 <= row["click_through_rate"] <= 100

            # Test that costs are non-negative
            if row["total_cost"] is not None:
                assert row["total_cost"] >= 0

            # Test that revenue is non-negative
            if row["total_revenue"] is not None:
                assert row["total_revenue"] >= 0


class TestCustomerAcquisitionCost:
    """Test customer acquisition cost analysis functionality."""

    def test_calculate_customer_acquisition_cost_simple(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_acquisition_data,
    ):
        """Test simple CAC calculation without attribution."""
        result = marketing_attribution.calculate_customer_acquisition_cost(
            sample_marketing_spend_data, sample_acquisition_data
        )

        assert isinstance(result, dict)
        assert "cac_by_channel" in result
        assert "cac_by_campaign" in result
        assert "cac_summary" in result

        cac_by_channel = result["cac_by_channel"]
        assert cac_by_channel.count() > 0

        # Check that CAC values are reasonable
        for row in cac_by_channel.collect():
            assert row["customer_acquisition_cost"] > 0
            assert row["total_customers"] > 0

    def test_calculate_customer_acquisition_cost_with_attribution(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_acquisition_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test CAC calculation with attribution data."""
        # First create attribution data
        attribution_result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data, sample_conversion_data
        )
        attribution_df = attribution_result["attribution_results"]

        result = marketing_attribution.calculate_customer_acquisition_cost(
            sample_marketing_spend_data, sample_acquisition_data, attribution_df
        )

        assert "attributed_cac" in result
        attributed_cac = result["attributed_cac"]
        assert attributed_cac.count() > 0

    def test_calculate_cac_cohort_analysis(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_acquisition_data,
    ):
        """Test CAC cohort analysis functionality."""
        result = marketing_attribution.calculate_customer_acquisition_cost(
            sample_marketing_spend_data, sample_acquisition_data
        )

        assert "cohort_analysis" in result
        cohort_analysis = result["cohort_analysis"]

        # Check cohort analysis structure
        for row in cohort_analysis.collect():
            assert "acquisition_cohort" in row.asDict()
            assert "customers_acquired" in row.asDict()
            assert "average_cac" in row.asDict()

    def test_cac_ltv_ratio_calculation(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_acquisition_data,
    ):
        """Test CAC to LTV ratio calculation."""
        result = marketing_attribution.calculate_customer_acquisition_cost(
            sample_marketing_spend_data, sample_acquisition_data
        )

        cac_summary = result["cac_summary"]

        # Check that LTV ratios are present and reasonable
        for row in cac_summary.collect():
            if row["ltv_cac_ratio"] is not None:
                assert row["ltv_cac_ratio"] > 0  # Ratio should be positive

    def test_payback_period_calculation(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_acquisition_data,
    ):
        """Test payback period calculation."""
        result = marketing_attribution.calculate_customer_acquisition_cost(
            sample_marketing_spend_data, sample_acquisition_data
        )

        cac_summary = result["cac_summary"]

        # Check that payback periods are calculated
        for row in cac_summary.collect():
            if row["payback_period_days"] is not None:
                assert row["payback_period_days"] > 0  # Should be positive


class TestMarketingROI:
    """Test marketing ROI analysis functionality."""

    def test_calculate_marketing_roi_basic(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_revenue_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test basic marketing ROI calculation."""
        # Create attribution data
        attribution_result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data, sample_conversion_data
        )
        attribution_df = attribution_result["attribution_results"]

        result = marketing_attribution.calculate_marketing_roi(
            sample_marketing_spend_data, sample_revenue_data, attribution_df
        )

        assert isinstance(result, dict)
        assert "roi_by_channel" in result
        assert "roi_by_campaign" in result
        assert "roi_time_series" in result

        roi_by_channel = result["roi_by_channel"]
        assert roi_by_channel.count() > 0

        # Check ROI calculations
        for row in roi_by_channel.collect():
            assert "return_on_ad_spend" in row.asDict()
            assert "roi_percentage" in row.asDict()

    def test_calculate_marketing_roi_monthly_period(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_revenue_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test marketing ROI calculation with monthly time period."""
        attribution_result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data, sample_conversion_data
        )
        attribution_df = attribution_result["attribution_results"]

        result = marketing_attribution.calculate_marketing_roi(
            sample_marketing_spend_data,
            sample_revenue_data,
            attribution_df,
            time_period="month",
        )

        roi_time_series = result["roi_time_series"]

        # Check monthly aggregation
        for row in roi_time_series.collect():
            time_dim = row["time_dimension"]
            assert time_dim.day == 1  # Should be first day of month

    def test_roi_budget_optimization(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_revenue_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test budget optimization recommendations."""
        attribution_result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data, sample_conversion_data
        )
        attribution_df = attribution_result["attribution_results"]

        result = marketing_attribution.calculate_marketing_roi(
            sample_marketing_spend_data, sample_revenue_data, attribution_df
        )

        assert "budget_optimization" in result
        budget_opt = result["budget_optimization"]

        # Check optimization recommendations structure
        for row in budget_opt.collect():
            assert "recommended_budget_allocation" in row.asDict()
            assert "efficiency_score" in row.asDict()

    def test_incremental_roi_analysis(
        self,
        marketing_attribution,
        sample_marketing_spend_data,
        sample_revenue_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test incremental ROI analysis."""
        attribution_result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data, sample_conversion_data
        )
        attribution_df = attribution_result["attribution_results"]

        result = marketing_attribution.calculate_marketing_roi(
            sample_marketing_spend_data, sample_revenue_data, attribution_df
        )

        assert "incremental_analysis" in result
        incremental = result["incremental_analysis"]

        # Check incremental analysis structure
        assert incremental.count() > 0


class TestHelperMethods:
    """Test helper methods and utility functions."""

    def test_filter_by_date_range(self, marketing_attribution, sample_touchpoint_data):
        """Test date range filtering functionality."""
        start_date = datetime(2024, 1, 2)
        end_date = datetime(2024, 1, 3)

        filtered_df = marketing_attribution._filter_by_date_range(
            sample_touchpoint_data, start_date, end_date, "timestamp"
        )

        # Check that all rows are within the date range
        for row in filtered_df.collect():
            assert start_date <= row["timestamp"] <= end_date

    def test_add_time_dimension_day(
        self, marketing_attribution, sample_touchpoint_data
    ):
        """Test adding daily time dimension."""
        result_df = marketing_attribution._add_time_dimension(
            sample_touchpoint_data, "day", "timestamp"
        )

        assert "time_dimension" in result_df.columns

        # Check time dimension format for daily aggregation
        for row in result_df.collect():
            time_dim = row["time_dimension"]
            assert isinstance(time_dim, str)
            # Should be in YYYY-MM-DD format
            datetime.strptime(time_dim, "%Y-%m-%d")

    def test_add_time_dimension_month(
        self, marketing_attribution, sample_touchpoint_data
    ):
        """Test adding monthly time dimension."""
        result_df = marketing_attribution._add_time_dimension(
            sample_touchpoint_data, "month", "timestamp"
        )

        # Check time dimension format for monthly aggregation
        for row in result_df.collect():
            time_dim = row["time_dimension"]
            # Should be in YYYY-MM-01 format (first day of month)
            parsed_date = datetime.strptime(time_dim, "%Y-%m-%d")
            assert parsed_date.day == 1

    def test_calculate_attribution_weights_linear(self, marketing_attribution):
        """Test linear attribution weight calculation."""
        touchpoint_count = 3
        weights = marketing_attribution._calculate_attribution_weights(
            touchpoint_count, "linear"
        )

        assert len(weights) == touchpoint_count
        assert all(abs(w - 1.0 / touchpoint_count) < 0.001 for w in weights)
        assert abs(sum(weights) - 1.0) < 0.001

    def test_calculate_attribution_weights_time_decay(self, marketing_attribution):
        """Test time decay attribution weight calculation."""
        touchpoint_count = 3
        weights = marketing_attribution._calculate_attribution_weights(
            touchpoint_count, "time_decay"
        )

        assert len(weights) == touchpoint_count
        assert abs(sum(weights) - 1.0) < 0.001
        # Later touchpoints should have higher weights
        assert weights[-1] >= weights[0]

    def test_calculate_attribution_weights_position_based(self, marketing_attribution):
        """Test position-based attribution weight calculation."""
        touchpoint_count = 4
        weights = marketing_attribution._calculate_attribution_weights(
            touchpoint_count, "position_based"
        )

        assert len(weights) == touchpoint_count
        assert abs(sum(weights) - 1.0) < 0.001
        # First and last should have higher weights than middle touchpoints
        if touchpoint_count >= 3:
            assert weights[0] > weights[1]
            assert weights[-1] > weights[-2]


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_empty_touchpoint_data(
        self, marketing_attribution, spark, sample_conversion_data
    ):
        """Test handling of empty touchpoint data."""
        empty_touchpoint_df = spark.createDataFrame([], sample_conversion_data.schema)

        with pytest.raises(ValueError, match="No touchpoint data provided"):
            marketing_attribution.analyze_multi_touch_attribution(
                empty_touchpoint_df, sample_conversion_data
            )

    def test_empty_conversion_data(
        self, marketing_attribution, sample_touchpoint_data, spark
    ):
        """Test handling of empty conversion data."""
        empty_conversion_df = spark.createDataFrame([], sample_touchpoint_data.schema)

        with pytest.raises(ValueError, match="No conversion data provided"):
            marketing_attribution.analyze_multi_touch_attribution(
                sample_touchpoint_data, empty_conversion_df
            )

    def test_invalid_time_period(
        self,
        marketing_attribution,
        sample_campaign_data,
        sample_touchpoint_data,
        sample_conversion_data,
    ):
        """Test handling of invalid time period."""
        with pytest.raises(ValueError, match="Unsupported time period"):
            marketing_attribution.track_campaign_performance(
                sample_campaign_data,
                sample_touchpoint_data,
                sample_conversion_data,
                time_period="invalid_period",
            )

    def test_missing_required_columns(
        self, marketing_attribution, spark, sample_conversion_data
    ):
        """Test handling of missing required columns."""
        # Create touchpoint data without required 'user_id' column
        incomplete_schema = StructType(
            [
                StructField("touchpoint_id", StringType(), False),
                StructField("channel", StringType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        )

        incomplete_df = spark.createDataFrame(
            [("tp_001", "paid_search", datetime(2024, 1, 1))], incomplete_schema
        )

        with pytest.raises(ValueError, match="Missing required column"):
            marketing_attribution.analyze_multi_touch_attribution(
                incomplete_df, sample_conversion_data
            )

    def test_negative_attribution_window(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test handling of negative attribution window."""
        with pytest.raises(ValueError, match="Attribution window must be positive"):
            marketing_attribution.analyze_multi_touch_attribution(
                sample_touchpoint_data,
                sample_conversion_data,
                attribution_window_days=-1,
            )


@pytest.mark.integration
class TestIntegrationScenarios:
    """Integration tests for complete marketing attribution workflows."""

    def test_complete_attribution_workflow(
        self,
        marketing_attribution,
        sample_touchpoint_data,
        sample_conversion_data,
        sample_campaign_data,
        sample_marketing_spend_data,
        sample_acquisition_data,
        sample_revenue_data,
    ):
        """Test complete end-to-end attribution workflow."""
        # Step 1: Multi-touch attribution
        attribution_result = marketing_attribution.analyze_multi_touch_attribution(
            sample_touchpoint_data, sample_conversion_data, attribution_model="linear"
        )
        attribution_df = attribution_result["attribution_results"]

        # Step 2: Campaign performance tracking
        campaign_result = marketing_attribution.track_campaign_performance(
            sample_campaign_data, sample_touchpoint_data, sample_conversion_data
        )

        # Step 3: Customer acquisition cost analysis
        cac_result = marketing_attribution.calculate_customer_acquisition_cost(
            sample_marketing_spend_data, sample_acquisition_data, attribution_df
        )

        # Step 4: Marketing ROI analysis
        roi_result = marketing_attribution.calculate_marketing_roi(
            sample_marketing_spend_data, sample_revenue_data, attribution_df
        )

        # Verify all results are generated
        assert attribution_result is not None
        assert campaign_result is not None
        assert cac_result is not None
        assert roi_result is not None

        # Verify data consistency across results
        assert attribution_df.count() > 0
        assert campaign_result["campaign_metrics"].count() > 0
        assert cac_result["cac_by_channel"].count() > 0
        assert roi_result["roi_by_channel"].count() > 0

    def test_multi_model_attribution_comparison(
        self, marketing_attribution, sample_touchpoint_data, sample_conversion_data
    ):
        """Test comparison across different attribution models."""
        models = ["first_touch", "last_touch", "linear", "time_decay", "position_based"]
        results = {}

        for model in models:
            try:
                result = marketing_attribution.analyze_multi_touch_attribution(
                    sample_touchpoint_data,
                    sample_conversion_data,
                    attribution_model=model,
                )
                results[model] = result
            except Exception as e:
                pytest.fail(f"Attribution model {model} failed: {e}")

        # All models should produce results
        assert len(results) == len(models)

        # All results should have the same users but different attribution credits
        user_ids = set()
        for model, result in results.items():
            model_users = set(
                [row["user_id"] for row in result["attribution_results"].collect()]
            )
            if not user_ids:
                user_ids = model_users
            else:
                assert user_ids == model_users, f"Model {model} has different users"

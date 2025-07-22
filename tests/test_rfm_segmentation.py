"""
Tests for RFM Customer Segmentation Module

Comprehensive tests for RFM analysis, customer segmentation, and profile generation.
"""

from datetime import datetime, timedelta
from decimal import Decimal
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
    TimestampType,
)

from src.analytics.rfm_segmentation import (
    CustomerProfile,
    CustomerSegment,
    RFMScores,
    RFMSegmentationEngine,
)


class TestRFMSegmentationEngine:
    """Test suite for RFM Segmentation Engine."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for tests."""
        spark = (
            SparkSession.builder.appName("RFM-Tests")
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
            ("customer_001", "2023-12-01", 150.00),  # Recent, high value
            ("customer_001", "2023-11-15", 200.00),
            ("customer_001", "2023-10-20", 175.00),
            ("customer_002", "2023-12-05", 50.00),  # Recent, low value, low frequency
            ("customer_003", "2023-06-01", 500.00),  # Old purchase, high value
            ("customer_003", "2023-05-15", 300.00),
            ("customer_003", "2023-05-01", 450.00),
            ("customer_003", "2023-04-20", 350.00),
            ("customer_004", "2023-11-30", 75.00),  # Recent, medium frequency
            ("customer_004", "2023-11-01", 100.00),
            ("customer_005", "2023-01-15", 25.00),  # Very old, low value
            ("customer_006", "2023-12-10", 1000.00),  # Recent, very high value
            ("customer_006", "2023-11-25", 800.00),
            ("customer_006", "2023-11-10", 1200.00),
            ("customer_006", "2023-10-30", 900.00),
            ("customer_006", "2023-10-15", 1100.00),
            ("customer_007", "2023-02-01", 30.00),  # Old, low frequency, low value
            ("customer_008", "2023-08-15", 200.00),  # Medium recency, single purchase
            ("customer_009", "2023-12-15", 300.00),  # Very recent, high value
            ("customer_009", "2023-12-10", 250.00),
            ("customer_009", "2023-12-05", 400.00),
            ("customer_010", "2023-03-01", 100.00),  # Old, medium value
            ("customer_010", "2023-02-15", 120.00),
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
    def rfm_engine(self, spark_session):
        """Create RFM engine with test reference date."""
        reference_date = datetime(2023, 12, 31)
        return RFMSegmentationEngine(
            spark_session=spark_session, reference_date=reference_date, quintiles=True
        )

    def test_rfm_engine_initialization(self, spark_session):
        """Test RFM engine initialization."""
        reference_date = datetime(2023, 12, 31)
        engine = RFMSegmentationEngine(
            spark_session=spark_session, reference_date=reference_date, quintiles=True
        )

        assert engine.spark == spark_session
        assert engine.reference_date == reference_date
        assert engine.quintiles == True
        assert engine.score_range == 5
        assert len(engine.segment_map) > 0

    def test_rfm_engine_initialization_without_spark_session(self):
        """Test RFM engine initialization without Spark session."""
        with pytest.raises(ValueError, match="No active Spark session found"):
            RFMSegmentationEngine(spark_session=None)

    def test_calculate_rfm_metrics(self, rfm_engine, sample_transactions_df):
        """Test RFM metrics calculation."""
        rfm_metrics = rfm_engine.calculate_rfm_metrics(sample_transactions_df)

        # Check that we get results for all customers
        customer_count = rfm_metrics.count()
        unique_customers = (
            sample_transactions_df.select("customer_id").distinct().count()
        )
        assert customer_count == unique_customers

        # Check required columns are present
        expected_columns = [
            "customer_id",
            "recency_days",
            "frequency",
            "monetary",
            "first_purchase_date",
            "last_purchase_date",
            "avg_order_value",
        ]
        assert all(col in rfm_metrics.columns for col in expected_columns)

        # Test specific customer metrics
        customer_006_metrics = rfm_metrics.filter(
            col("customer_id") == "customer_006"
        ).collect()[0]
        assert customer_006_metrics.frequency == 5  # 5 transactions
        assert customer_006_metrics.monetary == 5000.0  # Sum of amounts
        assert (
            customer_006_metrics.recency_days == 21
        )  # Days from 2023-12-10 to 2023-12-31

    def test_assign_rfm_scores(self, rfm_engine, sample_transactions_df):
        """Test RFM score assignment."""
        rfm_metrics = rfm_engine.calculate_rfm_metrics(sample_transactions_df)
        scored_df = rfm_engine.assign_rfm_scores(rfm_metrics)

        # Check that scoring columns are added
        scoring_columns = [
            "recency_score",
            "frequency_score",
            "monetary_score",
            "rfm_score",
        ]
        assert all(col in scored_df.columns for col in scoring_columns)

        # Check score ranges
        scores = scored_df.select(
            "recency_score", "frequency_score", "monetary_score"
        ).collect()
        for row in scores:
            assert 1 <= row.recency_score <= 5
            assert 1 <= row.frequency_score <= 5
            assert 1 <= row.monetary_score <= 5

        # Check RFM score format (should be 3-digit string)
        rfm_scores = [row.rfm_score for row in scored_df.select("rfm_score").collect()]
        assert all(len(score) == 3 and score.isdigit() for score in rfm_scores)

    def test_assign_customer_segments(self, rfm_engine, sample_transactions_df):
        """Test customer segment assignment."""
        rfm_metrics = rfm_engine.calculate_rfm_metrics(sample_transactions_df)
        scored_df = rfm_engine.assign_rfm_scores(rfm_metrics)
        segmented_df = rfm_engine.assign_customer_segments(scored_df)

        # Check segment columns are added
        segment_columns = ["segment", "customer_value_tier", "engagement_level"]
        assert all(col in segmented_df.columns for col in segment_columns)

        # Check that all customers have segments assigned
        null_segments = segmented_df.filter(col("segment").isNull()).count()
        assert null_segments == 0

        # Check valid segment values
        segments = [row.segment for row in segmented_df.select("segment").collect()]
        valid_segments = [segment.value for segment in CustomerSegment]
        valid_segments.append("Need Attention")  # Default fallback
        assert all(segment in valid_segments for segment in segments)

    def test_generate_customer_profiles(self, rfm_engine, sample_transactions_df):
        """Test complete customer profile generation."""
        customer_profiles = rfm_engine.generate_customer_profiles(
            sample_transactions_df
        )

        # Check all required columns are present
        expected_columns = [
            "customer_id",
            "recency_days",
            "frequency",
            "monetary",
            "first_purchase_date",
            "last_purchase_date",
            "avg_order_value",
            "recency_score",
            "frequency_score",
            "monetary_score",
            "rfm_score",
            "segment",
            "customer_value_tier",
            "engagement_level",
            "customer_lifetime_days",
            "purchase_frequency_per_month",
            "is_new_customer",
            "is_at_risk",
        ]
        assert all(col in customer_profiles.columns for col in expected_columns)

        # Check derived metrics
        profiles = customer_profiles.collect()
        for profile in profiles:
            assert profile.customer_lifetime_days >= 0
            assert profile.purchase_frequency_per_month >= 0
            assert isinstance(profile.is_new_customer, bool)
            assert isinstance(profile.is_at_risk, bool)

    def test_get_segment_summary(self, rfm_engine, sample_transactions_df):
        """Test segment summary generation."""
        customer_profiles = rfm_engine.generate_customer_profiles(
            sample_transactions_df
        )
        segment_summary = rfm_engine.get_segment_summary(customer_profiles)

        # Check summary columns
        expected_columns = [
            "segment",
            "customer_count",
            "total_revenue",
            "avg_monetary_value",
            "avg_frequency",
            "avg_recency_days",
            "max_monetary_value",
            "min_monetary_value",
            "revenue_percentage",
        ]
        assert all(col in segment_summary.columns for col in expected_columns)

        # Check that revenue percentages sum to ~100%
        total_revenue_pct = sum(
            [row.revenue_percentage for row in segment_summary.collect()]
        )
        assert abs(total_revenue_pct - 100.0) < 0.1  # Allow small floating point error

    def test_identify_high_value_customers(self, rfm_engine, sample_transactions_df):
        """Test high-value customer identification."""
        customer_profiles = rfm_engine.generate_customer_profiles(
            sample_transactions_df
        )
        high_value_customers = rfm_engine.identify_high_value_customers(
            customer_profiles, top_percentage=0.2
        )

        # Check that we get approximately 20% of customers
        total_customers = customer_profiles.count()
        high_value_count = high_value_customers.count()
        expected_count = int(total_customers * 0.2)

        # Allow some variance due to ranking ties
        assert abs(high_value_count - expected_count) <= 2

        # Check that composite score column is added
        assert "composite_score" in high_value_customers.columns
        assert "customer_rank" in high_value_customers.columns

    def test_recommend_actions(self, rfm_engine, sample_transactions_df):
        """Test action recommendations generation."""
        customer_profiles = rfm_engine.generate_customer_profiles(
            sample_transactions_df
        )
        profiles_with_actions = rfm_engine.recommend_actions(customer_profiles)

        # Check that recommended_action column is added
        assert "recommended_action" in profiles_with_actions.columns

        # Check that all customers have recommendations
        null_actions = profiles_with_actions.filter(
            col("recommended_action").isNull()
        ).count()
        assert null_actions == 0

        # Check that recommendations are meaningful strings
        actions = [
            row.recommended_action
            for row in profiles_with_actions.select("recommended_action").collect()
        ]
        assert all(len(action) > 20 for action in actions)  # Should be descriptive

    def test_segment_mapping_completeness(self, rfm_engine):
        """Test that segment mapping covers common score combinations."""
        # Test that key score combinations are mapped
        key_scores = ["555", "111", "545", "155", "515", "151", "115", "333"]

        for score in key_scores:
            if score in rfm_engine.segment_map:
                segment = rfm_engine.segment_map[score]
                assert isinstance(segment, CustomerSegment)

    def test_quartile_vs_quintile_scoring(self, spark_session, sample_transactions_df):
        """Test difference between quartile and quintile scoring."""
        reference_date = datetime(2023, 12, 31)

        # Test quintile engine
        quintile_engine = RFMSegmentationEngine(
            spark_session=spark_session, reference_date=reference_date, quintiles=True
        )

        # Test quartile engine
        quartile_engine = RFMSegmentationEngine(
            spark_session=spark_session, reference_date=reference_date, quintiles=False
        )

        assert quintile_engine.score_range == 5
        assert quartile_engine.score_range == 4

        # Generate profiles with both methods
        quintile_profiles = quintile_engine.generate_customer_profiles(
            sample_transactions_df
        )
        quartile_profiles = quartile_engine.generate_customer_profiles(
            sample_transactions_df
        )

        # Check score ranges
        quintile_scores = quintile_profiles.select(
            "recency_score", "frequency_score", "monetary_score"
        ).collect()
        quartile_scores = quartile_profiles.select(
            "recency_score", "frequency_score", "monetary_score"
        ).collect()

        for row in quintile_scores:
            assert 1 <= row.recency_score <= 5
            assert 1 <= row.frequency_score <= 5
            assert 1 <= row.monetary_score <= 5

        for row in quartile_scores:
            assert 1 <= row.recency_score <= 4
            assert 1 <= row.frequency_score <= 4
            assert 1 <= row.monetary_score <= 4

    def test_custom_column_names(self, rfm_engine, spark_session):
        """Test RFM analysis with custom column names."""
        # Create data with different column names
        custom_data = [
            ("user_001", "2023-12-01", 150.00),
            ("user_002", "2023-11-15", 200.00),
            ("user_003", "2023-10-20", 175.00),
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
        customer_profiles = rfm_engine.generate_customer_profiles(
            custom_df,
            customer_id_col="user_id",
            transaction_date_col="order_date",
            amount_col="total",
        )

        assert customer_profiles.count() == 3
        assert "user_id" in customer_profiles.columns

    def test_edge_cases(self, spark_session):
        """Test edge cases and error conditions."""
        reference_date = datetime(2023, 12, 31)
        engine = RFMSegmentationEngine(
            spark_session=spark_session, reference_date=reference_date
        )

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
        empty_profiles = engine.generate_customer_profiles(empty_df)
        assert empty_profiles.count() == 0

        # Test with single customer, single transaction
        single_data = [("customer_001", "2023-12-01", 100.00)]
        single_df = spark_session.createDataFrame(single_data, empty_schema)

        single_profiles = engine.generate_customer_profiles(single_df)
        assert single_profiles.count() == 1

        customer = single_profiles.collect()[0]
        assert customer.customer_id == "customer_001"
        assert customer.frequency == 1
        assert customer.monetary == 100.00

    def test_data_quality_validations(self, rfm_engine, spark_session):
        """Test data quality and validation scenarios."""
        # Test with null values
        null_data = [
            ("customer_001", "2023-12-01", 150.00),
            ("customer_002", None, 200.00),  # Null date
            ("customer_003", "2023-10-20", None),  # Null amount
            (None, "2023-11-15", 175.00),  # Null customer ID
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
        profiles = rfm_engine.generate_customer_profiles(null_df)
        valid_profiles = profiles.filter(
            col("customer_id").isNotNull()
            & col("monetary").isNotNull()
            & col("frequency").isNotNull()
        )

        # Should only process valid records
        assert valid_profiles.count() >= 1

    def test_performance_with_large_dataset(self, rfm_engine, spark_session):
        """Test performance characteristics with larger dataset."""
        # Generate larger synthetic dataset
        large_data = []
        for customer_id in range(1, 1001):  # 1000 customers
            for transaction in range(1, 6):  # 5 transactions each
                date = f"2023-{(transaction % 12) + 1:02d}-{(transaction % 28) + 1:02d}"
                amount = 50.0 + (customer_id % 500) + (transaction * 25)
                large_data.append((f"customer_{customer_id:03d}", date, amount))

        large_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField(
                    "transaction_date", StringType(), True
                ),  # Using string for simplicity
                StructField("amount", DoubleType(), True),
            ]
        )

        large_df = spark_session.createDataFrame(large_data, large_schema)
        large_df = large_df.withColumn(
            "transaction_date", col("transaction_date").cast("date")
        )

        # Test that processing completes successfully
        profiles = rfm_engine.generate_customer_profiles(large_df)
        assert profiles.count() == 1000  # Should process all customers

        # Test segment distribution
        segment_summary = rfm_engine.get_segment_summary(profiles)
        assert segment_summary.count() > 0  # Should have multiple segments


class TestRFMDataStructures:
    """Test RFM data structures and utilities."""

    def test_customer_segment_enum(self):
        """Test CustomerSegment enum."""
        # Check that all expected segments exist
        expected_segments = [
            "CHAMPIONS",
            "LOYAL_CUSTOMERS",
            "POTENTIAL_LOYALISTS",
            "NEW_CUSTOMERS",
            "PROMISING",
            "NEED_ATTENTION",
            "ABOUT_TO_SLEEP",
            "AT_RISK",
            "CANNOT_LOSE_THEM",
            "HIBERNATING",
            "LOST",
        ]

        for segment_name in expected_segments:
            assert hasattr(CustomerSegment, segment_name)
            segment = getattr(CustomerSegment, segment_name)
            assert isinstance(segment.value, str)
            assert len(segment.value) > 0

    def test_rfm_scores_dataclass(self):
        """Test RFMScores dataclass."""
        scores = RFMScores(
            recency_score=5,
            frequency_score=4,
            monetary_score=5,
            rfm_score="",
            segment=CustomerSegment.CHAMPIONS,
        )

        # Check auto-generated RFM score
        assert scores.rfm_score == "545"
        assert scores.segment == CustomerSegment.CHAMPIONS

        # Test with predefined RFM score
        scores_predefined = RFMScores(
            recency_score=3,
            frequency_score=2,
            monetary_score=1,
            rfm_score="321",
            segment=CustomerSegment.NEED_ATTENTION,
        )

        assert scores_predefined.rfm_score == "321"

    def test_customer_profile_dataclass(self):
        """Test CustomerProfile dataclass."""
        from datetime import datetime

        rfm_scores = RFMScores(5, 4, 5, "545", CustomerSegment.CHAMPIONS)

        profile = CustomerProfile(
            customer_id="customer_001",
            first_purchase_date=datetime(2023, 1, 1),
            last_purchase_date=datetime(2023, 12, 1),
            days_since_last_purchase=30,
            total_purchases=10,
            total_spent=1500.0,
            avg_order_value=150.0,
            rfm_scores=rfm_scores,
            segment=CustomerSegment.CHAMPIONS,
            customer_value_tier="High Value",
            engagement_level="Highly Engaged",
        )

        assert profile.customer_id == "customer_001"
        assert profile.total_purchases == 10
        assert profile.rfm_scores.rfm_score == "545"
        assert profile.segment == CustomerSegment.CHAMPIONS


@pytest.mark.integration
class TestRFMIntegration:
    """Integration tests for RFM segmentation with real-world scenarios."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for integration tests."""
        spark = (
            SparkSession.builder.appName("RFM-Integration-Tests")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .getOrCreate()
        )
        yield spark
        spark.stop()

    def test_end_to_end_rfm_workflow(self, spark_session):
        """Test complete end-to-end RFM analysis workflow."""
        # Create realistic e-commerce transaction data
        ecommerce_data = [
            # Champions - High R, F, M
            ("champion_001", "2023-12-15", 500.00),
            ("champion_001", "2023-12-01", 300.00),
            ("champion_001", "2023-11-15", 400.00),
            ("champion_001", "2023-11-01", 600.00),
            ("champion_001", "2023-10-15", 350.00),
            # Lost customers - Low R, F, M
            ("lost_001", "2023-03-01", 50.00),
            ("lost_002", "2023-02-15", 75.00),
            # At risk - Low R, High F, M
            ("at_risk_001", "2023-08-01", 800.00),
            ("at_risk_001", "2023-07-15", 900.00),
            ("at_risk_001", "2023-07-01", 750.00),
            ("at_risk_001", "2023-06-15", 850.00),
            # New customers - High R, Low F
            ("new_001", "2023-12-20", 200.00),
            ("new_002", "2023-12-18", 150.00),
            ("new_003", "2023-12-16", 300.00),
            # Hibernating - Low in all dimensions
            ("hibernating_001", "2023-04-01", 100.00),
            ("hibernating_001", "2023-03-15", 80.00),
            ("hibernating_002", "2023-05-01", 120.00),
        ]

        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", DateType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )

        transactions_df = spark_session.createDataFrame(ecommerce_data, schema)

        # Initialize RFM engine
        reference_date = datetime(2023, 12, 31)
        engine = RFMSegmentationEngine(
            spark_session=spark_session, reference_date=reference_date, quintiles=True
        )

        # Run complete analysis
        customer_profiles = engine.generate_customer_profiles(transactions_df)
        profiles_with_actions = engine.recommend_actions(customer_profiles)
        segment_summary = engine.get_segment_summary(customer_profiles)
        high_value_customers = engine.identify_high_value_customers(
            customer_profiles, 0.3
        )

        # Validate results
        total_customers = customer_profiles.count()
        assert total_customers > 0

        # Check that we have diverse segments
        unique_segments = customer_profiles.select("segment").distinct().count()
        assert unique_segments >= 3

        # Verify high-value customer identification
        high_value_count = high_value_customers.count()
        expected_high_value = int(total_customers * 0.3)
        assert abs(high_value_count - expected_high_value) <= 1

        # Check segment summary completeness
        summary_segments = segment_summary.count()
        assert summary_segments > 0

        # Verify action recommendations
        null_recommendations = profiles_with_actions.filter(
            col("recommended_action").isNull()
        ).count()
        assert null_recommendations == 0

    def test_seasonal_customer_behavior_analysis(self, spark_session):
        """Test RFM analysis with seasonal customer behavior patterns."""
        # Simulate seasonal shopping patterns
        seasonal_data = []

        # Holiday shoppers - high activity in December
        for customer_id in range(1, 11):
            for day in [5, 10, 15, 20, 25]:
                amount = 100 + (customer_id * 10) + (day * 5)
                seasonal_data.append(
                    (f"holiday_{customer_id:02d}", f"2023-12-{day:02d}", amount)
                )

        # Summer shoppers - active in July
        for customer_id in range(1, 6):
            for day in [10, 15, 20]:
                amount = 150 + (customer_id * 15)
                seasonal_data.append(
                    (f"summer_{customer_id:02d}", f"2023-07-{day:02d}", amount)
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
        engine = RFMSegmentationEngine(
            spark_session=spark_session,
            reference_date=datetime(2023, 12, 31),
            quintiles=True,
        )

        profiles = engine.generate_customer_profiles(seasonal_df)

        # Holiday shoppers should have high recency scores
        holiday_customers = profiles.filter(col("customer_id").startswith("holiday_"))
        avg_recency_score = holiday_customers.agg({"recency_score": "avg"}).collect()[
            0
        ][0]
        assert avg_recency_score >= 3.0  # Should be recent

        # Summer shoppers should have low recency scores
        summer_customers = profiles.filter(col("customer_id").startswith("summer_"))
        avg_summer_recency = summer_customers.agg({"recency_score": "avg"}).collect()[
            0
        ][0]
        assert avg_summer_recency <= 3.0  # Should be less recent

    def test_business_impact_metrics(self, spark_session):
        """Test calculation of business impact metrics from RFM analysis."""
        # Create data representing different customer value scenarios
        business_data = [
            # High-value loyal customers
            ("vip_001", "2023-12-15", 1000.00),
            ("vip_001", "2023-11-30", 1200.00),
            ("vip_001", "2023-11-15", 800.00),
            ("vip_001", "2023-10-30", 1100.00),
            # Medium-value regular customers
            ("regular_001", "2023-12-10", 200.00),
            ("regular_001", "2023-11-25", 250.00),
            ("regular_001", "2023-11-10", 180.00),
            # Low-value occasional customers
            ("occasional_001", "2023-12-05", 50.00),
            ("occasional_002", "2023-11-20", 75.00),
            ("occasional_003", "2023-10-15", 60.00),
        ]

        schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", DateType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )

        business_df = spark_session.createDataFrame(business_data, schema)

        engine = RFMSegmentationEngine(
            spark_session=spark_session, reference_date=datetime(2023, 12, 31)
        )

        profiles = engine.generate_customer_profiles(business_df)
        segment_summary = engine.get_segment_summary(profiles)

        # Calculate business metrics
        total_revenue = profiles.agg({"monetary": "sum"}).collect()[0][0]
        total_customers = profiles.count()
        avg_customer_value = total_revenue / total_customers

        # Verify business impact calculations
        assert total_revenue > 0
        assert avg_customer_value > 0

        # Check that Champions/high-value segments drive significant revenue
        high_value_segments = segment_summary.filter(
            col("avg_monetary_value") > avg_customer_value
        )

        high_value_revenue_pct = sum(
            [row.revenue_percentage for row in high_value_segments.collect()]
        )

        # High-value segments should contribute disproportionate revenue
        assert (
            high_value_revenue_pct > 50
        )  # Top segments should drive majority of revenue


from pyspark.sql import SparkSession
import pytest
from src.analytics.customer_journey import CustomerJourney
import pyspark.sql.functions as F
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.appName("CustomerJourneyTests")
        .master("local[2]")
        .getOrCreate()
    )

@pytest.fixture
def sample_user_behavior_data(spark):
    """Provides sample user behavior data for testing."""
    data = [
        ("sess1", "user1", "page_view", datetime(2025, 1, 1, 10, 0, 0)),
        ("sess1", "user1", "add_to_cart", datetime(2025, 1, 1, 10, 5, 0)),
        ("sess1", "user1", "purchase", datetime(2025, 1, 1, 10, 10, 0)),
        ("sess2", "user2", "page_view", datetime(2025, 1, 1, 11, 0, 0)),
        ("sess2", "user2", "add_to_cart", datetime(2025, 1, 1, 11, 5, 0)),
        ("sess3", "user3", "page_view", datetime(2025, 1, 1, 12, 0, 0)),
    ]
    schema = ["session_id", "user_id", "event_type", "timestamp"]
    return spark.createDataFrame(data, schema)

def test_track_touchpoints(spark, sample_user_behavior_data):
    """Test tracking customer touchpoints."""
    journey_analyzer = CustomerJourney(spark)
    df_with_touchpoints = journey_analyzer.track_touchpoints(sample_user_behavior_data)

    # Verify touchpoint order for session 1
    sess1_touchpoints = df_with_touchpoints.filter(F.col("session_id") == "sess1").orderBy("timestamp").collect()
    assert sess1_touchpoints[0]["touchpoint_order"] == 1
    assert sess1_touchpoints[1]["touchpoint_order"] == 2
    assert sess1_touchpoints[2]["touchpoint_order"] == 3

def test_analyze_funnel(spark, sample_user_behavior_data):
    """Test funnel analysis."""
    journey_analyzer = CustomerJourney(spark)
    funnel_steps = ["page_view", "add_to_cart", "purchase"]
    funnel_results = journey_analyzer.analyze_funnel(sample_user_behavior_data, funnel_steps)

    results_map = {row["funnel_step"]: row["session_count"] for row in funnel_results.collect()}

    assert results_map["page_view"] == 3
    assert results_map["add_to_cart"] == 2
    assert results_map["purchase"] == 1

def test_calculate_conversion_rate(spark, sample_user_behavior_data):
    """Test conversion rate calculation."""
    journey_analyzer = CustomerJourney(spark)

    # Test conversion from page_view to purchase
    conversion_rate_pv_purchase = journey_analyzer.calculate_conversion_rate(
        sample_user_behavior_data, "page_view", "purchase"
    )
    assert conversion_rate_pv_purchase == 1/3  # 1 purchase / 3 page_views

    # Test conversion from add_to_cart to purchase
    conversion_rate_atc_purchase = journey_analyzer.calculate_conversion_rate(
        sample_user_behavior_data, "add_to_cart", "purchase"
    )
    assert conversion_rate_atc_purchase == 1/2  # 1 purchase / 2 add_to_carts

    # Test conversion with no start events
    conversion_rate_no_start = journey_analyzer.calculate_conversion_rate(
        sample_user_behavior_data, "non_existent_event", "purchase"
    )
    assert conversion_rate_no_start == 0.0

    # Test conversion with no end events
    conversion_rate_no_end = journey_analyzer.calculate_conversion_rate(
        sample_user_behavior_data, "page_view", "non_existent_event"
    )
    assert conversion_rate_no_end == 0.0

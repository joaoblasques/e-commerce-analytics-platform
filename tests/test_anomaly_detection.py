from datetime import datetime

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from src.analytics.anomaly_detection import AnomalyDetector


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.appName("AnomalyDetectionTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_streaming_data(spark):
    """Provides sample streaming data for testing anomaly detection."""
    data = [
        ("txn1", "user1", 100.0, 1, datetime(2025, 1, 1, 10, 0, 0)),
        ("txn2", "user1", 105.0, 1, datetime(2025, 1, 1, 10, 1, 0)),
        ("txn3", "user2", 1000.0, 10, datetime(2025, 1, 1, 10, 2, 0)),  # Anomaly
        ("txn4", "user3", 110.0, 1, datetime(2025, 1, 1, 10, 3, 0)),
        ("txn5", "user4", 95.0, 1, datetime(2025, 1, 1, 10, 4, 0)),
        ("txn6", "user5", 10.0, 1, datetime(2025, 1, 1, 10, 5, 0)),  # Anomaly
    ]
    schema = ["transaction_id", "user_id", "price", "quantity", "timestamp"]
    return spark.createDataFrame(data, schema)


def test_detect_anomalies(spark, sample_streaming_data):
    """Test the anomaly detection logic."""
    detector = AnomalyDetector(
        spark, stream_source_path="dummy_path"
    )  # Path is not used in this test
    feature_cols = ["price", "quantity"]

    # For testing, we'll use the static DataFrame as if it were a micro-batch
    # The detect_anomalies method is designed for streaming, but can be tested with static data
    # if the internal logic doesn't rely on actual stream processing features (like awaitTermination)
    anomalies_df = detector.detect_anomalies(sample_streaming_data, feature_cols)

    # Collect results and check for expected anomalies
    anomalies = anomalies_df.filter(F.col("is_anomaly") == True).collect()

    # Expected anomalies are txn3 (price 1000.0) and txn6 (price 10.0)
    assert len(anomalies) == 2
    assert any(row["transaction_id"] == "txn3" for row in anomalies)
    assert any(row["transaction_id"] == "txn6" for row in anomalies)

    # Check that non-anomalous data is not marked as anomaly
    non_anomalies = anomalies_df.filter(F.col("is_anomaly") == False).collect()
    assert len(non_anomalies) == 4
    assert all(
        row["transaction_id"] in ["txn1", "txn2", "txn4", "txn5"]
        for row in non_anomalies
    )

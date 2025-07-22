import pytest
from pyspark.sql import SparkSession

from src.analytics.churn_model import ChurnModel


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.appName("ChurnModelTests").master("local[2]").getOrCreate()
    )


def test_churn_model_feature_engineering(spark):
    """Test the feature engineering logic of the churn model."""
    # Create a dummy DataFrame
    data = [
        ("user1", "2025-06-01 10:00:00", 100.0),
        ("user2", "2025-07-20 12:00:00", 200.0),
    ]
    schema = ["user_id", "timestamp", "price"]
    df = spark.createDataFrame(data, schema)

    # Instantiate the churn model
    churn_model = ChurnModel(spark)

    # Run feature engineering
    features_df = churn_model._feature_engineering(df)

    # Assertions
    assert "churn" in features_df.columns
    assert "recency" in features_df.columns
    assert "frequency" in features_df.columns
    assert "monetary" in features_df.columns
    assert features_df.count() == 2

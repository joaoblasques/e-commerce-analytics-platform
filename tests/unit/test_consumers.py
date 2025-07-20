from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from src.streaming.consumers import (
    TransactionStreamConsumer,
    UserBehaviorStreamConsumer,
)


@pytest.fixture(scope="module")
def spark_session():
    """Fixture for creating a SparkSession."""
    return (
        SparkSession.builder.master("local[2]").appName("test-consumers").getOrCreate()
    )


@pytest.fixture
def mock_data_quality_engine():
    """Fixture for mocking the DataQualityEngine."""
    with patch("src.streaming.data_quality.DataQualityEngine") as mock_engine:
        yield mock_engine


def test_transaction_consumer_data_quality_integration(
    spark_session, mock_data_quality_engine
):
    """Test that the TransactionStreamConsumer correctly calls the DataQualityEngine."""
    consumer = TransactionStreamConsumer(spark_session)
    mock_df = spark_session.createDataFrame([], schema=consumer.get_schema())

    # Mock the return value of assess_data_quality
    mock_quality_df = mock_df.withColumn("quality_passed", lit(True))
    mock_quality_report = MagicMock()
    mock_data_quality_engine.return_value.assess_data_quality.return_value = (
        mock_quality_df,
        mock_quality_report,
    )

    # Call the transform_stream method
    result_df = consumer.transform_stream(mock_df)

    # Assert that the data quality engine was called
    mock_data_quality_engine.return_value.assess_data_quality.assert_called_once()
    assert "quality_passed" in result_df.columns


def test_user_behavior_consumer_data_quality_integration(
    spark_session, mock_data_quality_engine
):
    """Test that the UserBehaviorStreamConsumer correctly calls the DataQualityEngine."""
    consumer = UserBehaviorStreamConsumer(spark_session)
    mock_df = spark_session.createDataFrame([], schema=consumer.get_schema())

    # Mock the return value of assess_data_quality
    mock_quality_df = mock_df.withColumn("quality_passed", lit(True))
    mock_quality_report = MagicMock()
    mock_data_quality_engine.return_value.assess_data_quality.return_value = (
        mock_quality_df,
        mock_quality_report,
    )

    # Call the transform_stream method
    result_df = consumer.transform_stream(mock_df)

    # Assert that the data quality engine was called
    mock_data_quality_engine.return_value.assess_data_quality.assert_called_once()
    assert "quality_passed" in result_df.columns

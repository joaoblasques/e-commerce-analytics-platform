"""
Unit tests for spark_utils module.
"""
from unittest.mock import Mock, patch

import pytest

from src.utils.spark_utils import (
    create_spark_session,
    stop_spark_session,
    validate_schema,
)


class TestSparkUtils:
    """Test cases for spark utilities."""

    @patch("src.utils.spark_utils.SparkSession")
    def test_create_spark_session_success(self, mock_spark_session):
        """Test successful Spark session creation."""
        # Setup mock
        mock_builder = Mock()
        mock_session = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        # Test
        result = create_spark_session("TestApp", "local[2]")

        # Assertions
        assert result == mock_session
        mock_builder.appName.assert_called_with("TestApp")
        mock_builder.master.assert_called_with("local[2]")
        mock_builder.getOrCreate.assert_called_once()

    @patch("src.utils.spark_utils.SparkSession", None)
    def test_create_spark_session_no_pyspark(self):
        """Test Spark session creation when PySpark is not available."""
        with pytest.raises(ImportError, match="PySpark is not available"):
            create_spark_session()

    @patch("src.utils.spark_utils.SparkSession")
    def test_create_spark_session_with_custom_config(self, mock_spark_session):
        """Test Spark session creation with custom configuration."""
        # Setup mock
        mock_builder = Mock()
        mock_session = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        custom_config = {"spark.test.config": "test_value"}

        # Test
        result = create_spark_session(config=custom_config)

        # Assertions
        assert result == mock_session
        # Verify custom config was applied
        config_calls = mock_builder.config.call_args_list
        assert any("spark.test.config" in str(call) for call in config_calls)

    @patch("src.utils.spark_utils.SparkSession")
    def test_create_spark_session_failure(self, mock_spark_session):
        """Test Spark session creation failure."""
        # Setup mock to raise exception
        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.side_effect = Exception("Spark error")

        # Test
        with pytest.raises(RuntimeError, match="Spark session creation failed"):
            create_spark_session()

    @patch("src.utils.spark_utils.StructType")
    def test_validate_schema_success(self, mock_struct_type):
        """Test successful schema validation."""
        # Setup mock schema
        mock_field1 = Mock()
        mock_field1.name = "field1"
        mock_field2 = Mock()
        mock_field2.name = "field2"

        mock_schema = Mock()
        mock_schema.fields = [mock_field1, mock_field2]

        # Test
        result = validate_schema(mock_schema, ["field1", "field2"])

        # Assertions
        assert result is True

    @patch("src.utils.spark_utils.StructType")
    def test_validate_schema_missing_fields(self, mock_struct_type):
        """Test schema validation with missing fields."""
        # Setup mock schema
        mock_field1 = Mock()
        mock_field1.name = "field1"

        mock_schema = Mock()
        mock_schema.fields = [mock_field1]

        # Test
        result = validate_schema(mock_schema, ["field1", "field2"])

        # Assertions
        assert result is False

    @patch("src.utils.spark_utils.StructType", None)
    def test_validate_schema_no_pyspark(self):
        """Test schema validation when PySpark is not available."""
        with pytest.raises(ImportError, match="PySpark is not available"):
            validate_schema(Mock(), ["field1"])

    def test_stop_spark_session_success(self):
        """Test successful Spark session stop."""
        mock_session = Mock()

        # Test
        stop_spark_session(mock_session)

        # Assertions
        mock_session.stop.assert_called_once()

    def test_stop_spark_session_exception(self):
        """Test Spark session stop with exception."""
        mock_session = Mock()
        mock_session.stop.side_effect = Exception("Stop error")

        # Test - should not raise exception
        stop_spark_session(mock_session)

        # Assertions
        mock_session.stop.assert_called_once()

    def test_stop_spark_session_none(self):
        """Test stopping None session."""
        # Test - should not raise exception
        stop_spark_session(None)
        # No assertions needed, just verify it doesn't crash

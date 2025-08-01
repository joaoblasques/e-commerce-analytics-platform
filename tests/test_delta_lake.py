"""
Tests for Delta Lake functionality
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.data_lake.delta import DeltaLakeManager
from src.data_lake.delta_config import DeltaTableConfigurations, DeltaTableSchemas
from src.data_lake.delta_maintenance import DeltaMaintenanceScheduler
from src.data_lake.delta_streaming import DeltaStreamingManager


class TestDeltaLakeManager:
    """Test cases for DeltaLakeManager."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.conf = Mock()
        spark.conf.set = Mock()
        spark.createDataFrame = Mock()
        spark.read = Mock()
        return spark

    @pytest.fixture
    def mock_delta_table(self):
        """Mock Delta table."""
        with patch("src.data_lake.delta.DeltaTable") as mock_dt:
            delta_table = Mock()
            mock_dt.forPath.return_value = delta_table
            delta_table.history.return_value = Mock(spec=DataFrame)
            delta_table.optimize.return_value = Mock()
            delta_table.vacuum.return_value = Mock(spec=DataFrame)
            yield delta_table

    @pytest.fixture
    def delta_manager(self, mock_spark):
        """DeltaLakeManager instance with mocked Spark."""
        with patch("src.data_lake.delta.configure_spark_with_delta_pip") as mock_config:
            mock_builder = Mock()
            mock_config.return_value = mock_builder
            mock_builder.getOrCreate.return_value = mock_spark

            manager = DeltaLakeManager(
                base_path="s3a://test-delta-lake", spark_session=mock_spark
            )
            return manager

    def test_initialization(self, delta_manager, mock_spark):
        """Test DeltaLakeManager initialization."""
        assert delta_manager.base_path == "s3a://test-delta-lake"
        assert delta_manager.spark == mock_spark
        assert delta_manager.tables == {}

    def test_create_delta_table(self, delta_manager):
        """Test creating a Delta table."""
        # Mock DataFrame creation
        mock_df = Mock(spec=DataFrame)
        mock_writer = Mock()
        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.save = Mock()

        delta_manager.spark.createDataFrame.return_value = mock_df

        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("value", IntegerType(), True),
            ]
        )

        table_path = delta_manager.create_delta_table(
            table_name="test_table",
            schema=schema,
            partition_columns=["id"],
            properties={"autoOptimize.optimizeWrite": "true"},
        )

        expected_path = "s3a://test-delta-lake/test_table"
        assert table_path == expected_path
        assert delta_manager.tables["test_table"] == expected_path

        # Verify method calls
        delta_manager.spark.createDataFrame.assert_called_once_with([], schema)
        mock_writer.format.assert_called_with("delta")
        mock_writer.partitionBy.assert_called_with("id")
        mock_writer.option.assert_called_with(
            "delta.autoOptimize.optimizeWrite", "true"
        )
        mock_writer.mode.assert_called_with("overwrite")
        mock_writer.save.assert_called_with(expected_path)

    def test_write_to_delta_append(self, delta_manager):
        """Test writing to Delta table in append mode."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_writer = Mock()
        mock_df.write = mock_writer
        mock_df.count.return_value = 100
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.save = Mock()

        delta_manager.write_to_delta(mock_df, "test_table", mode="append")

        mock_writer.format.assert_called_with("delta")
        mock_writer.mode.assert_called_with("append")
        mock_writer.save.assert_called_with("s3a://test-delta-lake/test_table")

    def test_write_to_delta_merge(self, delta_manager, mock_delta_table):
        """Test writing to Delta table in merge mode."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50
        mock_df.alias.return_value = mock_df

        # Mock merge operations
        mock_merge_builder = Mock()
        mock_delta_table.alias.return_value = mock_delta_table
        mock_delta_table.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder
        mock_merge_builder.execute = Mock()

        delta_manager.write_to_delta(
            mock_df, "test_table", mode="merge", merge_condition="target.id = source.id"
        )

        mock_delta_table.merge.assert_called_once()
        mock_merge_builder.whenMatchedUpdateAll.assert_called_once()
        mock_merge_builder.whenNotMatchedInsertAll.assert_called_once()
        mock_merge_builder.execute.assert_called_once()

    def test_create_streaming_writer(self, delta_manager):
        """Test creating streaming writer for Delta table."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock streaming DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_stream_writer = Mock()
        mock_query = Mock(spec=StreamingQuery)

        mock_df.writeStream = mock_stream_writer
        mock_stream_writer.format.return_value = mock_stream_writer
        mock_stream_writer.outputMode.return_value = mock_stream_writer
        mock_stream_writer.option.return_value = mock_stream_writer
        mock_stream_writer.trigger.return_value = mock_stream_writer
        mock_stream_writer.start.return_value = mock_query

        result = delta_manager.create_streaming_writer(
            mock_df,
            "test_table",
            checkpoint_location="s3a://test-checkpoints/test_table",
            trigger_interval="60 seconds",
            output_mode="append",
        )

        assert result == mock_query
        mock_stream_writer.format.assert_called_with("delta")
        mock_stream_writer.outputMode.assert_called_with("append")
        mock_stream_writer.option.assert_called_with(
            "checkpointLocation", "s3a://test-checkpoints/test_table"
        )
        mock_stream_writer.start.assert_called_with("s3a://test-delta-lake/test_table")

    def test_time_travel_read_by_timestamp(self, delta_manager):
        """Test time travel read by timestamp."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock read operations
        mock_reader = Mock()
        mock_df = Mock(spec=DataFrame)
        delta_manager.spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        timestamp = datetime(2023, 1, 1, 12, 0, 0)
        result = delta_manager.time_travel_read("test_table", timestamp=timestamp)

        assert result == mock_df
        mock_reader.format.assert_called_with("delta")
        mock_reader.option.assert_called_with("timestampAsOf", "2023-01-01 12:00:00")
        mock_reader.load.assert_called_with("s3a://test-delta-lake/test_table")

    def test_time_travel_read_by_version(self, delta_manager):
        """Test time travel read by version."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock read operations
        mock_reader = Mock()
        mock_df = Mock(spec=DataFrame)
        delta_manager.spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        result = delta_manager.time_travel_read("test_table", version=5)

        assert result == mock_df
        mock_reader.format.assert_called_with("delta")
        mock_reader.option.assert_called_with("versionAsOf", 5)
        mock_reader.load.assert_called_with("s3a://test-delta-lake/test_table")

    def test_get_table_history(self, delta_manager, mock_delta_table):
        """Test getting table history."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock history
        mock_history = Mock(spec=DataFrame)
        mock_delta_table.history.return_value = mock_history

        result = delta_manager.get_table_history("test_table", limit=10)

        assert result == mock_history
        mock_delta_table.history.assert_called_with(10)

    def test_evolve_schema(self, delta_manager):
        """Test schema evolution."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock DataFrame with new schema
        mock_df = Mock(spec=DataFrame)
        mock_writer = Mock()
        mock_df.write = mock_writer
        mock_df.schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("new_field", StringType(), True),
            ]
        )

        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.save = Mock()

        delta_manager.evolve_schema("test_table", mock_df, merge_schema=True)

        mock_writer.format.assert_called_with("delta")
        mock_writer.mode.assert_called_with("append")
        mock_writer.option.assert_called_with("mergeSchema", "true")
        mock_writer.save.assert_called_with("s3a://test-delta-lake/test_table")

    def test_optimize_table_basic(self, delta_manager, mock_delta_table):
        """Test basic table optimization."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock optimization
        mock_optimize = Mock()
        mock_delta_table.optimize.return_value = mock_optimize
        mock_optimize.executeCompaction = Mock()

        delta_manager.optimize_table("test_table")

        mock_delta_table.optimize.assert_called_once()
        mock_optimize.executeCompaction.assert_called_once()

    def test_optimize_table_with_zorder(self, delta_manager, mock_delta_table):
        """Test table optimization with Z-ordering."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock optimization
        mock_optimize = Mock()
        mock_delta_table.optimize.return_value = mock_optimize
        mock_optimize.executeZOrderBy = Mock()

        delta_manager.optimize_table("test_table", z_order_columns=["id", "timestamp"])

        mock_delta_table.optimize.assert_called_once()
        mock_optimize.executeZOrderBy.assert_called_with("id", "timestamp")

    def test_vacuum_table(self, delta_manager, mock_delta_table):
        """Test table vacuum operation."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock vacuum result
        mock_result = Mock(spec=DataFrame)
        mock_delta_table.vacuum.return_value = mock_result

        result = delta_manager.vacuum_table(
            "test_table", retention_hours=24, dry_run=False
        )

        assert result == mock_result
        mock_delta_table.vacuum.assert_called_with(24)

    def test_vacuum_table_dry_run(self, delta_manager, mock_delta_table):
        """Test table vacuum dry run."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock vacuum result
        mock_result = Mock(spec=DataFrame)
        mock_delta_table.vacuum.return_value = mock_result

        result = delta_manager.vacuum_table(
            "test_table", retention_hours=168, dry_run=True
        )

        assert result == mock_result
        mock_delta_table.vacuum.assert_called_with(168, dry_run=True)

    def test_create_table_properties(self, delta_manager):
        """Test creating table properties."""
        properties = delta_manager.create_table_properties(
            auto_optimize=True,
            auto_compact=False,
            deletion_vectors=True,
            checkpoint_interval=5,
        )

        expected = {
            "autoOptimize.optimizeWrite": "true",
            "autoOptimize.autoCompact": "false",
            "enableDeletionVectors": "true",
            "checkpointInterval": "5",
        }

        assert properties == expected

    def test_get_table_info(self, delta_manager, mock_delta_table):
        """Test getting comprehensive table information."""
        # Setup table
        delta_manager.tables["test_table"] = "s3a://test-delta-lake/test_table"

        # Mock current data
        mock_current_df = Mock(spec=DataFrame)
        mock_current_df.count.return_value = 1000
        mock_current_df.schema.json.return_value = '{"type":"struct","fields":[]}'

        # Mock history
        mock_history_df = Mock(spec=DataFrame)
        mock_history_row = Mock()
        mock_history_row.__getitem__ = lambda self, key: {
            "version": 10,
            "timestamp": datetime.now(),
        }[key]
        mock_history_df.first.return_value = mock_history_row
        mock_history_df.isEmpty.return_value = False
        mock_history_df.orderBy.return_value = mock_history_df

        with patch.object(
            delta_manager, "time_travel_read", return_value=mock_current_df
        ):
            with patch.object(
                delta_manager, "get_table_history", return_value=mock_history_df
            ):
                info = delta_manager.get_table_info("test_table")

        assert info["table_name"] == "test_table"
        assert info["table_path"] == "s3a://test-delta-lake/test_table"
        assert info["current_count"] == 1000
        assert info["latest_version"] == 10

    def test_list_tables(self, delta_manager):
        """Test listing all registered tables."""
        # Setup tables
        delta_manager.tables = {
            "transactions": "s3a://test-delta-lake/transactions",
            "user_events": "s3a://test-delta-lake/user_events",
        }

        tables = delta_manager.list_tables()

        assert len(tables) == 2
        assert tables[0]["name"] == "transactions"
        assert tables[0]["path"] == "s3a://test-delta-lake/transactions"
        assert tables[0]["status"] == "active"
        assert tables[1]["name"] == "user_events"

    def test_table_not_found_error(self, delta_manager):
        """Test error handling when table is not found."""
        with pytest.raises(ValueError, match="Table 'nonexistent' not found"):
            delta_manager.write_to_delta(Mock(spec=DataFrame), "nonexistent")

    def test_close(self, delta_manager):
        """Test closing the Delta Lake manager."""
        delta_manager.spark.stop = Mock()
        delta_manager.close()
        delta_manager.spark.stop.assert_called_once()


class TestDeltaTableSchemas:
    """Test cases for DeltaTableSchemas."""

    def test_transaction_schema(self):
        """Test transaction schema structure."""
        schema = DeltaTableSchemas.transaction_schema()

        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]

        # Check key fields are present
        assert "transaction_id" in field_names
        assert "customer_id" in field_names
        assert "product_id" in field_names
        assert "total_amount" in field_names
        assert "timestamp" in field_names

        # Check data types
        field_dict = {field.name: field.dataType for field in schema.fields}
        assert isinstance(field_dict["transaction_id"], StringType)
        assert isinstance(field_dict["total_amount"], DoubleType)
        assert isinstance(field_dict["timestamp"], TimestampType)

    def test_user_behavior_schema(self):
        """Test user behavior schema structure."""
        schema = DeltaTableSchemas.user_behavior_schema()

        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]

        # Check key fields are present
        assert "event_id" in field_names
        assert "session_id" in field_names
        assert "user_id" in field_names
        assert "event_type" in field_names
        assert "timestamp" in field_names

    def test_customer_profile_schema(self):
        """Test customer profile schema structure."""
        schema = DeltaTableSchemas.customer_profile_schema()

        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]

        # Check key fields are present
        assert "customer_id" in field_names
        assert "email" in field_names
        assert "registration_date" in field_names
        assert "customer_segment" in field_names


class TestDeltaTableConfigurations:
    """Test cases for DeltaTableConfigurations."""

    def test_get_table_config_transactions(self):
        """Test getting configuration for transactions table."""
        config = DeltaTableConfigurations.get_table_config("transactions")

        assert "partition_columns" in config
        assert "properties" in config
        assert "schema" in config
        assert config["partition_columns"] == ["year", "month", "day"]

    def test_get_table_config_user_events(self):
        """Test getting configuration for user_events table."""
        config = DeltaTableConfigurations.get_table_config("user_events")

        assert "partition_columns" in config
        assert config["partition_columns"] == ["year", "month", "day", "event_type"]

    def test_get_table_config_invalid_table(self):
        """Test getting configuration for invalid table."""
        with pytest.raises(ValueError, match="Unknown table type: invalid_table"):
            DeltaTableConfigurations.get_table_config("invalid_table")

    def test_get_recommended_properties(self):
        """Test getting recommended table properties."""
        properties = DeltaTableConfigurations.get_recommended_properties()

        assert "autoOptimize.optimizeWrite" in properties
        assert "autoOptimize.autoCompact" in properties
        assert "enableDeletionVectors" in properties
        assert properties["autoOptimize.optimizeWrite"] == "true"

    def test_get_partition_strategy(self):
        """Test getting partition strategy."""
        strategy = DeltaTableConfigurations.get_partition_strategy("transactions")

        assert strategy["type"] == "temporal"
        assert strategy["columns"] == ["year", "month", "day"]
        assert strategy["description"] is not None


class TestDeltaStreamingManager:
    """Test cases for DeltaStreamingManager."""

    @pytest.fixture
    def mock_delta_manager(self):
        """Mock DeltaLakeManager."""
        return Mock(spec=DeltaLakeManager)

    @pytest.fixture
    def streaming_manager(self, mock_delta_manager):
        """DeltaStreamingManager with mocked dependencies."""
        return DeltaStreamingManager(mock_delta_manager)

    def test_initialization(self, streaming_manager, mock_delta_manager):
        """Test DeltaStreamingManager initialization."""
        assert streaming_manager.delta_manager == mock_delta_manager
        assert streaming_manager.active_streams == {}

    def test_start_transaction_stream(self, streaming_manager):
        """Test starting transaction streaming."""
        # Mock streaming DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_query = Mock(spec=StreamingQuery)
        mock_query.id = "stream_123"

        streaming_manager.delta_manager.create_streaming_writer.return_value = (
            mock_query
        )

        result = streaming_manager.start_transaction_stream(
            source_df=mock_df, checkpoint_location="s3a://checkpoints/transactions"
        )

        assert result == mock_query
        assert streaming_manager.active_streams["transactions"] == mock_query

        streaming_manager.delta_manager.create_streaming_writer.assert_called_once()

    def test_start_user_behavior_stream(self, streaming_manager):
        """Test starting user behavior streaming."""
        # Mock streaming DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_query = Mock(spec=StreamingQuery)
        mock_query.id = "stream_456"

        streaming_manager.delta_manager.create_streaming_writer.return_value = (
            mock_query
        )

        result = streaming_manager.start_user_behavior_stream(
            source_df=mock_df, checkpoint_location="s3a://checkpoints/user_events"
        )

        assert result == mock_query
        assert streaming_manager.active_streams["user_events"] == mock_query

    def test_stop_stream(self, streaming_manager):
        """Test stopping a specific stream."""
        # Setup active stream
        mock_query = Mock(spec=StreamingQuery)
        mock_query.stop = Mock()
        streaming_manager.active_streams["test_stream"] = mock_query

        streaming_manager.stop_stream("test_stream")

        mock_query.stop.assert_called_once()
        assert "test_stream" not in streaming_manager.active_streams

    def test_stop_all_streams(self, streaming_manager):
        """Test stopping all active streams."""
        # Setup multiple active streams
        mock_query1 = Mock(spec=StreamingQuery)
        mock_query2 = Mock(spec=StreamingQuery)
        mock_query1.stop = Mock()
        mock_query2.stop = Mock()

        streaming_manager.active_streams = {
            "stream1": mock_query1,
            "stream2": mock_query2,
        }

        streaming_manager.stop_all_streams()

        mock_query1.stop.assert_called_once()
        mock_query2.stop.assert_called_once()
        assert streaming_manager.active_streams == {}

    def test_get_stream_status(self, streaming_manager):
        """Test getting status of active streams."""
        # Setup active streams with different statuses
        mock_query1 = Mock(spec=StreamingQuery)
        mock_query1.isActive = True
        mock_query1.lastProgress = {"batchId": 10, "inputRowsPerSecond": 100}

        mock_query2 = Mock(spec=StreamingQuery)
        mock_query2.isActive = False
        mock_query2.lastProgress = {"batchId": 5, "inputRowsPerSecond": 0}

        streaming_manager.active_streams = {
            "active_stream": mock_query1,
            "inactive_stream": mock_query2,
        }

        status = streaming_manager.get_stream_status()

        assert len(status) == 2
        assert status["active_stream"]["is_active"] is True
        assert status["active_stream"]["last_progress"]["batchId"] == 10
        assert status["inactive_stream"]["is_active"] is False


class TestDeltaMaintenanceScheduler:
    """Test cases for DeltaMaintenanceScheduler."""

    @pytest.fixture
    def mock_delta_manager(self):
        """Mock DeltaLakeManager."""
        return Mock(spec=DeltaLakeManager)

    @pytest.fixture
    def maintenance_scheduler(self, mock_delta_manager):
        """DeltaMaintenanceScheduler with mocked dependencies."""
        return DeltaMaintenanceScheduler(mock_delta_manager)

    def test_initialization(self, maintenance_scheduler, mock_delta_manager):
        """Test DeltaMaintenanceScheduler initialization."""
        assert maintenance_scheduler.delta_manager == mock_delta_manager
        assert maintenance_scheduler.maintenance_config is not None

    def test_optimize_table_maintenance(self, maintenance_scheduler):
        """Test table optimization maintenance."""
        # Setup table list
        maintenance_scheduler.delta_manager.list_tables.return_value = [
            {"name": "transactions", "path": "s3a://lake/transactions"}
        ]

        # Mock optimization
        maintenance_scheduler.delta_manager.optimize_table = Mock()

        maintenance_scheduler.optimize_table("transactions")

        maintenance_scheduler.delta_manager.optimize_table.assert_called_once_with(
            "transactions", where_clause=None, z_order_columns=None
        )

    def test_vacuum_table_maintenance(self, maintenance_scheduler):
        """Test table vacuum maintenance."""
        # Setup table list
        maintenance_scheduler.delta_manager.list_tables.return_value = [
            {"name": "transactions", "path": "s3a://lake/transactions"}
        ]

        # Mock vacuum
        mock_result = Mock(spec=DataFrame)
        maintenance_scheduler.delta_manager.vacuum_table.return_value = mock_result

        result = maintenance_scheduler.vacuum_table("transactions", retention_hours=72)

        assert result == mock_result
        maintenance_scheduler.delta_manager.vacuum_table.assert_called_once_with(
            "transactions", retention_hours=72, dry_run=False
        )

    def test_run_maintenance_schedule(self, maintenance_scheduler):
        """Test running scheduled maintenance."""
        # Setup maintenance configuration
        maintenance_config = {
            "optimize": {
                "enabled": True,
                "tables": ["transactions", "user_events"],
                "schedule": "daily",
            },
            "vacuum": {
                "enabled": True,
                "retention_hours": 168,
                "tables": ["transactions"],
            },
        }

        maintenance_scheduler.maintenance_config = maintenance_config

        # Mock methods
        maintenance_scheduler.optimize_table = Mock()
        maintenance_scheduler.vacuum_table = Mock()

        maintenance_scheduler.run_maintenance_schedule()

        # Verify optimization was called for both tables
        assert maintenance_scheduler.optimize_table.call_count == 2
        maintenance_scheduler.optimize_table.assert_any_call("transactions")
        maintenance_scheduler.optimize_table.assert_any_call("user_events")

        # Verify vacuum was called for transactions table
        maintenance_scheduler.vacuum_table.assert_called_once_with("transactions")

    def test_get_maintenance_metrics(self, maintenance_scheduler):
        """Test getting maintenance metrics."""
        # Mock table info
        maintenance_scheduler.delta_manager.get_table_info.return_value = {
            "current_count": 10000,
            "latest_version": 25,
        }

        maintenance_scheduler.delta_manager.list_tables.return_value = [
            {"name": "transactions", "path": "s3a://lake/transactions"}
        ]

        metrics = maintenance_scheduler.get_maintenance_metrics()

        assert "tables" in metrics
        assert "transactions" in metrics["tables"]
        assert metrics["tables"]["transactions"]["record_count"] == 10000
        assert metrics["tables"]["transactions"]["version"] == 25

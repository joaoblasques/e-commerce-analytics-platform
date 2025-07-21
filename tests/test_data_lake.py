"""
Tests for Data Lake functionality
"""

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.data_lake.compaction import DataCompactor
from src.data_lake.ingestion import DataLakeIngester
from src.data_lake.metadata import (
    ColumnMetadata,
    MetadataManager,
    TableMetadata,
    TableStatus,
)
from src.data_lake.storage import DataLakeStorage


class TestDataLakeStorage:
    """Test cases for DataLakeStorage."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.sparkContext._conf = Mock()
        spark.sparkContext._conf.set = Mock()
        return spark

    @pytest.fixture
    def storage(self, mock_spark):
        """DataLakeStorage instance with mocked Spark."""
        with patch(
            "src.data_lake.storage.create_spark_session", return_value=mock_spark
        ):
            return DataLakeStorage(
                base_path="s3a://test-lake", spark_session=mock_spark
            )

    def test_initialization(self, storage):
        """Test DataLakeStorage initialization."""
        assert storage.base_path == "s3a://test-lake"
        assert storage.compression == "snappy"

    def test_get_partitioning_strategy_transactions(self, storage):
        """Test partitioning strategy for transactions."""
        strategy = storage.get_partitioning_strategy("transactions")

        assert strategy["partition_columns"] == ["year", "month", "day"]
        assert strategy["date_column"] == "transaction_timestamp"
        assert strategy["sort_columns"] == ["transaction_id"]

    def test_get_partitioning_strategy_user_events(self, storage):
        """Test partitioning strategy for user events."""
        strategy = storage.get_partitioning_strategy("user_events")

        assert strategy["partition_columns"] == ["year", "month", "day", "event_type"]
        assert strategy["date_column"] == "event_timestamp"
        assert strategy["sort_columns"] == ["session_id", "event_timestamp"]

    def test_get_partitioning_strategy_custom(self, storage):
        """Test custom partitioning strategy."""
        custom_partitions = ["region", "category"]
        strategy = storage.get_partitioning_strategy("custom", custom_partitions)

        assert strategy["partition_columns"] == custom_partitions
        assert strategy["date_column"] is None

    def test_prepare_dataframe_for_partitioning(self, storage, mock_spark):
        """Test DataFrame preparation for partitioning."""
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["transaction_id", "transaction_timestamp", "amount"]
        mock_df.withColumn = Mock(return_value=mock_df)

        strategy = {
            "partition_columns": ["year", "month"],
            "date_column": "transaction_timestamp",
        }

        result_df = storage.prepare_dataframe_for_partitioning(mock_df, strategy)

        # Should add year and month columns
        assert mock_df.withColumn.call_count == 2

    @patch("src.data_lake.storage.datetime")
    def test_write_partitioned_data(self, mock_datetime, storage):
        """Test writing partitioned data."""
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["transaction_id", "amount"]
        mock_df.coalesce.return_value = mock_df
        mock_df.count.return_value = 1000

        # Mock writer
        mock_writer = Mock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.parquet = Mock()
        mock_df.write = mock_writer

        with patch.object(
            storage, "get_partitioning_strategy"
        ) as mock_strategy, patch.object(
            storage, "prepare_dataframe_for_partitioning"
        ) as mock_prepare:
            mock_strategy.return_value = {
                "partition_columns": ["year", "month"],
                "date_column": "transaction_timestamp",
                "sort_columns": ["transaction_id"],
                "description": "Test strategy",
            }
            mock_prepare.return_value = mock_df

            output_path = storage.write_partitioned_data(
                df=mock_df,
                table_name="test_table",
                data_type="transactions",
                mode="append",
            )

            assert output_path == "s3a://test-lake/test_table"
            mock_writer.parquet.assert_called_once()


class TestDataLakeIngester:
    """Test cases for DataLakeIngester."""

    @pytest.fixture
    def mock_storage(self):
        """Mock DataLakeStorage."""
        storage = Mock(spec=DataLakeStorage)
        storage.base_path = "s3a://test-lake"
        return storage

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = Mock(spec=SparkSession)
        return spark

    @pytest.fixture
    def ingester(self, mock_storage, mock_spark):
        """DataLakeIngester instance with mocks."""
        with patch(
            "src.data_lake.ingestion.create_spark_session", return_value=mock_spark
        ):
            return DataLakeIngester(
                storage=mock_storage,
                spark_session=mock_spark,
                kafka_bootstrap_servers="test-kafka:9092",
            )

    @patch("src.data_lake.ingestion.KafkaConsumer")
    def test_ingest_from_kafka_batch_success(
        self, mock_kafka_consumer, ingester, mock_spark
    ):
        """Test successful Kafka batch ingestion."""
        # Mock Kafka consumer
        mock_consumer = Mock()
        mock_kafka_consumer.return_value = mock_consumer

        # Mock message batch
        mock_message = Mock()
        mock_message.value = {"transaction_id": "123", "amount": 100}

        mock_consumer.poll.return_value = {"test-topic-0": [mock_message]}

        # Mock DataFrame creation and operations
        mock_df = Mock(spec=DataFrame)
        mock_df.withColumn.return_value = mock_df
        mock_spark.createDataFrame.return_value = mock_df

        # Mock storage write
        ingester.storage.write_partitioned_data.return_value = (
            "s3a://test-lake/test_table"
        )

        with patch("src.data_lake.ingestion.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

            result = ingester.ingest_from_kafka_batch(
                topic="test-topic",
                table_name="test_table",
                data_type="transactions",
                batch_size=100,
                max_batches=1,
            )

        assert result["status"] == "completed"
        assert result["total_records"] == 1
        assert result["topic"] == "test-topic"
        assert result["table_name"] == "test_table"

    def test_ingest_files_batch(self, ingester, mock_spark):
        """Test file batch ingestion."""
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.withColumn.return_value = mock_df
        mock_df.count.return_value = 500

        # Mock Spark read operations
        mock_spark.read.json.return_value = mock_df

        # Mock storage write
        ingester.storage.write_partitioned_data.return_value = (
            "s3a://test-lake/test_table"
        )

        with patch("src.data_lake.ingestion.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

            result = ingester.ingest_files_batch(
                file_paths=["file1.json"],
                table_name="test_table",
                data_type="user_events",
                file_format="json",
                parallel=False,
            )

        assert result["status"] == "completed"
        assert result["total_records"] == 500
        assert result["processed_files"] == 1


class TestDataCompactor:
    """Test cases for DataCompactor."""

    @pytest.fixture
    def mock_storage(self):
        """Mock DataLakeStorage."""
        storage = Mock(spec=DataLakeStorage)
        storage.base_path = "s3a://test-lake"
        return storage

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = Mock(spec=SparkSession)

        # Mock Hadoop FileSystem operations
        mock_hadoop_conf = Mock()
        mock_fs = Mock()
        mock_jvm = Mock()
        mock_jvm.org.apache.hadoop.fs.FileSystem.get.return_value = mock_fs
        mock_jvm.org.apache.hadoop.fs.Path = Mock

        spark.sparkContext._jsc.hadoopConfiguration.return_value = mock_hadoop_conf
        spark.sparkContext._jvm = mock_jvm

        return spark

    @pytest.fixture
    def compactor(self, mock_storage, mock_spark):
        """DataCompactor instance with mocks."""
        with patch(
            "src.data_lake.compaction.create_spark_session", return_value=mock_spark
        ):
            return DataCompactor(storage=mock_storage, spark_session=mock_spark)

    def test_analyze_table_files(self, compactor, mock_spark):
        """Test table file analysis."""
        # Mock file system operations
        mock_fs = mock_spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get()
        mock_fs.exists.return_value = True

        # Mock file status
        mock_status = Mock()
        mock_status.isDirectory.return_value = False
        mock_status.getPath().getName.return_value = "part-00000.parquet"
        mock_status.getLen.return_value = 50 * 1024 * 1024  # 50MB
        mock_status.getModificationTime.return_value = 1609459200000  # 2021-01-01
        mock_status.getPath.return_value = "/test/path"

        mock_fs.listStatus.return_value = [mock_status]

        analysis = compactor.analyze_table_files("test_table")

        assert analysis["table_name"] == "test_table"
        assert analysis["file_count"] == 1
        assert analysis["total_size_mb"] == 50.0

    def test_compact_table_success(self, compactor, mock_spark, mock_storage):
        """Test successful table compaction."""
        # Mock analysis result
        analysis_result = {
            "file_count": 20,
            "total_size_mb": 500.0,
            "needs_compaction": True,
        }

        # Mock DataFrame operations
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10000
        mock_df.coalesce.return_value = mock_df
        mock_df.write.mode.return_value.parquet = Mock()

        mock_spark.read.parquet.return_value = mock_df

        with patch.object(
            compactor, "analyze_table_files"
        ) as mock_analyze, patch.object(
            compactor, "_create_backup"
        ) as mock_backup, patch.object(
            compactor, "_replace_table_data"
        ) as mock_replace, patch.object(
            compactor, "_cleanup_path"
        ) as mock_cleanup, patch(
            "src.data_lake.compaction.datetime"
        ) as mock_datetime:
            mock_analyze.side_effect = [
                analysis_result,
                {"file_count": 4, "total_size_mb": 480.0},
            ]
            mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

            result = compactor.compact_table("test_table", target_file_size_mb=128)

            assert result["status"] == "success"
            assert result["original_files"] == 20
            assert result["compacted_files"] == 4
            assert result["file_reduction"] == 16


class TestMetadataManager:
    """Test cases for MetadataManager."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        spark = Mock(spec=SparkSession)
        return spark

    @pytest.fixture
    def metadata_manager(self, mock_spark):
        """MetadataManager instance with mocks."""
        with patch(
            "src.data_lake.metadata.create_spark_session", return_value=mock_spark
        ):
            return MetadataManager(
                spark_session=mock_spark, catalog_path="s3a://test-catalog"
            )

    def test_register_table(self, metadata_manager, mock_spark):
        """Test table registration."""
        # Mock DataFrame and schema
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000

        # Mock schema
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        mock_schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )
        mock_df.schema = mock_schema

        mock_spark.read.parquet.return_value = mock_df

        # Mock DataFrame for storing metadata
        mock_metadata_df = Mock(spec=DataFrame)
        mock_metadata_df.coalesce.return_value.write.mode.return_value.json = Mock()
        mock_spark.createDataFrame.return_value = mock_metadata_df

        with patch.object(
            metadata_manager,
            "_detect_partition_columns",
            return_value=["year", "month"],
        ), patch.object(metadata_manager, "_store_table_metadata"):
            metadata = metadata_manager.register_table(
                table_name="test_table",
                table_path="s3a://test-lake/test_table",
                description="Test table",
                owner="test_user",
            )

            assert metadata.table_name == "test_table"
            assert metadata.record_count == 1000
            assert len(metadata.columns) == 2
            assert metadata.partition_columns == ["year", "month"]

    def test_column_metadata_creation(self):
        """Test ColumnMetadata creation."""
        column = ColumnMetadata(
            name="test_column",
            data_type="StringType",
            nullable=True,
            description="Test column",
        )

        assert column.name == "test_column"
        assert column.data_type == "StringType"
        assert column.nullable is True
        assert column.tags == []  # Default value

    def test_table_metadata_creation(self):
        """Test TableMetadata creation."""
        columns = [
            ColumnMetadata(name="id", data_type="IntegerType", nullable=False),
            ColumnMetadata(name="name", data_type="StringType", nullable=True),
        ]

        metadata = TableMetadata(
            table_name="test_table",
            database_name="test_db",
            table_path="s3a://test-lake/test_table",
            schema={},
            columns=columns,
            partition_columns=["year", "month"],
            record_count=1000,
            description="Test table",
        )

        assert metadata.table_name == "test_table"
        assert metadata.status == TableStatus.ACTIVE  # Default value
        assert len(metadata.columns) == 2
        assert metadata.lineage == {"upstream": [], "downstream": []}  # Default value

    def test_search_tables(self, metadata_manager):
        """Test table search functionality."""
        # Mock table metadata
        table1 = TableMetadata(
            table_name="user_transactions",
            database_name="test_db",
            table_path="s3a://test-lake/user_transactions",
            schema={},
            columns=[],
            partition_columns=[],
            description="User transaction data",
            tags=["transactions", "users"],
        )

        table2 = TableMetadata(
            table_name="product_catalog",
            database_name="test_db",
            table_path="s3a://test-lake/product_catalog",
            schema={},
            columns=[],
            partition_columns=[],
            description="Product information",
            tags=["products", "catalog"],
        )

        with patch.object(
            metadata_manager, "list_tables", return_value=[table1, table2]
        ):
            # Search by name
            results = metadata_manager.search_tables("transaction", ["table_name"])
            assert len(results) == 1
            assert results[0].table_name == "user_transactions"

            # Search by tag
            results = metadata_manager.search_tables("products", ["tags"])
            assert len(results) == 1
            assert results[0].table_name == "product_catalog"

            # Search by description
            results = metadata_manager.search_tables("User", ["description"])
            assert len(results) == 1
            assert results[0].table_name == "user_transactions"


# Integration-style tests
class TestDataLakeIntegration:
    """Integration tests for data lake components."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session for integration tests."""
        spark = Mock(spec=SparkSession)

        # Mock comprehensive Spark operations
        spark.sparkContext._conf = Mock()
        spark.sparkContext._conf.set = Mock()

        # Mock Hadoop FileSystem
        mock_hadoop_conf = Mock()
        mock_fs = Mock()
        mock_jvm = Mock()
        mock_jvm.org.apache.hadoop.fs.FileSystem.get.return_value = mock_fs
        mock_jvm.org.apache.hadoop.fs.Path = Mock

        spark.sparkContext._jsc.hadoopConfiguration.return_value = mock_hadoop_conf
        spark.sparkContext._jvm = mock_jvm

        return spark

    def test_end_to_end_workflow(self, mock_spark):
        """Test end-to-end data lake workflow."""
        with patch(
            "src.data_lake.storage.create_spark_session", return_value=mock_spark
        ), patch(
            "src.data_lake.ingestion.create_spark_session", return_value=mock_spark
        ), patch(
            "src.data_lake.compaction.create_spark_session", return_value=mock_spark
        ), patch(
            "src.data_lake.metadata.create_spark_session", return_value=mock_spark
        ):
            # Initialize components
            storage = DataLakeStorage(
                base_path="s3a://test-lake", spark_session=mock_spark
            )
            ingester = DataLakeIngester(storage=storage, spark_session=mock_spark)
            compactor = DataCompactor(storage=storage, spark_session=mock_spark)
            metadata_manager = MetadataManager(spark_session=mock_spark)

            # Verify components are initialized
            assert storage.base_path == "s3a://test-lake"
            assert ingester.storage == storage
            assert compactor.storage == storage
            assert metadata_manager.database_name == "data_lake"

    def test_partitioning_strategies_comprehensive(self, mock_spark):
        """Test all partitioning strategies."""
        with patch(
            "src.data_lake.storage.create_spark_session", return_value=mock_spark
        ):
            storage = DataLakeStorage(spark_session=mock_spark)

            # Test all predefined strategies
            data_types = [
                "transactions",
                "user_events",
                "product_updates",
                "analytics_results",
                "customer_profiles",
            ]

            for data_type in data_types:
                strategy = storage.get_partitioning_strategy(data_type)
                assert "partition_columns" in strategy
                assert "description" in strategy
                assert isinstance(strategy["partition_columns"], list)
                assert len(strategy["partition_columns"]) > 0

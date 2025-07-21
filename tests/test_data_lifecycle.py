"""
Tests for Data Lifecycle Management

Comprehensive tests covering retention policies, archiving strategies,
lineage tracking, and cost optimization functionality.
"""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

from src.data_lake.lifecycle_manager import (
    DataLifecycleManager, RetentionRule, ArchivePolicy, LineageRecord,
    RetentionPolicy, DataTier, StorageMetrics
)
from src.data_lake.lifecycle_config import LifecycleConfigManager, DEFAULT_RETENTION_RULES
from src.data_lake.delta import DeltaLakeManager


class TestDataLifecycleManager:
    """Test suite for DataLifecycleManager."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing."""
        return SparkSession.builder \
            .appName("test_lifecycle") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
    
    @pytest.fixture
    def mock_delta_manager(self, spark):
        """Create mock Delta Lake manager."""
        mock_manager = Mock(spec=DeltaLakeManager)
        mock_manager.spark = spark
        mock_manager.base_path = "s3a://test-lake/delta"
        mock_manager.tables = {}
        mock_manager.table_exists.return_value = True
        
        # Mock table creation
        def mock_create_table(df, table_name, path, **kwargs):
            mock_manager.tables[table_name] = path
            return True
        
        mock_manager.create_delta_table = mock_create_table
        mock_manager.write_to_delta.return_value = True
        
        return mock_manager
    
    @pytest.fixture
    def lifecycle_manager(self, mock_delta_manager):
        """Create DataLifecycleManager instance."""
        with patch.object(DataLifecycleManager, '_initialize_metadata_tables'):
            manager = DataLifecycleManager(mock_delta_manager)
            return manager
    
    @pytest.fixture
    def sample_retention_rule(self):
        """Create sample retention rule."""
        return RetentionRule(
            table_name="test_table",
            hot_days=30,
            warm_days=90,
            cold_days=365,
            archive_after_days=1095,
            delete_after_days=2190,
            partition_column="created_at",
            enabled=True
        )
    
    @pytest.fixture
    def sample_archive_policy(self):
        """Create sample archive policy."""
        return ArchivePolicy(
            table_name="test_table",
            archive_path="s3a://test-archive/test_table",
            compression="gzip",
            format="parquet",
            partition_by=["year", "month"],
            enabled=True
        )
    
    @pytest.fixture
    def sample_lineage_record(self):
        """Create sample lineage record."""
        return LineageRecord(
            table_name="test_table",
            operation="CREATE",
            timestamp=datetime.now(),
            source_tables=["source_table1", "source_table2"],
            target_table="test_table",
            record_count=10000,
            bytes_processed=1048576,
            job_id="job_123456",
            user="test_user",
            metadata={"job_type": "batch", "cluster_id": "cluster_001"}
        )
    
    def test_initialization(self, mock_delta_manager):
        """Test DataLifecycleManager initialization."""
        with patch.object(DataLifecycleManager, '_initialize_metadata_tables') as mock_init:
            manager = DataLifecycleManager(mock_delta_manager)
            
            assert manager.delta_manager == mock_delta_manager
            assert manager.spark == mock_delta_manager.spark
            assert manager.metadata_table == "lifecycle_metadata"
            assert isinstance(manager.retention_rules, dict)
            assert isinstance(manager.archive_policies, dict)
            assert isinstance(manager.lineage_records, list)
            
            mock_init.assert_called_once()
    
    def test_add_retention_rule(self, lifecycle_manager, sample_retention_rule):
        """Test adding retention rule."""
        lifecycle_manager.add_retention_rule(sample_retention_rule)
        
        assert "test_table" in lifecycle_manager.retention_rules
        assert lifecycle_manager.retention_rules["test_table"] == sample_retention_rule
    
    def test_add_archive_policy(self, lifecycle_manager, sample_archive_policy):
        """Test adding archive policy."""
        lifecycle_manager.add_archive_policy(sample_archive_policy)
        
        assert "test_table" in lifecycle_manager.archive_policies
        assert lifecycle_manager.archive_policies["test_table"] == sample_archive_policy
    
    def test_track_lineage(self, lifecycle_manager, sample_lineage_record, spark):
        """Test lineage tracking."""
        # Mock the write_to_delta method
        lifecycle_manager.delta_manager.write_to_delta = Mock(return_value=True)
        
        lifecycle_manager.track_lineage(sample_lineage_record)
        
        # Verify lineage record was stored
        assert len(lifecycle_manager.lineage_records) == 1
        assert lifecycle_manager.lineage_records[0] == sample_lineage_record
        
        # Verify write_to_delta was called
        lifecycle_manager.delta_manager.write_to_delta.assert_called_once()
    
    def test_apply_retention_policies_dry_run(self, lifecycle_manager, sample_retention_rule, spark):
        """Test retention policy application in dry run mode."""
        # Setup
        lifecycle_manager.add_retention_rule(sample_retention_rule)
        
        # Create mock DataFrame with time-based data
        test_data = []
        current_date = datetime.now()
        
        # Add data for different time periods
        for days_ago in [10, 50, 120, 400, 1200]:  # Hot, warm, cold, archive, delete
            timestamp = current_date - timedelta(days=days_ago)
            for i in range(100):  # 100 records per period
                test_data.append((f"record_{days_ago}_{i}", timestamp))
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        
        test_df = spark.createDataFrame(test_data, schema)
        lifecycle_manager.delta_manager.read_delta_table = Mock(return_value=test_df)
        
        # Execute dry run
        results = lifecycle_manager.apply_retention_policies(dry_run=True)
        
        # Verify results
        assert "test_table" in results
        table_result = results["test_table"]
        
        assert table_result["hot_records"] == 100    # 10 days ago
        assert table_result["warm_records"] == 100   # 50 days ago
        assert table_result["cold_records"] == 100   # 120 days ago
        assert table_result["archived_records"] == 100  # 400 days ago
        assert table_result["deleted_records"] == 100   # 1200 days ago
    
    def test_get_lineage_graph(self, lifecycle_manager, sample_lineage_record, spark):
        """Test lineage graph generation."""
        # Setup mock lineage data
        lineage_data = [
            ("test_table", "CREATE", sample_lineage_record.timestamp, 
             '["source1", "source2"]', "test_table", 1000, 1048576, "job1", "user1", "{}"),
            ("downstream_table", "INSERT", sample_lineage_record.timestamp,
             '["test_table"]', "downstream_table", 500, 524288, "job2", "user1", "{}")
        ]
        
        lineage_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("operation", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("source_tables", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("record_count", IntegerType(), True),
            StructField("bytes_processed", IntegerType(), True),
            StructField("job_id", StringType(), True),
            StructField("user", StringType(), True),
            StructField("metadata", StringType(), True)
        ])
        
        lineage_df = spark.createDataFrame(lineage_data, lineage_schema)
        lifecycle_manager.delta_manager.read_delta_table = Mock(return_value=lineage_df)
        
        # Get lineage graph
        graph = lifecycle_manager.get_lineage_graph("test_table")
        
        # Verify graph structure
        assert graph["table"] == "test_table"
        assert "source1" in graph["upstream"]
        assert "source2" in graph["upstream"]
        assert "downstream_table" in graph["downstream"]
        assert len(graph["operations"]) == 1
    
    def test_optimize_storage_costs(self, lifecycle_manager, spark):
        """Test storage cost optimization."""
        # Mock table list
        lifecycle_manager.delta_manager.tables = {"test_table1": "path1", "test_table2": "path2"}
        
        # Mock table reading
        def mock_read_table(table_name):
            # Return mock DataFrame with different sizes
            if table_name == "test_table1":
                data = [(i, f"data_{i}") for i in range(1000)]  # Smaller table
            else:
                data = [(i, f"data_{i}") for i in range(10000)]  # Larger table
            
            return spark.createDataFrame(data, ["id", "data"])
        
        lifecycle_manager.delta_manager.read_delta_table = mock_read_table
        
        # Execute optimization
        metrics = lifecycle_manager.optimize_storage_costs()
        
        # Verify metrics
        assert len(metrics) == 2
        assert "test_table1" in metrics
        assert "test_table2" in metrics
        
        # Verify metrics structure
        table1_metrics = metrics["test_table1"]
        assert isinstance(table1_metrics, StorageMetrics)
        assert table1_metrics.table_name == "test_table1"
        assert table1_metrics.record_count == 1000
        assert isinstance(table1_metrics.tier, DataTier)
    
    def test_generate_lifecycle_report(self, lifecycle_manager, sample_retention_rule):
        """Test lifecycle report generation."""
        # Setup
        lifecycle_manager.add_retention_rule(sample_retention_rule)
        
        # Mock methods
        lifecycle_manager.apply_retention_policies = Mock(return_value={
            "test_table": {"hot_records": 1000, "warm_records": 500, "cold_records": 100}
        })
        
        lifecycle_manager.optimize_storage_costs = Mock(return_value={
            "test_table": StorageMetrics(
                table_name="test_table",
                tier=DataTier.STANDARD,
                size_bytes=1048576,
                record_count=1000,
                last_accessed=datetime.now(),
                storage_cost_usd=0.023,
                access_count_30d=100,
                estimated_monthly_cost=0.69
            )
        })
        
        # Generate report
        report = lifecycle_manager.generate_lifecycle_report()
        
        # Verify report structure
        assert "timestamp" in report
        assert "retention_summary" in report
        assert "storage_summary" in report
        assert "cost_optimization" in report
        assert "lineage_summary" in report
        assert "recommendations" in report
        
        # Verify content
        assert report["storage_summary"]["total_tables"] == 1
        assert isinstance(report["recommendations"], list)
    
    def test_configuration_save_load(self, lifecycle_manager, sample_retention_rule, sample_archive_policy):
        """Test configuration save and load functionality."""
        # Add configurations
        lifecycle_manager.add_retention_rule(sample_retention_rule)
        lifecycle_manager.add_archive_policy(sample_archive_policy)
        
        # Test save
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config_path = f.name
        
        lifecycle_manager.save_configuration(config_path)
        
        # Verify file exists and has content
        assert Path(config_path).exists()
        
        with open(config_path, 'r') as f:
            saved_config = json.load(f)
        
        assert "retention_rules" in saved_config
        assert "archive_policies" in saved_config
        assert len(saved_config["retention_rules"]) == 1
        assert len(saved_config["archive_policies"]) == 1
        
        # Test load
        new_manager = DataLifecycleManager(lifecycle_manager.delta_manager)
        new_manager.load_configuration(config_path)
        
        assert "test_table" in new_manager.retention_rules
        assert "test_table" in new_manager.archive_policies
        
        # Cleanup
        Path(config_path).unlink()


class TestLifecycleConfigManager:
    """Test suite for LifecycleConfigManager."""
    
    @pytest.fixture
    def config_manager(self):
        """Create LifecycleConfigManager instance."""
        return LifecycleConfigManager()
    
    def test_initialization(self, config_manager):
        """Test config manager initialization."""
        assert len(config_manager.retention_rules) > 0
        assert len(config_manager.archive_policies) > 0
        assert len(config_manager.tier_configurations) > 0
        assert len(config_manager.automation_schedules) > 0
    
    def test_default_retention_rules(self, config_manager):
        """Test default retention rules are loaded correctly."""
        # Test specific rules
        transactions_rule = config_manager.get_retention_rule("transactions")
        assert transactions_rule.table_name == "transactions"
        assert transactions_rule.hot_days == 30
        assert transactions_rule.warm_days == 90
        assert transactions_rule.cold_days == 365
        assert transactions_rule.enabled == True
        
        user_events_rule = config_manager.get_retention_rule("user_events")
        assert user_events_rule.table_name == "user_events"
        assert user_events_rule.hot_days == 7
        assert user_events_rule.warm_days == 30
        assert user_events_rule.cold_days == 90
    
    def test_get_unknown_table_rule(self, config_manager):
        """Test getting rule for unknown table returns default."""
        unknown_rule = config_manager.get_retention_rule("unknown_table")
        assert unknown_rule.table_name == "unknown_table"
        assert unknown_rule.enabled == False  # Disabled by default for safety
        assert unknown_rule.hot_days == 30    # Default values
    
    def test_update_retention_rule(self, config_manager):
        """Test updating retention rule."""
        new_rule = RetentionRule(
            table_name="custom_table",
            hot_days=14,
            warm_days=60,
            cold_days=180,
            archive_after_days=720,
            delete_after_days=1440,
            partition_column="updated_at",
            enabled=True
        )
        
        config_manager.update_retention_rule("custom_table", new_rule)
        
        retrieved_rule = config_manager.get_retention_rule("custom_table")
        assert retrieved_rule.hot_days == 14
        assert retrieved_rule.warm_days == 60
        assert retrieved_rule.partition_column == "updated_at"
    
    def test_tier_configurations(self, config_manager):
        """Test storage tier configurations."""
        # Test cost optimized configuration
        cost_config = config_manager.get_tier_configuration("cost_optimized")
        assert DataTier.STANDARD in cost_config
        assert DataTier.ARCHIVE in cost_config
        assert cost_config[DataTier.STANDARD]["max_age_days"] == 7
        
        # Test performance optimized configuration
        perf_config = config_manager.get_tier_configuration("performance_optimized")
        assert perf_config[DataTier.STANDARD]["max_age_days"] == 30
        
        # Test unknown configuration returns default
        default_config = config_manager.get_tier_configuration("unknown_strategy")
        assert default_config == cost_config
    
    def test_export_import_configuration(self, config_manager):
        """Test configuration export and import."""
        # Modify a rule
        new_rule = RetentionRule(
            table_name="test_export",
            hot_days=15,
            warm_days=45,
            cold_days=120,
            enabled=True
        )
        config_manager.update_retention_rule("test_export", new_rule)
        
        # Export configuration
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config_path = f.name
        
        config_manager.export_configuration(config_path)
        
        # Create new manager and import
        new_manager = LifecycleConfigManager()
        new_manager.import_configuration(config_path)
        
        # Verify imported configuration
        imported_rule = new_manager.get_retention_rule("test_export")
        assert imported_rule.hot_days == 15
        assert imported_rule.warm_days == 45
        assert imported_rule.cold_days == 120
        
        # Cleanup
        Path(config_path).unlink()
    
    def test_validate_configuration(self, config_manager):
        """Test configuration validation."""
        # Add invalid rule
        invalid_rule = RetentionRule(
            table_name="invalid_table",
            hot_days=90,    # Invalid: hot > warm
            warm_days=60,
            cold_days=30,   # Invalid: cold < warm
            enabled=True
        )
        config_manager.update_retention_rule("invalid_table", invalid_rule)
        
        # Add invalid archive policy
        invalid_policy = ArchivePolicy(
            table_name="invalid_table",
            archive_path="invalid_path",  # Invalid path
            format="invalid_format",      # Invalid format
            enabled=True
        )
        config_manager.update_archive_policy("invalid_table", invalid_policy)
        
        # Validate configuration
        issues = config_manager.validate_configuration()
        
        # Should have multiple validation issues
        assert len(issues) > 0
        assert any("hot_days should be less than warm_days" in issue for issue in issues)
        assert any("warm_days should be less than cold_days" in issue for issue in issues)
        assert any("invalid_path" in issue for issue in issues)
        assert any("invalid_format" in issue for issue in issues)
    
    def test_table_lifecycle_summary(self, config_manager):
        """Test table lifecycle summary generation."""
        summary = config_manager.get_table_lifecycle_summary("transactions")
        
        # Verify summary structure
        assert "table_name" in summary
        assert "retention" in summary
        assert "archiving" in summary
        assert "estimated_lifecycle" in summary
        
        # Verify retention details
        retention = summary["retention"]
        assert retention["hot_days"] == 30
        assert retention["warm_days"] == 90
        assert retention["cold_days"] == 365
        assert retention["enabled"] == True
        
        # Verify archiving details
        archiving = summary["archiving"]
        assert "archive_path" in archiving
        assert archiving["format"] == "parquet"
        assert archiving["compression"] == "gzip"
        
        # Verify lifecycle estimation
        lifecycle = summary["estimated_lifecycle"]
        assert "hot_period" in lifecycle
        assert "warm_period" in lifecycle
        assert "cold_period" in lifecycle


class TestIntegration:
    """Integration tests for lifecycle management components."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for integration testing."""
        return SparkSession.builder \
            .appName("test_integration") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
    
    def test_end_to_end_lifecycle_management(self, spark):
        """Test complete lifecycle management workflow."""
        # Create mock Delta manager
        mock_delta_manager = Mock(spec=DeltaLakeManager)
        mock_delta_manager.spark = spark
        mock_delta_manager.base_path = "s3a://test-lake/delta"
        mock_delta_manager.tables = {"test_table": "s3a://test-lake/delta/test_table"}
        mock_delta_manager.table_exists.return_value = True
        
        # Create test data
        current_date = datetime.now()
        test_data = []
        
        # Add data for different retention periods
        for days_ago in [5, 15, 45, 120, 400]:
            timestamp = current_date - timedelta(days=days_ago)
            for i in range(50):
                test_data.append((f"record_{days_ago}_{i}", timestamp, f"data_{i}"))
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("data", StringType(), True)
        ])
        
        test_df = spark.createDataFrame(test_data, schema)
        mock_delta_manager.read_delta_table = Mock(return_value=test_df)
        mock_delta_manager.write_to_delta = Mock(return_value=True)
        mock_delta_manager.delete_from_table = Mock(return_value=50)
        
        # Initialize lifecycle manager with config
        with patch.object(DataLifecycleManager, '_initialize_metadata_tables'):
            lifecycle_manager = DataLifecycleManager(mock_delta_manager)
        
        config_manager = LifecycleConfigManager()
        
        # Add configuration
        retention_rule = config_manager.get_retention_rule("test_table")
        lifecycle_manager.add_retention_rule(retention_rule)
        
        archive_policy = config_manager.get_archive_policy("test_table") 
        lifecycle_manager.add_archive_policy(archive_policy)
        
        # Track lineage
        lineage_record = LineageRecord(
            table_name="test_table",
            operation="INSERT",
            timestamp=datetime.now(),
            source_tables=["source_data"],
            target_table="test_table",
            record_count=250,
            bytes_processed=1048576,
            job_id="test_job_123",
            user="test_user",
            metadata={"test": "integration"}
        )
        
        lifecycle_manager.track_lineage(lineage_record)
        
        # Apply retention policies
        retention_results = lifecycle_manager.apply_retention_policies(dry_run=False)
        
        # Verify retention was applied
        assert "test_table" in retention_results
        assert retention_results["test_table"]["hot_records"] == 100  # 5 and 15 days ago
        
        # Optimize storage
        storage_metrics = lifecycle_manager.optimize_storage_costs()
        
        # Verify optimization results
        assert "test_table" in storage_metrics
        assert isinstance(storage_metrics["test_table"], StorageMetrics)
        
        # Generate comprehensive report
        report = lifecycle_manager.generate_lifecycle_report()
        
        # Verify report completeness
        assert "timestamp" in report
        assert "retention_summary" in report
        assert "storage_summary" in report
        assert "lineage_summary" in report
        assert report["lineage_summary"]["total_operations"] == 1
        assert report["storage_summary"]["total_tables"] == 1
        
        # Verify lineage tracking
        lineage_graph = lifecycle_manager.get_lineage_graph("test_table")
        assert lineage_graph["table"] == "test_table"
        assert "source_data" in lineage_graph["upstream"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
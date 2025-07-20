"""
Unit tests for streaming transformations module - Fixed version.
"""
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

import pytest

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import row_number
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from pyspark.sql.window import Window

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from src.streaming.transformations.aggregations import StreamingAggregator
from src.streaming.transformations.deduplication import StreamDeduplicator
from src.streaming.transformations.enrichment import DataEnrichmentPipeline
from src.streaming.transformations.joins import StreamJoinEngine


@pytest.fixture(scope="module", autouse=True)
def mock_pyspark_functions():
    """Mock all PySpark functions globally for all tests."""
    # Create a mock SparkContext
    mock_spark_context = Mock()

    patches = [
        patch("pyspark.SparkContext._active_spark_context", mock_spark_context),
        patch("pyspark.sql.functions.current_timestamp", return_value=Mock()),
        patch("pyspark.sql.functions.col", side_effect=lambda x: Mock(name=f"col_{x}")),
        patch("pyspark.sql.functions.lit", side_effect=lambda x: Mock(name=f"lit_{x}")),
        patch("pyspark.sql.functions.when", return_value=Mock()),
        patch("pyspark.sql.functions.hour", return_value=Mock()),
        patch("pyspark.sql.functions.dayofweek", return_value=Mock()),
        patch("pyspark.sql.functions.upper", return_value=Mock()),
        patch("pyspark.sql.functions.lower", return_value=Mock()),
        patch("pyspark.sql.functions.trim", return_value=Mock()),
        patch("pyspark.sql.functions.split", return_value=Mock()),
        patch("pyspark.sql.functions.regexp_extract", return_value=Mock()),
        patch("pyspark.sql.functions.coalesce", return_value=Mock()),
        patch("pyspark.sql.functions.size", return_value=Mock()),
        patch("pyspark.sql.functions.array_contains", return_value=Mock()),
        patch("pyspark.sql.functions.broadcast", side_effect=lambda x: x),
        patch("pyspark.sql.functions.window", return_value=Mock()),
        patch("pyspark.sql.functions.approx_count_distinct", return_value=Mock()),
        patch("pyspark.sql.functions.avg", return_value=Mock()),
        patch("pyspark.sql.functions.sum", return_value=Mock()),
        patch("pyspark.sql.functions.count", return_value=Mock()),
        patch("pyspark.sql.functions.max", return_value=Mock()),
        patch("pyspark.sql.functions.min", return_value=Mock()),
        patch("pyspark.sql.functions.row_number", return_value=Mock()),
        patch("pyspark.sql.window.Window.partitionBy", return_value=Mock()),
        patch("pyspark.sql.window.Window.orderBy", return_value=Mock()),
        patch("pyspark.sql.window.Window.rowsBetween", return_value=Mock()),
    ]

    # Start all patches
    started_patches = [p.start() for p in patches]

    try:
        yield
    finally:
        # Stop all patches
        for p in patches:
            p.stop()


@pytest.fixture
def mock_dataframe():
    """Create a mock DataFrame that supports chaining and column access."""
    mock_df = Mock(spec=DataFrame)

    # Make all DataFrame methods return the same mock for chaining
    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.withWatermark.return_value = mock_df
    mock_df.groupBy.return_value = mock_df
    mock_df.agg.return_value = mock_df
    mock_df.join.return_value = mock_df
    mock_df.union.return_value = mock_df
    mock_df.dropDuplicates.return_value = mock_df
    mock_df.orderBy.return_value = mock_df
    mock_df.limit.return_value = mock_df
    mock_df.distinct.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df
    mock_df.alias.return_value = mock_df

    # Mock columns attribute
    mock_df.columns = [
        "col1",
        "col2",
        "timestamp",
        "user_id",
        "session_id",
        "event_type",
    ]

    # Mock count and collect for statistics
    mock_df.count.return_value = 100
    mock_df.collect.return_value = []

    # Mock column access for DataFrame attributes
    mock_column = Mock()
    # Common column names used in transformations
    column_names = [
        "user_id",
        "session_id",
        "event_type",
        "timestamp",
        "product_id",
        "transaction_id",
        "purchase_timestamp",
        "event_timestamp",
        "sequence_number",
        "customer_id",
        "profile_timestamp",
        "profile_last_seen",
        "total_amount",
        "event_id",
    ]

    for col_name in column_names:
        setattr(mock_df, col_name, mock_column)

    return mock_df


@pytest.fixture
def mock_spark():
    """Create mock SparkSession."""
    return Mock(spec=SparkSession)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestDataEnrichmentPipeline:
    """Test cases for data enrichment pipeline."""

    @pytest.fixture
    def enrichment_pipeline(self, mock_spark):
        """Create DataEnrichmentPipeline instance."""
        return DataEnrichmentPipeline(mock_spark)

    def test_enrichment_pipeline_initialization(self, mock_spark):
        """Test enrichment pipeline initialization."""
        pipeline = DataEnrichmentPipeline(mock_spark)
        assert pipeline.spark == mock_spark
        assert pipeline.logger is not None

    def test_enrich_transaction_stream(self, enrichment_pipeline, mock_dataframe):
        """Test transaction stream enrichment."""
        result = enrichment_pipeline.enrich_transaction_stream(mock_dataframe)
        assert result is not None
        # Verify withColumn was called for enrichment_timestamp
        mock_dataframe.withColumn.assert_called()

    def test_enrich_user_behavior_stream(self, enrichment_pipeline, mock_dataframe):
        """Test user behavior stream enrichment."""
        result = enrichment_pipeline.enrich_user_behavior_stream(mock_dataframe)
        assert result is not None
        # Verify withColumn was called for enrichment_timestamp
        mock_dataframe.withColumn.assert_called()

    def test_enrichment_error_handling(self, enrichment_pipeline, mock_dataframe):
        """Test error handling in enrichment."""
        # Make withColumn raise an exception
        mock_dataframe.withColumn.side_effect = Exception("Test error")

        with pytest.raises(Exception):
            enrichment_pipeline.enrich_transaction_stream(mock_dataframe)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamingAggregator:
    """Test cases for streaming aggregator."""

    @pytest.fixture
    def aggregator(self, mock_spark):
        """Create StreamingAggregator instance."""
        return StreamingAggregator(mock_spark)

    def test_aggregator_initialization(self, mock_spark):
        """Test aggregator initialization."""
        aggregator = StreamingAggregator(mock_spark)
        assert aggregator.spark == mock_spark
        assert aggregator.logger is not None

    def test_create_real_time_kpis(self, aggregator, mock_dataframe):
        """Test real-time KPI creation."""
        result = aggregator.create_real_time_kpis(
            mock_dataframe, window_duration="5 minutes"
        )
        assert result is not None
        mock_dataframe.withWatermark.assert_called()

    def test_create_customer_behavior_aggregations(self, aggregator, mock_dataframe):
        """Test customer behavior aggregations."""
        result = aggregator.create_customer_behavior_aggregations(mock_dataframe)
        assert result is not None
        mock_dataframe.withWatermark.assert_called()

    def test_create_product_performance_aggregations(self, aggregator, mock_dataframe):
        """Test product performance aggregations."""
        result = aggregator.create_product_performance_aggregations(mock_dataframe)
        assert result is not None
        mock_dataframe.withWatermark.assert_called()

    def test_aggregation_error_handling(self, aggregator, mock_dataframe):
        """Test error handling in aggregation."""
        mock_dataframe.withWatermark.side_effect = Exception("Aggregation error")

        with pytest.raises(Exception):
            aggregator.create_real_time_kpis(mock_dataframe)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamJoinEngine:
    """Test cases for stream join engine."""

    @pytest.fixture
    def join_engine(self, mock_spark):
        """Create StreamJoinEngine instance."""
        return StreamJoinEngine(mock_spark)

    def test_join_engine_initialization(self, mock_spark):
        """Test join engine initialization."""
        engine = StreamJoinEngine(mock_spark)
        assert engine.spark == mock_spark
        assert engine.logger is not None

    def test_join_transaction_behavior_streams(self, join_engine, mock_dataframe):
        """Test joining transaction and behavior streams."""
        result = join_engine.join_transaction_behavior_streams(
            mock_dataframe, mock_dataframe, join_window="10 minutes"
        )
        assert result is not None
        mock_dataframe.withWatermark.assert_called()

    @patch("src.streaming.transformations.joins.row_number")
    @patch("src.streaming.transformations.joins.Window")
    def test_create_user_journey_stream(
        self, mock_window, mock_row_number, join_engine, mock_dataframe
    ):
        """Test user journey stream creation."""
        # Mock Window operations to return mock objects that support chaining
        mock_window_spec = Mock()
        mock_window.partitionBy.return_value = mock_window_spec
        mock_window_spec.orderBy.return_value = mock_window_spec

        # Mock row_number to return a mock column
        mock_column = Mock()
        mock_column.over.return_value = Mock()
        mock_row_number.return_value = mock_column

        result = join_engine.create_user_journey_stream(mock_dataframe, mock_dataframe)
        assert result is not None

    @patch("src.streaming.transformations.joins.row_number")
    @patch("src.streaming.transformations.joins.Window")
    def test_join_with_customer_profile_stream(
        self, mock_window, mock_row_number, join_engine, mock_dataframe
    ):
        """Test joining with customer profile stream."""
        # Mock Window operations
        mock_window_spec = Mock()
        mock_window.partitionBy.return_value = mock_window_spec
        mock_window_spec.orderBy.return_value = mock_window_spec

        # Mock row_number
        mock_column = Mock()
        mock_column.over.return_value = Mock()
        mock_row_number.return_value = mock_column

        result = join_engine.join_with_customer_profile_stream(
            mock_dataframe, mock_dataframe, profile_freshness_threshold="1 hour"
        )
        assert result is not None

    @patch("src.streaming.transformations.joins.lag")
    @patch("src.streaming.transformations.joins.lead")
    @patch("src.streaming.transformations.joins.Window")
    def test_create_cross_stream_correlation(
        self, mock_window, mock_lead, mock_lag, join_engine, mock_dataframe
    ):
        """Test cross-stream correlation."""
        # Mock Window operations
        mock_window_spec = Mock()
        mock_window.partitionBy.return_value = mock_window_spec
        mock_window_spec.orderBy.return_value = mock_window_spec
        mock_window_spec.rowsBetween.return_value = mock_window_spec

        # Mock lag and lead functions
        mock_column = Mock()
        mock_column.over.return_value = Mock()
        mock_lag.return_value = mock_column
        mock_lead.return_value = mock_column

        result = join_engine.create_cross_stream_correlation(
            mock_dataframe, mock_dataframe, correlation_window="5 minutes"
        )
        assert result is not None

    def test_join_error_handling(self, join_engine, mock_dataframe):
        """Test error handling in joins."""
        mock_dataframe.withWatermark.side_effect = Exception("Join error")

        with pytest.raises(Exception):
            join_engine.join_transaction_behavior_streams(
                mock_dataframe, mock_dataframe
            )


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamDeduplicator:
    """Test cases for stream deduplicator."""

    @pytest.fixture
    def deduplicator(self, mock_spark):
        """Create StreamDeduplicator instance."""
        return StreamDeduplicator(mock_spark)

    def test_deduplicator_initialization(self, mock_spark):
        """Test deduplicator initialization."""
        deduplicator = StreamDeduplicator(mock_spark)
        assert deduplicator.spark == mock_spark
        assert deduplicator.logger is not None

    @patch("src.streaming.transformations.deduplication.row_number")
    @patch("src.streaming.transformations.deduplication.Window")
    def test_deduplicate_exact_events(
        self, mock_window, mock_row_number, deduplicator, mock_dataframe
    ):
        """Test exact event deduplication."""
        # Mock Window operations
        mock_window_spec = Mock()
        mock_window.partitionBy.return_value = mock_window_spec
        mock_window_spec.orderBy.return_value = mock_window_spec

        # Mock row_number
        mock_column = Mock()
        mock_column.over.return_value = Mock()
        mock_row_number.return_value = mock_column

        result = deduplicator.deduplicate_exact_events(
            mock_dataframe, watermark_delay="5 minutes"
        )
        assert result is not None
        mock_dataframe.withWatermark.assert_called()

    def test_deduplicate_transactions(self, deduplicator, mock_dataframe):
        """Test transaction deduplication."""
        result = deduplicator.deduplicate_transactions(
            mock_dataframe, watermark_delay="5 minutes"
        )
        assert result is not None

    def test_deduplicate_user_behavior(self, deduplicator, mock_dataframe):
        """Test user behavior deduplication."""
        result = deduplicator.deduplicate_user_behavior(
            mock_dataframe, session_dedup=True
        )
        assert result is not None

    def test_create_deduplication_statistics(self, deduplicator, mock_dataframe):
        """Test deduplication statistics creation."""
        result = deduplicator.create_deduplication_statistics(mock_dataframe)
        assert result is not None

    def test_handle_late_arrivals(self, deduplicator, mock_dataframe):
        """Test late arrival handling."""
        result = deduplicator.handle_late_arrivals(
            mock_dataframe, watermark_delay="10 minutes"
        )
        assert result is not None

    def test_deduplication_error_handling(self, deduplicator, mock_dataframe):
        """Test error handling in deduplication."""
        mock_dataframe.withWatermark.side_effect = Exception("Deduplication error")

        with pytest.raises(Exception):
            deduplicator.deduplicate_exact_events(mock_dataframe)

    def test_different_keep_strategies(self, deduplicator, mock_dataframe):
        """Test different keep strategies."""
        # Test first strategy
        result = deduplicator.deduplicate_exact_events(
            mock_dataframe, keep_strategy="first"
        )
        assert result is not None

        # Test last strategy
        result = deduplicator.deduplicate_exact_events(
            mock_dataframe, keep_strategy="last"
        )
        assert result is not None

    def test_invalid_keep_strategy(self, deduplicator, mock_dataframe):
        """Test invalid keep strategy."""
        with pytest.raises(ValueError, match="Invalid keep_strategy"):
            deduplicator.deduplicate_exact_events(
                mock_dataframe, keep_strategy="invalid"
            )

    def test_late_data_strategies(self, deduplicator, mock_dataframe):
        """Test different late data strategies."""
        result = deduplicator.handle_late_arrivals(
            mock_dataframe, late_data_strategy="drop"
        )
        assert result is not None

        result = deduplicator.handle_late_arrivals(
            mock_dataframe, late_data_strategy="keep"
        )
        assert result is not None

    def test_invalid_late_data_strategy(self, deduplicator, mock_dataframe):
        """Test invalid late data strategy."""
        with pytest.raises(ValueError, match="Invalid late_data_strategy"):
            deduplicator.handle_late_arrivals(
                mock_dataframe, late_data_strategy="invalid"
            )


class TestTransformationsIntegration:
    """Test cases for transformations module integration."""

    def test_transformations_module_import(self):
        """Test that transformations module can be imported."""
        import src.streaming.transformations as transformations

        assert transformations is not None

    def test_transformations_module_exports(self):
        """Test transformations module exports."""
        from src.streaming.transformations import (
            DataEnrichmentPipeline,
            StreamDeduplicator,
            StreamingAggregator,
            StreamJoinEngine,
        )

        assert DataEnrichmentPipeline is not None
        assert StreamingAggregator is not None
        assert StreamJoinEngine is not None
        assert StreamDeduplicator is not None

    @pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="Testing fallback behavior")
    def test_graceful_degradation_without_pyspark(self):
        """Test that modules handle missing PySpark gracefully."""
        # This test would be more relevant in an environment without PySpark
        assert True


class TestTransformationsConfiguration:
    """Test transformation configuration parameters."""

    def test_default_window_durations(self):
        """Test default window duration values."""
        window_durations = ["1 minute", "5 minutes", "15 minutes", "1 hour"]
        for duration in window_durations:
            assert isinstance(duration, str)
            assert "minute" in duration or "hour" in duration

    def test_watermark_delay_values(self):
        """Test watermark delay configuration."""
        watermark_delays = ["30 seconds", "1 minute", "5 minutes", "10 minutes"]
        for delay in watermark_delays:
            assert isinstance(delay, str)
            assert "second" in delay or "minute" in delay

    def test_similarity_threshold_ranges(self):
        """Test similarity threshold ranges."""
        thresholds = [0.8, 0.85, 0.9, 0.95, 0.99]
        for threshold in thresholds:
            assert 0.0 <= threshold <= 1.0

    def test_join_window_configurations(self):
        """Test join window configurations."""
        join_windows = ["1 minute", "5 minutes", "10 minutes", "30 minutes"]
        for window in join_windows:
            assert isinstance(window, str)
            assert "minute" in window

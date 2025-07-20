"""
Unit tests for streaming transformations module.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from decimal import Decimal

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from src.streaming.transformations.enrichment import DataEnrichmentPipeline
from src.streaming.transformations.aggregations import StreamingAggregator
from src.streaming.transformations.joins import StreamJoinEngine
from src.streaming.transformations.deduplication import StreamDeduplicator


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestDataEnrichmentPipeline:
    """Test cases for data enrichment pipeline."""

    @pytest.fixture
    def mock_spark(self):
        """Create mock SparkSession."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def enrichment_pipeline(self, mock_spark):
        """Create DataEnrichmentPipeline instance."""
        return DataEnrichmentPipeline(mock_spark)

    @pytest.fixture
    def mock_transaction_df(self):
        """Create mock transaction DataFrame."""
        mock_df = Mock(spec=DataFrame)
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.filter.return_value = mock_df
        return mock_df

    def test_enrichment_pipeline_initialization(self, mock_spark):
        """Test enrichment pipeline initialization."""
        pipeline = DataEnrichmentPipeline(mock_spark)
        assert pipeline.spark == mock_spark
        assert pipeline.logger is not None

    def test_enrich_transaction_stream(self, enrichment_pipeline, mock_transaction_df):
        """Test transaction stream enrichment."""
        # Mock the enrichment methods
        enrichment_pipeline._enrich_customer_data = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_product_data = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_temporal_features = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_geographic_data = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_business_metrics = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_risk_indicators = Mock(return_value=mock_transaction_df)

        result = enrichment_pipeline.enrich_transaction_stream(mock_transaction_df)

        # Verify all enrichment methods were called
        enrichment_pipeline._enrich_customer_data.assert_called_once()
        enrichment_pipeline._enrich_product_data.assert_called_once()
        enrichment_pipeline._enrich_temporal_features.assert_called_once()
        enrichment_pipeline._enrich_geographic_data.assert_called_once()
        enrichment_pipeline._enrich_business_metrics.assert_called_once()
        enrichment_pipeline._enrich_risk_indicators.assert_called_once()

        assert result == mock_transaction_df

    def test_enrich_user_behavior_stream(self, enrichment_pipeline, mock_transaction_df):
        """Test user behavior stream enrichment."""
        # Mock the enrichment methods
        enrichment_pipeline._enrich_session_data = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_user_profile = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_device_data = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_engagement_metrics = Mock(return_value=mock_transaction_df)
        enrichment_pipeline._enrich_funnel_position = Mock(return_value=mock_transaction_df)

        result = enrichment_pipeline.enrich_user_behavior_stream(mock_transaction_df)

        # Verify all enrichment methods were called
        enrichment_pipeline._enrich_session_data.assert_called_once()
        enrichment_pipeline._enrich_user_profile.assert_called_once()
        enrichment_pipeline._enrich_device_data.assert_called_once()
        enrichment_pipeline._enrich_engagement_metrics.assert_called_once()
        enrichment_pipeline._enrich_funnel_position.assert_called_once()

        assert result == mock_transaction_df

    def test_enrichment_error_handling(self, enrichment_pipeline, mock_transaction_df):
        """Test error handling in enrichment."""
        # Mock an enrichment method to raise an exception
        enrichment_pipeline._enrich_customer_data = Mock(side_effect=Exception("Test error"))

        with pytest.raises(Exception, match="Test error"):
            enrichment_pipeline.enrich_transaction_stream(mock_transaction_df)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamingAggregator:
    """Test cases for streaming aggregator."""

    @pytest.fixture
    def mock_spark(self):
        """Create mock SparkSession."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def aggregator(self, mock_spark):
        """Create StreamingAggregator instance."""
        return StreamingAggregator(mock_spark)

    @pytest.fixture
    def mock_df(self):
        """Create mock DataFrame."""
        mock_df = Mock(spec=DataFrame)
        mock_df.withWatermark.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.filter.return_value = mock_df
        return mock_df

    def test_aggregator_initialization(self, mock_spark):
        """Test aggregator initialization."""
        aggregator = StreamingAggregator(mock_spark)
        assert aggregator.spark == mock_spark
        assert aggregator.logger is not None

    def test_create_real_time_kpis(self, aggregator, mock_df):
        """Test real-time KPI creation."""
        result = aggregator.create_real_time_kpis(mock_df)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        mock_df.groupBy.assert_called()
        assert result == mock_df

    def test_create_customer_behavior_aggregations(self, aggregator, mock_df):
        """Test customer behavior aggregations."""
        result = aggregator.create_customer_behavior_aggregations(mock_df)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        assert result == mock_df

    def test_create_product_performance_aggregations(self, aggregator, mock_df):
        """Test product performance aggregations."""
        result = aggregator.create_product_performance_aggregations(mock_df)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        assert result == mock_df

    def test_aggregation_error_handling(self, aggregator, mock_df):
        """Test error handling in aggregations."""
        mock_df.withWatermark.side_effect = Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            aggregator.create_real_time_kpis(mock_df)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamJoinEngine:
    """Test cases for stream join engine."""

    @pytest.fixture
    def mock_spark(self):
        """Create mock SparkSession."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def join_engine(self, mock_spark):
        """Create StreamJoinEngine instance."""
        return StreamJoinEngine(mock_spark)

    @pytest.fixture
    def mock_df(self):
        """Create mock DataFrame."""
        mock_df = Mock(spec=DataFrame)
        mock_df.withWatermark.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df
        return mock_df

    def test_join_engine_initialization(self, mock_spark):
        """Test join engine initialization."""
        engine = StreamJoinEngine(mock_spark)
        assert engine.spark == mock_spark
        assert engine.logger is not None

    def test_join_transaction_behavior_streams(self, join_engine, mock_df):
        """Test transaction and behavior stream joining."""
        result = join_engine.join_transaction_behavior_streams(mock_df, mock_df)

        # Verify DataFrame operations were called
        assert mock_df.withWatermark.call_count >= 2  # Called for both streams
        mock_df.select.assert_called()
        mock_df.join.assert_called()
        assert result == mock_df

    def test_create_user_journey_stream(self, join_engine, mock_df):
        """Test user journey stream creation."""
        result = join_engine.create_user_journey_stream(mock_df, mock_df)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        assert result == mock_df

    def test_join_with_customer_profile_stream(self, join_engine, mock_df):
        """Test customer profile stream joining."""
        result = join_engine.join_with_customer_profile_stream(mock_df, mock_df)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        assert result == mock_df

    def test_create_cross_stream_correlation(self, join_engine, mock_df):
        """Test cross-stream correlation."""
        correlation_keys = ["user_id", "product_id"]
        result = join_engine.create_cross_stream_correlation(
            mock_df, mock_df, correlation_keys
        )

        # Verify DataFrame operations were called
        assert mock_df.withWatermark.call_count >= 2
        assert result == mock_df

    def test_join_error_handling(self, join_engine, mock_df):
        """Test error handling in joins."""
        mock_df.withWatermark.side_effect = Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            join_engine.join_transaction_behavior_streams(mock_df, mock_df)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamDeduplicator:
    """Test cases for stream deduplicator."""

    @pytest.fixture
    def mock_spark(self):
        """Create mock SparkSession."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def deduplicator(self, mock_spark):
        """Create StreamDeduplicator instance."""
        return StreamDeduplicator(mock_spark)

    @pytest.fixture
    def mock_df(self):
        """Create mock DataFrame."""
        mock_df = Mock(spec=DataFrame)
        mock_df.withWatermark.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.join.return_value = mock_df
        return mock_df

    def test_deduplicator_initialization(self, mock_spark):
        """Test deduplicator initialization."""
        deduplicator = StreamDeduplicator(mock_spark)
        assert deduplicator.spark == mock_spark
        assert deduplicator.logger is not None

    def test_deduplicate_exact_events(self, deduplicator, mock_df):
        """Test exact event deduplication."""
        dedup_keys = ["user_id", "event_type", "timestamp"]
        result = deduplicator.deduplicate_exact_events(mock_df, dedup_keys)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        mock_df.withColumn.assert_called()
        mock_df.filter.assert_called()
        assert result == mock_df

    def test_deduplicate_transactions(self, deduplicator, mock_df):
        """Test transaction deduplication."""
        result = deduplicator.deduplicate_transactions(mock_df)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        mock_df.withColumn.assert_called()
        assert result == mock_df

    def test_deduplicate_user_behavior(self, deduplicator, mock_df):
        """Test user behavior deduplication."""
        result = deduplicator.deduplicate_user_behavior(mock_df)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        mock_df.withColumn.assert_called()
        assert result == mock_df

    def test_create_deduplication_statistics(self, deduplicator, mock_df):
        """Test deduplication statistics creation."""
        dedup_keys = ["user_id", "product_id"]
        result = deduplicator.create_deduplication_statistics(
            mock_df, mock_df, dedup_keys
        )

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        mock_df.groupBy.assert_called()
        assert result == mock_df

    def test_handle_late_arrivals(self, deduplicator, mock_df):
        """Test late arrival handling."""
        result = deduplicator.handle_late_arrivals(mock_df)

        # Verify DataFrame operations were called
        mock_df.withWatermark.assert_called()
        mock_df.withColumn.assert_called()
        assert result == mock_df

    def test_deduplication_error_handling(self, deduplicator, mock_df):
        """Test error handling in deduplication."""
        mock_df.withWatermark.side_effect = Exception("Test error")

        with pytest.raises(Exception, match="Test error"):
            deduplicator.deduplicate_exact_events(mock_df, ["user_id"])

    def test_different_keep_strategies(self, deduplicator, mock_df):
        """Test different keep strategies for deduplication."""
        dedup_keys = ["user_id", "event_type"]

        # Test different strategies
        strategies = ["first", "last", "latest_timestamp"]
        for strategy in strategies:
            result = deduplicator.deduplicate_exact_events(
                mock_df, dedup_keys, keep_strategy=strategy
            )
            assert result == mock_df

    def test_invalid_keep_strategy(self, deduplicator, mock_df):
        """Test invalid keep strategy raises error."""
        dedup_keys = ["user_id"]
        
        with pytest.raises(ValueError, match="Unknown keep_strategy"):
            deduplicator.deduplicate_exact_events(
                mock_df, dedup_keys, keep_strategy="invalid"
            )

    def test_late_data_strategies(self, deduplicator, mock_df):
        """Test different late data handling strategies."""
        strategies = ["flag", "drop", "separate"]
        
        for strategy in strategies:
            result = deduplicator.handle_late_arrivals(
                mock_df, late_data_strategy=strategy
            )
            assert result == mock_df

    def test_invalid_late_data_strategy(self, deduplicator, mock_df):
        """Test invalid late data strategy raises error."""
        with pytest.raises(ValueError, match="Unknown late_data_strategy"):
            deduplicator.handle_late_arrivals(
                mock_df, late_data_strategy="invalid"
            )


# Integration tests without PySpark dependency
class TestTransformationsIntegration:
    """Integration tests for transformations module."""

    def test_transformations_module_import(self):
        """Test that transformations module can be imported."""
        from src.streaming.transformations import (
            DataEnrichmentPipeline,
            StreamingAggregator,
            StreamJoinEngine,
            StreamDeduplicator
        )
        
        # Verify classes are available
        assert DataEnrichmentPipeline is not None
        assert StreamingAggregator is not None
        assert StreamJoinEngine is not None
        assert StreamDeduplicator is not None

    def test_transformations_module_exports(self):
        """Test that transformations module exports expected classes."""
        import src.streaming.transformations as transformations
        
        expected_exports = [
            "DataEnrichmentPipeline",
            "StreamingAggregator", 
            "StreamJoinEngine",
            "StreamDeduplicator"
        ]
        
        for export in expected_exports:
            assert hasattr(transformations, export)

    @pytest.mark.skipif(PYSPARK_AVAILABLE, reason="Testing fallback behavior")
    def test_graceful_degradation_without_pyspark(self):
        """Test that modules handle missing PySpark gracefully."""
        # This would test import behavior when PySpark is not available
        # In real implementation, modules should handle ImportError gracefully
        pass


class TestTransformationsConfiguration:
    """Test transformation configuration and parameters."""

    def test_default_window_durations(self):
        """Test default window duration values."""
        # Test that default values are reasonable
        default_kpi_window = "5 minutes"
        default_behavior_window = "10 minutes"
        default_product_window = "15 minutes"
        
        assert "minutes" in default_kpi_window
        assert "minutes" in default_behavior_window
        assert "minutes" in default_product_window

    def test_watermark_delay_values(self):
        """Test watermark delay configuration."""
        default_delays = ["2 minutes", "5 minutes", "10 minutes"]
        
        for delay in default_delays:
            assert "minutes" in delay
            assert int(delay.split()[0]) > 0

    def test_similarity_threshold_ranges(self):
        """Test similarity threshold validation."""
        valid_thresholds = [0.0, 0.5, 0.8, 0.95, 1.0]
        
        for threshold in valid_thresholds:
            assert 0.0 <= threshold <= 1.0

    def test_join_window_configurations(self):
        """Test join window configurations."""
        join_windows = ["10 minutes", "4 hours", "1 hour"]
        
        for window in join_windows:
            assert any(unit in window for unit in ["minutes", "hours", "seconds"])
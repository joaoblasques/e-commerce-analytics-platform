"""
Real-time data transformations for streaming analytics.

This package provides comprehensive transformation capabilities for streaming data:
- Data enrichment pipelines
- Real-time aggregations (windowed)
- Stream-to-stream joins
- Data deduplication logic
"""

from .enrichment import DataEnrichmentPipeline
from .aggregations import StreamingAggregator
from .joins import StreamJoinEngine
from .deduplication import StreamDeduplicator

__all__ = [
    "DataEnrichmentPipeline",
    "StreamingAggregator", 
    "StreamJoinEngine",
    "StreamDeduplicator",
]
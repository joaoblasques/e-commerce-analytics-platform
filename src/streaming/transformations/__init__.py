"""
Real-time data transformations for streaming analytics.

This package provides comprehensive transformation capabilities for streaming:
- Data enrichment pipelines
- Real-time aggregations (windowed)
- Stream-to-stream joins
- Data deduplication logic
"""

from .aggregations import StreamingAggregator
from .deduplication import StreamDeduplicator
from .enrichment import DataEnrichmentPipeline
from .joins import StreamJoinEngine

__all__ = [
    "DataEnrichmentPipeline",
    "StreamingAggregator",
    "StreamJoinEngine",
    "StreamDeduplicator",
]

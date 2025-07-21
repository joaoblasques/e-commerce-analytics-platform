"""
Data Lake Storage Layer

This module provides data lake storage functionality including:
- Parquet file storage with optimal partitioning
- Automated data ingestion to S3/MinIO
- Data compaction and optimization
- Metadata management and cataloging
"""

from .compaction import DataCompactor
from .ingestion import DataLakeIngester
from .metadata import MetadataManager
from .storage import DataLakeStorage

__all__ = ["DataLakeStorage", "DataLakeIngester", "DataCompactor", "MetadataManager"]

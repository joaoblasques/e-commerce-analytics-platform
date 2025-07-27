"""
Data Catalog and Schema Management.

Provides comprehensive data cataloging capabilities including:
- Schema discovery and registration
- Metadata management
- Data asset inventory
- Classification and tagging
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


@dataclass
class DataAsset:
    """Represents a data asset in the catalog."""

    name: str
    path: str
    format: str
    schema: Dict[str, Any]
    description: str
    owner: str
    created_at: datetime
    updated_at: datetime
    tags: List[str]
    classification: str  # public, internal, confidential, restricted
    retention_days: int
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    partition_columns: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        data["updated_at"] = self.updated_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataAsset":
        """Create from dictionary."""
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        data["updated_at"] = datetime.fromisoformat(data["updated_at"])
        return cls(**data)


class DataCatalog:
    """
    Data Catalog for managing data assets and metadata.

    Provides capabilities for:
    - Schema discovery and registration
    - Metadata management
    - Data classification
    - Asset inventory
    """

    def __init__(
        self, catalog_path: str = "data/catalog", spark: Optional[SparkSession] = None
    ):
        """
        Initialize data catalog.

        Args:
            catalog_path: Path to store catalog metadata
            spark: Optional Spark session for schema discovery
        """
        self.catalog_path = Path(catalog_path)
        self.catalog_path.mkdir(parents=True, exist_ok=True)
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # Catalog storage
        self.assets_file = self.catalog_path / "assets.json"
        self.schemas_dir = self.catalog_path / "schemas"
        self.schemas_dir.mkdir(exist_ok=True)

        # Load existing catalog
        self.assets = self._load_assets()

    def _load_assets(self) -> Dict[str, DataAsset]:
        """Load existing assets from storage."""
        if not self.assets_file.exists():
            return {}

        try:
            with open(self.assets_file, "r") as f:
                data = json.load(f)
            return {
                name: DataAsset.from_dict(asset_data)
                for name, asset_data in data.items()
            }
        except Exception as e:
            self.logger.error(f"Error loading assets: {e}")
            return {}

    def _save_assets(self):
        """Save assets to storage."""
        try:
            data = {name: asset.to_dict() for name, asset in self.assets.items()}
            with open(self.assets_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving assets: {e}")

    def register_asset(
        self,
        name: str,
        path: str,
        format: str,
        description: str,
        owner: str,
        classification: str = "internal",
        tags: Optional[List[str]] = None,
        retention_days: int = 365,
        auto_discover_schema: bool = True,
    ) -> DataAsset:
        """
        Register a new data asset in the catalog.

        Args:
            name: Asset name
            path: Data path
            format: Data format (parquet, delta, json, etc.)
            description: Asset description
            owner: Data owner
            classification: Data classification level
            tags: Asset tags
            retention_days: Data retention period
            auto_discover_schema: Whether to automatically discover schema

        Returns:
            Created DataAsset
        """
        tags = tags or []
        now = datetime.now()

        # Discover schema if requested
        schema = {}
        size_bytes = None
        row_count = None
        partition_columns = None

        if auto_discover_schema:
            try:
                schema_info = self._discover_schema(path, format)
                schema = schema_info.get("schema", {})
                size_bytes = schema_info.get("size_bytes")
                row_count = schema_info.get("row_count")
                partition_columns = schema_info.get("partition_columns")
            except Exception as e:
                self.logger.warning(f"Failed to discover schema for {name}: {e}")

        # Create asset
        asset = DataAsset(
            name=name,
            path=path,
            format=format,
            schema=schema,
            description=description,
            owner=owner,
            created_at=now,
            updated_at=now,
            tags=tags,
            classification=classification,
            retention_days=retention_days,
            size_bytes=size_bytes,
            row_count=row_count,
            partition_columns=partition_columns,
        )

        # Store asset
        self.assets[name] = asset
        self._save_assets()

        # Save schema separately
        self._save_schema(name, schema)

        self.logger.info(f"Registered asset: {name}")
        return asset

    def _discover_schema(self, path: str, format: str) -> Dict[str, Any]:
        """
        Discover schema and metadata for a data asset.

        Args:
            path: Data path
            format: Data format

        Returns:
            Schema information dictionary
        """
        if not self.spark:
            return {}

        try:
            # Read data based on format
            if format.lower() == "delta":
                df = self.spark.read.format("delta").load(path)
            elif format.lower() == "parquet":
                df = self.spark.read.parquet(path)
            elif format.lower() == "json":
                df = self.spark.read.json(path)
            elif format.lower() == "csv":
                df = self.spark.read.option("header", "true").csv(path)
            else:
                return {}

            # Get schema
            schema = self._spark_schema_to_dict(df.schema)

            # Get statistics (with caching for performance)
            try:
                df.cache()
                row_count = df.count()
                size_bytes = None  # Would need to calculate from file system

                # Get partition columns if available
                partition_columns = []
                if (
                    hasattr(df, "rdd")
                    and hasattr(df.rdd, "getNumPartitions")
                    and "=" in path
                ):
                    import re

                    partition_matches = re.findall(r"(\w+)=", path)
                    partition_columns = list(set(partition_matches))

                df.unpersist()

                return {
                    "schema": schema,
                    "row_count": row_count,
                    "size_bytes": size_bytes,
                    "partition_columns": partition_columns
                    if partition_columns
                    else None,
                }
            except Exception as e:
                self.logger.warning(f"Failed to get statistics: {e}")
                return {"schema": schema}

        except Exception as e:
            self.logger.error(f"Schema discovery failed: {e}")
            return {}

    def _spark_schema_to_dict(self, schema: StructType) -> Dict[str, Any]:
        """Convert Spark schema to dictionary format."""
        return {
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable,
                    "metadata": field.metadata,
                }
                for field in schema.fields
            ]
        }

    def _save_schema(self, asset_name: str, schema: Dict[str, Any]):
        """Save schema to separate file."""
        schema_file = self.schemas_dir / f"{asset_name}.json"
        try:
            with open(schema_file, "w") as f:
                json.dump(schema, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving schema for {asset_name}: {e}")

    def get_asset(self, name: str) -> Optional[DataAsset]:
        """Get asset by name."""
        return self.assets.get(name)

    def list_assets(
        self,
        classification: Optional[str] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> List[DataAsset]:
        """
        List assets with optional filtering.

        Args:
            classification: Filter by classification
            owner: Filter by owner
            tags: Filter by tags (asset must have all specified tags)

        Returns:
            List of matching assets
        """
        assets = list(self.assets.values())

        if classification:
            assets = [a for a in assets if a.classification == classification]

        if owner:
            assets = [a for a in assets if a.owner == owner]

        if tags:
            assets = [a for a in assets if all(tag in a.tags for tag in tags)]

        return sorted(assets, key=lambda a: a.name)

    def update_asset(
        self,
        name: str,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        classification: Optional[str] = None,
        retention_days: Optional[int] = None,
        refresh_schema: bool = False,
    ) -> Optional[DataAsset]:
        """
        Update an existing asset.

        Args:
            name: Asset name
            description: New description
            tags: New tags
            classification: New classification
            retention_days: New retention period
            refresh_schema: Whether to refresh schema from source

        Returns:
            Updated asset or None if not found
        """
        asset = self.assets.get(name)
        if not asset:
            return None

        # Update fields
        if description is not None:
            asset.description = description
        if tags is not None:
            asset.tags = tags
        if classification is not None:
            asset.classification = classification
        if retention_days is not None:
            asset.retention_days = retention_days

        asset.updated_at = datetime.now()

        # Refresh schema if requested
        if refresh_schema:
            try:
                schema_info = self._discover_schema(asset.path, asset.format)
                asset.schema = schema_info.get("schema", asset.schema)
                asset.size_bytes = schema_info.get("size_bytes", asset.size_bytes)
                asset.row_count = schema_info.get("row_count", asset.row_count)
                asset.partition_columns = schema_info.get(
                    "partition_columns", asset.partition_columns
                )
                self._save_schema(name, asset.schema)
            except Exception as e:
                self.logger.warning(f"Failed to refresh schema for {name}: {e}")

        self._save_assets()
        self.logger.info(f"Updated asset: {name}")
        return asset

    def delete_asset(self, name: str) -> bool:
        """
        Delete an asset from the catalog.

        Args:
            name: Asset name

        Returns:
            True if deleted, False if not found
        """
        if name not in self.assets:
            return False

        del self.assets[name]
        self._save_assets()

        # Remove schema file
        schema_file = self.schemas_dir / f"{name}.json"
        if schema_file.exists():
            schema_file.unlink()

        self.logger.info(f"Deleted asset: {name}")
        return True

    def search_assets(self, query: str) -> List[DataAsset]:
        """
        Search assets by name, description, or tags.

        Args:
            query: Search query

        Returns:
            List of matching assets
        """
        query_lower = query.lower()
        matching_assets = []

        for asset in self.assets.values():
            if (
                query_lower in asset.name.lower()
                or query_lower in asset.description.lower()
                or any(query_lower in tag.lower() for tag in asset.tags)
            ):
                matching_assets.append(asset)

        return sorted(matching_assets, key=lambda a: a.name)

    def get_catalog_stats(self) -> Dict[str, Any]:
        """Get catalog statistics."""
        assets = list(self.assets.values())

        if not assets:
            return {
                "total_assets": 0,
                "by_classification": {},
                "by_format": {},
                "total_size_bytes": 0,
                "total_rows": 0,
            }

        # Classification distribution
        by_classification = {}
        for asset in assets:
            by_classification[asset.classification] = (
                by_classification.get(asset.classification, 0) + 1
            )

        # Format distribution
        by_format = {}
        for asset in assets:
            by_format[asset.format] = by_format.get(asset.format, 0) + 1

        # Size and row totals
        total_size = sum(asset.size_bytes or 0 for asset in assets)
        total_rows = sum(asset.row_count or 0 for asset in assets)

        return {
            "total_assets": len(assets),
            "by_classification": by_classification,
            "by_format": by_format,
            "total_size_bytes": total_size,
            "total_rows": total_rows,
        }

    def export_catalog(self, output_path: str, format: str = "json"):
        """
        Export catalog to file.

        Args:
            output_path: Output file path
            format: Export format (json, csv)
        """
        if format.lower() == "json":
            data = {name: asset.to_dict() for name, asset in self.assets.items()}
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)

        elif format.lower() == "csv":
            # Convert to flat structure for CSV
            rows = []
            for asset in self.assets.values():
                row = {
                    "name": asset.name,
                    "path": asset.path,
                    "format": asset.format,
                    "description": asset.description,
                    "owner": asset.owner,
                    "classification": asset.classification,
                    "created_at": asset.created_at.isoformat(),
                    "updated_at": asset.updated_at.isoformat(),
                    "tags": ",".join(asset.tags),
                    "retention_days": asset.retention_days,
                    "size_bytes": asset.size_bytes,
                    "row_count": asset.row_count,
                    "partition_columns": ",".join(asset.partition_columns or []),
                }
                rows.append(row)

            df = pd.DataFrame(rows)
            df.to_csv(output_path, index=False)

        else:
            raise ValueError(f"Unsupported format: {format}")

        self.logger.info(f"Exported catalog to {output_path} ({format})")

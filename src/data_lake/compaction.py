"""
Data Lake Compaction and Optimization Module

Provides automated compaction, optimization, and maintenance operations
for data lake storage to ensure optimal performance and cost efficiency.
"""

import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import StructType

from ..utils.logger import setup_logging
from ..utils.spark_utils import create_spark_session
from .storage import DataLakeStorage

logger = setup_logging(__name__)


class DataCompactor:
    """
    Data Lake Compaction and Optimization.

    Provides automated compaction, optimization, and maintenance operations
    to ensure optimal performance and storage efficiency.
    """

    def __init__(
        self,
        storage: DataLakeStorage,
        spark_session: Optional[SparkSession] = None,
        temp_path: str = "s3a://data-lake/temp",
    ):
        """
        Initialize DataCompactor.

        Args:
            storage: DataLakeStorage instance
            spark_session: Existing Spark session, creates new if None
            temp_path: Temporary path for compaction operations
        """
        self.storage = storage
        self.spark = spark_session or create_spark_session("DataCompactor")
        self.temp_path = temp_path.rstrip("/")

        logger.info("Initialized DataCompactor")

    def analyze_table_files(self, table_name: str) -> Dict[str, Any]:
        """
        Analyze file structure and sizes for a table.

        Args:
            table_name: Name of the table to analyze

        Returns:
            Dictionary with file analysis results
        """
        try:
            table_path = f"{self.storage.base_path}/{table_name}"

            # Get file information using Spark
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_conf
            )
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(table_path)

            if not fs.exists(path):
                return {"table_name": table_name, "error": "Table not found"}

            # Recursively analyze files
            file_stats = []
            total_size = 0
            file_count = 0

            def analyze_directory(dir_path):
                nonlocal total_size, file_count

                statuses = fs.listStatus(dir_path)
                for status in statuses:
                    if status.isDirectory():
                        analyze_directory(status.getPath())
                    elif status.getPath().getName().endswith(".parquet"):
                        size_mb = status.getLen() / (1024 * 1024)
                        file_stats.append(
                            {
                                "path": str(status.getPath()),
                                "size_mb": round(size_mb, 2),
                                "modified": datetime.fromtimestamp(
                                    status.getModificationTime() / 1000
                                ).isoformat(),
                            }
                        )
                        total_size += size_mb
                        file_count += 1

            analyze_directory(path)

            # Calculate statistics
            if file_stats:
                sizes = [f["size_mb"] for f in file_stats]
                avg_size = sum(sizes) / len(sizes)
                small_files = [f for f in file_stats if f["size_mb"] < 32]  # < 32MB
                large_files = [f for f in file_stats if f["size_mb"] > 256]  # > 256MB

                # Compaction recommendation
                needs_compaction = len(small_files) > 10 or len(large_files) > 5

                analysis = {
                    "table_name": table_name,
                    "table_path": table_path,
                    "file_count": file_count,
                    "total_size_mb": round(total_size, 2),
                    "average_file_size_mb": round(avg_size, 2),
                    "smallest_file_mb": min(sizes),
                    "largest_file_mb": max(sizes),
                    "small_files_count": len(small_files),
                    "large_files_count": len(large_files),
                    "needs_compaction": needs_compaction,
                    "compaction_reason": self._get_compaction_reason(
                        small_files, large_files
                    ),
                    "analyzed_at": datetime.now().isoformat(),
                }

                logger.info(
                    f"Analyzed table {table_name}: {file_count} files, {total_size:.1f}MB"
                )
                return analysis
            else:
                return {
                    "table_name": table_name,
                    "table_path": table_path,
                    "file_count": 0,
                    "total_size_mb": 0,
                    "analyzed_at": datetime.now().isoformat(),
                }

        except Exception as e:
            logger.error(f"Failed to analyze table {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "error": str(e),
                "analyzed_at": datetime.now().isoformat(),
            }

    def _get_compaction_reason(
        self, small_files: List[Dict], large_files: List[Dict]
    ) -> str:
        """Get human-readable compaction recommendation reason."""
        reasons = []

        if len(small_files) > 10:
            reasons.append(f"{len(small_files)} files smaller than 32MB")
        if len(large_files) > 5:
            reasons.append(f"{len(large_files)} files larger than 256MB")

        return "; ".join(reasons) if reasons else "No compaction needed"

    def compact_table(
        self,
        table_name: str,
        target_file_size_mb: int = 128,
        max_file_size_mb: int = 256,
        preserve_partitioning: bool = True,
        backup: bool = True,
    ) -> Dict[str, Any]:
        """
        Compact a table to optimize file sizes.

        Args:
            table_name: Name of the table to compact
            target_file_size_mb: Target file size for compaction
            max_file_size_mb: Maximum allowed file size
            preserve_partitioning: Whether to preserve existing partitioning
            backup: Whether to create backup before compaction

        Returns:
            Dictionary with compaction results
        """
        try:
            start_time = datetime.now()
            table_path = f"{self.storage.base_path}/{table_name}"

            # Analyze current state
            analysis = self.analyze_table_files(table_name)
            if "error" in analysis:
                return analysis

            original_file_count = analysis["file_count"]
            original_size_mb = analysis["total_size_mb"]

            logger.info(
                f"Starting compaction for {table_name}: {original_file_count} files, {original_size_mb:.1f}MB"
            )

            # Create backup if requested
            backup_path = None
            if backup:
                backup_path = f"{table_path}_backup_{int(start_time.timestamp())}"
                self._create_backup(table_path, backup_path)

            # Read the data
            df = self.spark.read.parquet(table_path)
            original_record_count = df.count()

            # Calculate optimal partition count
            estimated_size_mb = original_size_mb
            target_partitions = max(1, int(estimated_size_mb / target_file_size_mb))

            logger.info(
                f"Compacting to {target_partitions} partitions (target: {target_file_size_mb}MB each)"
            )

            # Perform compaction
            if preserve_partitioning:
                # Try to detect existing partitioning
                partitioned_df = df.coalesce(target_partitions)
            else:
                partitioned_df = df.repartition(target_partitions)

            # Write compacted data to temporary location first
            temp_compacted_path = (
                f"{self.temp_path}/compacted_{table_name}_{int(start_time.timestamp())}"
            )

            partitioned_df.write.mode("overwrite").parquet(temp_compacted_path)

            # Verify compacted data
            compacted_df = self.spark.read.parquet(temp_compacted_path)
            compacted_record_count = compacted_df.count()

            if compacted_record_count != original_record_count:
                raise Exception(
                    f"Record count mismatch: {original_record_count} -> {compacted_record_count}"
                )

            # Replace original with compacted version
            self._replace_table_data(table_path, temp_compacted_path)

            # Clean up temporary files
            self._cleanup_path(temp_compacted_path)

            # Analyze new state
            new_analysis = self.analyze_table_files(table_name)
            new_file_count = new_analysis.get("file_count", 0)
            new_size_mb = new_analysis.get("total_size_mb", 0)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Calculate compression ratio
            compression_ratio = (
                (original_size_mb / new_size_mb) if new_size_mb > 0 else 1.0
            )

            result = {
                "table_name": table_name,
                "status": "success",
                "original_files": original_file_count,
                "compacted_files": new_file_count,
                "file_reduction": original_file_count - new_file_count,
                "original_size_mb": round(original_size_mb, 2),
                "compacted_size_mb": round(new_size_mb, 2),
                "size_reduction_mb": round(original_size_mb - new_size_mb, 2),
                "compression_ratio": round(compression_ratio, 3),
                "record_count": original_record_count,
                "target_file_size_mb": target_file_size_mb,
                "actual_partitions": target_partitions,
                "backup_path": backup_path,
                "duration_seconds": round(duration, 2),
                "compacted_at": end_time.isoformat(),
            }

            logger.info(
                f"Compaction completed for {table_name}: "
                f"{original_file_count} -> {new_file_count} files, "
                f"{original_size_mb:.1f} -> {new_size_mb:.1f}MB in {duration:.1f}s"
            )

            return result

        except Exception as e:
            logger.error(f"Compaction failed for table {table_name}: {str(e)}")

            # Try to restore from backup if it was created
            if backup and backup_path and self._path_exists(backup_path):
                try:
                    self._replace_table_data(table_path, backup_path)
                    logger.info(f"Restored {table_name} from backup")
                except Exception as restore_error:
                    logger.error(f"Failed to restore from backup: {str(restore_error)}")

            return {
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "backup_path": backup_path,
                "failed_at": datetime.now().isoformat(),
            }

    def optimize_all_tables(
        self,
        min_file_count: int = 10,
        target_file_size_mb: int = 128,
        parallel: bool = True,
        max_workers: int = 4,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Optimize all tables that need compaction.

        Args:
            min_file_count: Minimum file count to trigger optimization
            target_file_size_mb: Target file size for optimization
            parallel: Whether to process tables in parallel
            max_workers: Maximum number of parallel workers

        Returns:
            Dictionary with optimization results for all tables
        """
        try:
            # Get all tables
            tables = self.storage.list_tables()

            if not tables:
                logger.info("No tables found in data lake")
                return {"optimized": [], "skipped": [], "failed": []}

            logger.info(f"Found {len(tables)} tables, analyzing for optimization needs")

            # Analyze all tables first
            optimization_candidates = []
            for table_info in tables:
                table_name = table_info["table_name"]
                analysis = self.analyze_table_files(table_name)

                if "error" not in analysis and analysis.get("needs_compaction", False):
                    if analysis.get("file_count", 0) >= min_file_count:
                        optimization_candidates.append(table_name)

            logger.info(
                f"Found {len(optimization_candidates)} tables needing optimization"
            )

            optimized = []
            skipped = []
            failed = []

            def optimize_table(table_name: str) -> Dict[str, Any]:
                try:
                    result = self.compact_table(
                        table_name=table_name,
                        target_file_size_mb=target_file_size_mb,
                        preserve_partitioning=True,
                        backup=True,
                    )
                    return result
                except Exception as e:
                    logger.error(f"Failed to optimize table {table_name}: {str(e)}")
                    return {
                        "table_name": table_name,
                        "status": "failed",
                        "error": str(e),
                    }

            # Process optimization candidates
            if parallel and len(optimization_candidates) > 1:
                # Parallel optimization
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_table = {
                        executor.submit(optimize_table, table_name): table_name
                        for table_name in optimization_candidates
                    }

                    for future in as_completed(future_to_table):
                        result = future.result()
                        table_name = result["table_name"]

                        if result["status"] == "success":
                            optimized.append(result)
                        else:
                            failed.append(result)
            else:
                # Sequential optimization
                for table_name in optimization_candidates:
                    result = optimize_table(table_name)

                    if result["status"] == "success":
                        optimized.append(result)
                    else:
                        failed.append(result)

            # Record skipped tables
            all_table_names = {table["table_name"] for table in tables}
            processed_tables = {result["table_name"] for result in optimized + failed}
            skipped_table_names = all_table_names - processed_tables

            for table_name in skipped_table_names:
                skipped.append(
                    {"table_name": table_name, "reason": "Does not need optimization"}
                )

            summary = {
                "optimized": optimized,
                "skipped": skipped,
                "failed": failed,
                "summary": {
                    "total_tables": len(tables),
                    "optimized_count": len(optimized),
                    "skipped_count": len(skipped),
                    "failed_count": len(failed),
                    "optimization_completed_at": datetime.now().isoformat(),
                },
            }

            logger.info(
                f"Optimization complete: {len(optimized)} optimized, "
                f"{len(skipped)} skipped, {len(failed)} failed"
            )

            return summary

        except Exception as e:
            logger.error(f"Failed to optimize all tables: {str(e)}")
            return {"optimized": [], "skipped": [], "failed": [], "error": str(e)}

    def cleanup_old_files(
        self, table_name: str, retention_days: int = 30, file_pattern: str = "_backup_"
    ) -> Dict[str, Any]:
        """
        Clean up old backup and temporary files.

        Args:
            table_name: Name of the table to clean up
            retention_days: Number of days to retain files
            file_pattern: Pattern to identify files for cleanup

        Returns:
            Dictionary with cleanup results
        """
        try:
            start_time = datetime.now()
            cutoff_time = start_time - timedelta(days=retention_days)

            base_path = f"{self.storage.base_path}"

            # Get file system
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_conf
            )
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path)

            cleaned_files = []
            total_size_cleaned = 0

            if fs.exists(path):
                statuses = fs.listStatus(path)

                for status in statuses:
                    file_path = status.getPath()
                    file_name = file_path.getName()

                    # Check if file matches pattern and is old enough
                    if (
                        file_pattern in file_name
                        and table_name in file_name
                        and datetime.fromtimestamp(status.getModificationTime() / 1000)
                        < cutoff_time
                    ):
                        try:
                            file_size_mb = status.getLen() / (1024 * 1024)
                            fs.delete(file_path, True)  # Recursive delete

                            cleaned_files.append(
                                {
                                    "path": str(file_path),
                                    "size_mb": round(file_size_mb, 2),
                                    "modified": datetime.fromtimestamp(
                                        status.getModificationTime() / 1000
                                    ).isoformat(),
                                }
                            )

                            total_size_cleaned += file_size_mb

                        except Exception as e:
                            logger.error(f"Failed to delete {file_path}: {str(e)}")

            result = {
                "table_name": table_name,
                "retention_days": retention_days,
                "file_pattern": file_pattern,
                "files_cleaned": len(cleaned_files),
                "total_size_cleaned_mb": round(total_size_cleaned, 2),
                "cleaned_files": cleaned_files,
                "status": "completed",
                "cleaned_at": datetime.now().isoformat(),
            }

            logger.info(
                f"Cleanup completed for {table_name}: {len(cleaned_files)} files, "
                f"{total_size_cleaned:.1f}MB cleaned"
            )

            return result

        except Exception as e:
            logger.error(f"Cleanup failed for table {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "status": "failed",
                "error": str(e),
                "cleaned_at": datetime.now().isoformat(),
            }

    def _create_backup(self, source_path: str, backup_path: str) -> None:
        """Create backup of table data."""
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            hadoop_conf
        )

        source = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(source_path)
        backup = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(backup_path)

        # Copy the entire directory
        hadoop_util = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileUtil
        hadoop_util.copy(fs, source, fs, backup, False, hadoop_conf)

        logger.info(f"Created backup: {source_path} -> {backup_path}")

    def _replace_table_data(self, table_path: str, source_path: str) -> None:
        """Replace table data with data from source path."""
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            hadoop_conf
        )

        table = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(table_path)
        source = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(source_path)

        # Delete original
        if fs.exists(table):
            fs.delete(table, True)

        # Move source to table location
        fs.rename(source, table)

        logger.debug(f"Replaced table data: {source_path} -> {table_path}")

    def _cleanup_path(self, path: str) -> None:
        """Clean up temporary path."""
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_conf
            )
            path_obj = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)

            if fs.exists(path_obj):
                fs.delete(path_obj, True)
                logger.debug(f"Cleaned up temporary path: {path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup path {path}: {str(e)}")

    def _path_exists(self, path: str) -> bool:
        """Check if path exists."""
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_conf
            )
            path_obj = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
            return fs.exists(path_obj)
        except Exception:
            return False

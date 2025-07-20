"""
Streaming data profiling capabilities.

This module provides comprehensive data profiling for streaming data including
statistical analysis, data distribution insights, and pattern detection.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    asc,
    coalesce,
    col,
    count,
    countDistinct,
    current_timestamp,
    desc,
    isnan,
    isnull,
    kurtosis,
    length,
    lit,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import mean
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import (
    percentile_approx,
    regexp_extract,
    size,
    skewness,
    split,
    stddev,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NumericType,
    StringType,
    StructType,
    TimestampType,
)

from src.utils.logger import get_logger


@dataclass
class ColumnProfile:
    """Data profile for a single column."""

    name: str
    data_type: str
    count: int
    null_count: int
    distinct_count: int
    completeness_rate: float

    # Numeric statistics (if applicable)
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None
    skewness_value: Optional[float] = None
    kurtosis_value: Optional[float] = None
    quartile_25: Optional[float] = None
    quartile_75: Optional[float] = None

    # String statistics (if applicable)
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None

    # Top values and patterns
    top_values: Optional[List[Dict[str, Any]]] = None
    common_patterns: Optional[List[str]] = None

    # Quality indicators
    zero_count: Optional[int] = None
    negative_count: Optional[int] = None
    outlier_count: Optional[int] = None


class StreamingDataProfiler:
    """
    Comprehensive data profiling for streaming data.

    Provides statistical analysis, distribution insights, and pattern detection
    for real-time streaming data to support data quality assessment.
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize streaming data profiler.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary for profiling settings
        """
        self.spark = spark
        self.logger = get_logger(__name__)
        self.config = config or {}

        # Profiling configuration
        self.max_distinct_values = self.config.get("max_distinct_values", 100)
        self.sample_size = self.config.get("sample_size", 10000)
        self.outlier_threshold = self.config.get(
            "outlier_threshold", 3.0
        )  # Z-score threshold
        self.enable_pattern_detection = self.config.get(
            "enable_pattern_detection", True
        )

    def profile_stream(
        self,
        df: DataFrame,
        stream_type: str = "unknown",
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Profile streaming data to understand its characteristics.

        Args:
            df: Input DataFrame to profile
            stream_type: Type of stream for context
            columns: Specific columns to profile (None for all)

        Returns:
            Dictionary with comprehensive data profile
        """
        try:
            self.logger.info(f"Starting data profiling for {stream_type} stream")

            # Sample data for profiling if dataset is large
            profiling_df = self._sample_data_for_profiling(df)

            # Select columns to profile
            columns_to_profile = columns or profiling_df.columns

            # Generate column profiles
            column_profiles = {}
            for column in columns_to_profile:
                if column in profiling_df.columns:
                    profile = self._profile_column(profiling_df, column)
                    column_profiles[column] = profile

            # Generate overall dataset profile
            dataset_profile = self._generate_dataset_profile(profiling_df, stream_type)

            # Detect correlations (for numeric columns)
            correlations = self._detect_correlations(profiling_df)

            # Generate data quality insights
            quality_insights = self._generate_quality_insights(
                column_profiles, dataset_profile
            )

            profile_result = {
                "profiling_timestamp": datetime.now(),
                "stream_type": stream_type,
                "dataset_profile": dataset_profile,
                "column_profiles": {
                    name: self._column_profile_to_dict(profile)
                    for name, profile in column_profiles.items()
                },
                "correlations": correlations,
                "quality_insights": quality_insights,
                "recommendations": self._generate_recommendations(
                    column_profiles, dataset_profile
                ),
            }

            self.logger.info(
                f"Data profiling completed for {len(column_profiles)} columns"
            )
            return profile_result

        except Exception as e:
            self.logger.error(f"Error in data profiling: {e}")
            raise

    def _sample_data_for_profiling(self, df: DataFrame) -> DataFrame:
        """Sample data for profiling to ensure reasonable performance."""

        try:
            row_count = df.count()

            if row_count <= self.sample_size:
                return df
            else:
                sample_fraction = self.sample_size / row_count
                self.logger.info(
                    f"Sampling {self.sample_size} rows from {row_count} total rows"
                )
                return df.sample(
                    withReplacement=False, fraction=sample_fraction, seed=42
                )

        except Exception as e:
            self.logger.warning(f"Error sampling data, using full dataset: {e}")
            return df

    def _profile_column(self, df: DataFrame, column_name: str) -> ColumnProfile:
        """Generate comprehensive profile for a single column."""

        try:
            # Basic statistics
            basic_stats = df.agg(
                count(col(column_name)).alias("count"),
                count("*").alias("total_count"),
                countDistinct(col(column_name)).alias("distinct_count"),
            ).collect()[0]

            total_count = basic_stats["total_count"]
            value_count = basic_stats["count"]
            null_count = total_count - value_count
            distinct_count = basic_stats["distinct_count"]
            completeness_rate = value_count / max(total_count, 1)

            # Get data type
            column_type = dict(df.dtypes)[column_name]

            # Initialize profile
            profile = ColumnProfile(
                name=column_name,
                data_type=column_type,
                count=value_count,
                null_count=null_count,
                distinct_count=distinct_count,
                completeness_rate=completeness_rate,
            )

            # Add type-specific statistics
            if self._is_numeric_type(column_type):
                self._add_numeric_statistics(df, column_name, profile)

            if self._is_string_type(column_type):
                self._add_string_statistics(df, column_name, profile)

            # Add top values (for columns with reasonable cardinality)
            if distinct_count <= self.max_distinct_values:
                profile.top_values = self._get_top_values(df, column_name)

            # Add pattern detection for strings
            if self._is_string_type(column_type) and self.enable_pattern_detection:
                profile.common_patterns = self._detect_patterns(df, column_name)

            return profile

        except Exception as e:
            self.logger.error(f"Error profiling column {column_name}: {e}")
            return ColumnProfile(
                name=column_name,
                data_type="unknown",
                count=0,
                null_count=0,
                distinct_count=0,
                completeness_rate=0.0,
            )

    def _is_numeric_type(self, data_type: str) -> bool:
        """Check if data type is numeric."""
        numeric_types = [
            "int",
            "integer",
            "long",
            "float",
            "double",
            "decimal",
            "bigint",
            "smallint",
        ]
        return any(nt in data_type.lower() for nt in numeric_types)

    def _is_string_type(self, data_type: str) -> bool:
        """Check if data type is string."""
        return "string" in data_type.lower() or "varchar" in data_type.lower()

    def _add_numeric_statistics(
        self, df: DataFrame, column_name: str, profile: ColumnProfile
    ) -> None:
        """Add numeric statistics to column profile."""

        try:
            # Basic numeric statistics
            numeric_stats = df.select(
                spark_min(col(column_name)).alias("min_val"),
                spark_max(col(column_name)).alias("max_val"),
                mean(col(column_name)).alias("mean_val"),
                stddev(col(column_name)).alias("stddev_val"),
                skewness(col(column_name)).alias("skewness_val"),
                kurtosis(col(column_name)).alias("kurtosis_val"),
            ).collect()[0]

            profile.min_value = numeric_stats["min_val"]
            profile.max_value = numeric_stats["max_val"]
            profile.mean_value = numeric_stats["mean_val"]
            profile.std_dev = numeric_stats["stddev_val"]
            profile.skewness_value = numeric_stats["skewness_val"]
            profile.kurtosis_value = numeric_stats["kurtosis_val"]

            # Percentiles
            percentiles = df.select(
                percentile_approx(col(column_name), 0.25).alias("q25"),
                percentile_approx(col(column_name), 0.5).alias("median"),
                percentile_approx(col(column_name), 0.75).alias("q75"),
            ).collect()[0]

            profile.quartile_25 = percentiles["q25"]
            profile.median_value = percentiles["median"]
            profile.quartile_75 = percentiles["q75"]

            # Special value counts
            profile.zero_count = df.filter(col(column_name) == 0).count()
            profile.negative_count = df.filter(col(column_name) < 0).count()

            # Outlier detection using IQR method
            if profile.quartile_25 is not None and profile.quartile_75 is not None:
                iqr = profile.quartile_75 - profile.quartile_25
                lower_bound = profile.quartile_25 - (1.5 * iqr)
                upper_bound = profile.quartile_75 + (1.5 * iqr)

                profile.outlier_count = df.filter(
                    (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
                ).count()

        except Exception as e:
            self.logger.warning(
                f"Error adding numeric statistics for {column_name}: {e}"
            )

    def _add_string_statistics(
        self, df: DataFrame, column_name: str, profile: ColumnProfile
    ) -> None:
        """Add string statistics to column profile."""

        try:
            # String length statistics
            length_stats = df.select(
                spark_min(length(col(column_name))).alias("min_length"),
                spark_max(length(col(column_name))).alias("max_length"),
                mean(length(col(column_name))).alias("avg_length"),
            ).collect()[0]

            profile.min_length = length_stats["min_length"]
            profile.max_length = length_stats["max_length"]
            profile.avg_length = length_stats["avg_length"]

        except Exception as e:
            self.logger.warning(
                f"Error adding string statistics for {column_name}: {e}"
            )

    def _get_top_values(
        self, df: DataFrame, column_name: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get top values and their frequencies."""

        try:
            top_values = (
                df.groupBy(col(column_name))
                .count()
                .orderBy(desc("count"))
                .limit(limit)
                .collect()
            )

            total_count = df.count()
            return [
                {
                    "value": row[column_name],
                    "count": row["count"],
                    "percentage": round(row["count"] / max(total_count, 1) * 100, 2),
                }
                for row in top_values
            ]

        except Exception as e:
            self.logger.warning(f"Error getting top values for {column_name}: {e}")
            return []

    def _detect_patterns(self, df: DataFrame, column_name: str) -> List[str]:
        """Detect common patterns in string data."""

        try:
            patterns = []

            # Sample some values for pattern detection
            sample_values = (
                df.select(col(column_name))
                .filter(col(column_name).isNotNull())
                .limit(1000)
                .collect()
            )

            if not sample_values:
                return patterns

            # Check for common patterns
            email_count = sum(
                1
                for row in sample_values
                if row[column_name] and "@" in str(row[column_name])
            )
            if email_count > len(sample_values) * 0.5:
                patterns.append("email_format")

            url_count = sum(
                1
                for row in sample_values
                if row[column_name]
                and str(row[column_name]).startswith(("http://", "https://"))
            )
            if url_count > len(sample_values) * 0.5:
                patterns.append("url_format")

            # Check for numeric patterns in strings
            numeric_string_count = sum(
                1
                for row in sample_values
                if row[column_name] and str(row[column_name]).isdigit()
            )
            if numeric_string_count > len(sample_values) * 0.5:
                patterns.append("numeric_string")

            # Check for UUID-like patterns
            uuid_count = sum(
                1
                for row in sample_values
                if row[column_name]
                and len(str(row[column_name])) >= 32
                and "-" in str(row[column_name])
            )
            if uuid_count > len(sample_values) * 0.5:
                patterns.append("uuid_like")

            return patterns

        except Exception as e:
            self.logger.warning(f"Error detecting patterns for {column_name}: {e}")
            return []

    def _generate_dataset_profile(
        self, df: DataFrame, stream_type: str
    ) -> Dict[str, Any]:
        """Generate overall dataset profile."""

        try:
            row_count = df.count()
            column_count = len(df.columns)

            # Data types distribution
            type_distribution = {}
            for col_name, col_type in df.dtypes:
                type_distribution[col_type] = type_distribution.get(col_type, 0) + 1

            # Memory footprint estimation (rough)
            estimated_size_mb = (
                row_count * column_count * 8 / (1024 * 1024)
            )  # Rough estimate

            return {
                "stream_type": stream_type,
                "row_count": row_count,
                "column_count": column_count,
                "data_types": type_distribution,
                "estimated_size_mb": round(estimated_size_mb, 2),
                "profiling_sample_size": min(row_count, self.sample_size),
                "schema": df.schema.json(),
            }

        except Exception as e:
            self.logger.error(f"Error generating dataset profile: {e}")
            return {"error": str(e)}

    def _detect_correlations(self, df: DataFrame) -> Dict[str, Dict[str, float]]:
        """Detect correlations between numeric columns."""

        try:
            numeric_columns = [
                col_name
                for col_name, col_type in df.dtypes
                if self._is_numeric_type(col_type)
            ]

            if len(numeric_columns) < 2:
                return {}

            correlations = {}

            # Calculate pairwise correlations for numeric columns
            for i, col1 in enumerate(numeric_columns):
                correlations[col1] = {}
                for j, col2 in enumerate(numeric_columns):
                    if i != j:
                        try:
                            corr_value = df.stat.corr(col1, col2)
                            correlations[col1][col2] = (
                                round(corr_value, 4) if corr_value is not None else 0.0
                            )
                        except Exception:
                            correlations[col1][col2] = 0.0
                    else:
                        correlations[col1][col2] = 1.0

            return correlations

        except Exception as e:
            self.logger.warning(f"Error detecting correlations: {e}")
            return {}

    def _generate_quality_insights(
        self, column_profiles: Dict[str, ColumnProfile], dataset_profile: Dict[str, Any]
    ) -> List[str]:
        """Generate data quality insights based on profiling results."""

        insights = []

        try:
            # Overall completeness insights
            total_columns = len(column_profiles)
            high_completeness_columns = sum(
                1 for p in column_profiles.values() if p.completeness_rate >= 0.95
            )

            if high_completeness_columns / max(total_columns, 1) >= 0.8:
                insights.append(
                    "Dataset has good overall completeness (80%+ columns >95% complete)"
                )
            else:
                insights.append("Dataset has completeness issues in multiple columns")

            # Cardinality insights
            high_cardinality_columns = [
                name
                for name, profile in column_profiles.items()
                if profile.distinct_count > profile.count * 0.8
            ]
            if high_cardinality_columns:
                insights.append(
                    f"High cardinality detected in: {', '.join(high_cardinality_columns[:3])}"
                )

            # Numeric insights
            numeric_columns = [
                name
                for name, profile in column_profiles.items()
                if self._is_numeric_type(profile.data_type)
            ]
            if numeric_columns:
                outlier_columns = [
                    name
                    for name in numeric_columns
                    if column_profiles[name].outlier_count
                    and column_profiles[name].outlier_count
                    > column_profiles[name].count * 0.05
                ]
                if outlier_columns:
                    insights.append(
                        f"Significant outliers detected in: {', '.join(outlier_columns[:3])}"
                    )

            # Pattern insights
            pattern_columns = [
                name
                for name, profile in column_profiles.items()
                if profile.common_patterns
            ]
            if pattern_columns:
                insights.append(
                    f"Structured patterns detected in: {', '.join(pattern_columns[:3])}"
                )

            # Data distribution insights
            skewed_columns = [
                name
                for name, profile in column_profiles.items()
                if profile.skewness_value and abs(profile.skewness_value) > 2
            ]
            if skewed_columns:
                insights.append(
                    f"Highly skewed distributions in: {', '.join(skewed_columns[:3])}"
                )

        except Exception as e:
            self.logger.warning(f"Error generating quality insights: {e}")
            insights.append("Unable to generate complete quality insights")

        return insights

    def _generate_recommendations(
        self, column_profiles: Dict[str, ColumnProfile], dataset_profile: Dict[str, Any]
    ) -> List[str]:
        """Generate actionable recommendations based on profiling results."""

        recommendations = []

        try:
            # Completeness recommendations
            low_completeness = [
                name
                for name, profile in column_profiles.items()
                if profile.completeness_rate < 0.9
            ]
            if low_completeness:
                recommendations.append(
                    f"Investigate data quality issues in columns with low completeness: {', '.join(low_completeness[:3])}"
                )

            # Cardinality recommendations
            unique_id_candidates = [
                name
                for name, profile in column_profiles.items()
                if profile.distinct_count == profile.count and profile.count > 100
            ]
            if unique_id_candidates:
                recommendations.append(
                    f"Consider using as primary keys: {', '.join(unique_id_candidates[:2])}"
                )

            # Data type recommendations
            numeric_strings = [
                name
                for name, profile in column_profiles.items()
                if profile.common_patterns
                and "numeric_string" in profile.common_patterns
            ]
            if numeric_strings:
                recommendations.append(
                    f"Consider converting to numeric types: {', '.join(numeric_strings[:3])}"
                )

            # Outlier recommendations
            outlier_columns = [
                name
                for name, profile in column_profiles.items()
                if profile.outlier_count and profile.outlier_count > profile.count * 0.1
            ]
            if outlier_columns:
                recommendations.append(
                    f"Review outlier handling strategies for: {', '.join(outlier_columns[:3])}"
                )

            # Performance recommendations
            if dataset_profile.get("row_count", 0) > 1000000:
                recommendations.append(
                    "Consider data partitioning strategies for large dataset"
                )

            if not recommendations:
                recommendations.append(
                    "Data profile looks healthy - continue monitoring"
                )

        except Exception as e:
            self.logger.warning(f"Error generating recommendations: {e}")
            recommendations.append("Unable to generate complete recommendations")

        return recommendations

    def _column_profile_to_dict(self, profile: ColumnProfile) -> Dict[str, Any]:
        """Convert ColumnProfile to dictionary for serialization."""

        return {
            "name": profile.name,
            "data_type": profile.data_type,
            "count": profile.count,
            "null_count": profile.null_count,
            "distinct_count": profile.distinct_count,
            "completeness_rate": round(profile.completeness_rate, 4),
            "min_value": profile.min_value,
            "max_value": profile.max_value,
            "mean_value": round(profile.mean_value, 4) if profile.mean_value else None,
            "median_value": round(profile.median_value, 4)
            if profile.median_value
            else None,
            "std_dev": round(profile.std_dev, 4) if profile.std_dev else None,
            "skewness_value": round(profile.skewness_value, 4)
            if profile.skewness_value
            else None,
            "kurtosis_value": round(profile.kurtosis_value, 4)
            if profile.kurtosis_value
            else None,
            "quartile_25": round(profile.quartile_25, 4)
            if profile.quartile_25
            else None,
            "quartile_75": round(profile.quartile_75, 4)
            if profile.quartile_75
            else None,
            "min_length": profile.min_length,
            "max_length": profile.max_length,
            "avg_length": round(profile.avg_length, 2) if profile.avg_length else None,
            "top_values": profile.top_values,
            "common_patterns": profile.common_patterns,
            "zero_count": profile.zero_count,
            "negative_count": profile.negative_count,
            "outlier_count": profile.outlier_count,
        }

    def create_profiling_summary_stream(
        self, df: DataFrame, profile_results: Dict[str, Any]
    ) -> DataFrame:
        """
        Create a summary stream for profiling monitoring.

        Args:
            df: Original DataFrame
            profile_results: Results from profiling

        Returns:
            DataFrame with profiling summary for monitoring
        """
        try:
            # Create a simple summary record
            summary_data = [
                (
                    profile_results.get("profiling_timestamp", datetime.now()),
                    profile_results.get("stream_type", "unknown"),
                    profile_results.get("dataset_profile", {}).get("row_count", 0),
                    profile_results.get("dataset_profile", {}).get("column_count", 0),
                    len(profile_results.get("quality_insights", [])),
                    len(profile_results.get("recommendations", [])),
                )
            ]

            return self.spark.createDataFrame(
                summary_data,
                [
                    "profiling_timestamp",
                    "stream_type",
                    "row_count",
                    "column_count",
                    "insights_count",
                    "recommendations_count",
                ],
            )

        except Exception as e:
            self.logger.error(f"Error creating profiling summary stream: {e}")
            return self.spark.createDataFrame(
                [], "profiling_timestamp timestamp, error string"
            )

from typing import TYPE_CHECKING

from pyspark.sql.functions import col, count, lit, when

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


def validate_schema(df: "DataFrame", expected_schema: "StructType") -> "DataFrame":
    """
    Validates the schema of a DataFrame and adds a data quality column.

    Args:
        df (DataFrame): The input DataFrame.
        expected_schema (StructType): The expected schema.

    Returns:
        DataFrame: The DataFrame with an added 'quality_checks' column.
    """
    # Basic schema validation (column names)
    expected_columns = {field.name for field in expected_schema.fields}
    actual_columns = set(df.columns)

    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns

    # Add a column to flag schema issues
    if missing_columns or extra_columns:
        error_msg = (
            f"Schema mismatch. Missing: {missing_columns}, Extra: {extra_columns}"
        )
        return df.withColumn("quality_checks", lit(error_msg))

    # More granular type checking can be added here if needed

    return df.withColumn("quality_checks", lit("passed"))


def check_nulls(df: "DataFrame", columns: list[str]) -> "DataFrame":
    """
    Checks for null values in specified columns.

    Args:
        df (DataFrame): The input DataFrame.
        columns (list[str]): A list of column names to check for nulls.

    Returns:
        DataFrame: The DataFrame with null checks added to the 'quality_checks' column.
    """
    if "quality_checks" not in df.columns:
        df = df.withColumn("quality_checks", lit("passed"))

    for column_name in columns:
        df = df.withColumn(
            "quality_checks",
            when(col(column_name).isNull(), f"failed: {column_name} is null").otherwise(
                col("quality_checks")
            ),
        )
    return df


def detect_anomalies(
    df: "DataFrame", column: str, lower_bound: float, upper_bound: float
) -> "DataFrame":
    """
    Detects anomalies in a numerical column based on a given range.

    Args:
        df (DataFrame): The input DataFrame.
        column (str): The numerical column to check.
        lower_bound (float): The lower bound of the valid range.
        upper_bound (float): The upper bound of the valid range.

    Returns:
        DataFrame: The DataFrame with anomaly checks added to the 'quality_checks' column.
    """
    if "quality_checks" not in df.columns:
        df = df.withColumn("quality_checks", lit("passed"))

    return df.withColumn(
        "quality_checks",
        when(
            (col(column) < lower_bound) | (col(column) > upper_bound),
            f"failed: {column} out of range",
        ).otherwise(col("quality_checks")),
    )


def profile_data(df: "DataFrame", group_by_col: str) -> "DataFrame":
    """

    Performs basic data profiling on a streaming DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        group_by_col (str): The column to group by for profiling.

    Returns:
        DataFrame: A DataFrame with profiling metrics.
    """
    return df.groupBy(group_by_col).agg(count("*").alias("record_count"))

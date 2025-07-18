import time
from pyspark.sql import DataFrame
from typing import Callable

class PerformanceMonitor:
    """
    Utility for monitoring the performance of Spark operations.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)

    def measure_time(self, func: Callable, *args, **kwargs):
        """
        Measures the execution time of a given function.
        """
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        self.logger.info(f"Function '{func.__name__}' executed in {duration:.2f} seconds.")
        return result, duration

    def get_dataframe_size(self, df: DataFrame) -> int:
        """
        Returns the number of rows in a DataFrame.
        """
        size = df.count()
        self.logger.info(f"DataFrame has {size} rows.")
        return size

    def get_dataframe_memory_usage(self, df: DataFrame) -> str:
        """
        Estimates the memory usage of a DataFrame.
        Note: This is an approximation and might not be accurate for complex DataFrames.
        ""
        # This is a very rough estimate. Actual memory usage is complex in Spark.
        # A more accurate approach would involve Spark's metrics API or external tools.
        num_partitions = df.rdd.getNumPartitions()
        avg_partition_size_bytes = df.limit(100).toPandas().memory_usage(deep=True).sum() / 100 if df.count() > 0 else 0
        estimated_total_bytes = avg_partition_size_bytes * df.count()

        # Convert to human-readable format
        for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if estimated_total_bytes < 1024.0:
                return f"{estimated_total_bytes:.2f} {unit}"
            estimated_total_bytes /= 1024.0
        return f"{estimated_total_bytes:.2f} PB" # Should not reach here for typical data
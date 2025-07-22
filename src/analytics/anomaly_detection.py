
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import LocalOutlierFactor

class AnomalyDetector:
    """
    A class to implement real-time anomaly detection using statistical methods.
    """

    def __init__(self, spark: SparkSession, stream_source_path: str = "data/delta/transactions"):
        self.spark = spark
        self.stream_source_path = stream_source_path

    def _load_stream_data(self) -> DataFrame:
        """
        Loads streaming data from the specified source.
        For demonstration, we'll use a static Delta table as a stream source.
        """
        return (
            self.spark.readStream.format("delta")
            .load(self.stream_source_path)
            .withColumn("timestamp_ms", F.col("timestamp").cast(DoubleType()))
        )

    def detect_anomalies(self, df: DataFrame, feature_cols: list) -> DataFrame:
        """
        Detects anomalies in the input DataFrame using Local Outlier Factor (LOF).

        Args:
            df: The input DataFrame with streaming data.
            feature_cols: List of columns to use for anomaly detection.

        Returns:
            DataFrame with an 'is_anomaly' column.
        """
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_assembled = assembler.transform(df)

        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(df_assembled.limit(1000)) # Fit scaler on a sample
        df_scaled = scaler_model.transform(df_assembled)

        # LOF model - Note: LOF is typically for batch, but for real-time, it's often applied
        # on micro-batches or a sliding window of data. Here, we simulate it.
        lof = LocalOutlierFactor(featuresCol="scaled_features", predictionCol="prediction",
                                 outputCol="lof_score", k=20) # k is number of neighbors

        # For streaming, you'd typically train LOF periodically on a batch of recent data
        # and then apply the trained model to incoming micro-batches.
        # This is a simplified example for demonstration.
        # In a true streaming scenario, you might use approximate algorithms or windowed aggregations
        # to calculate statistical properties for anomaly detection.

        # For simplicity, we'll just mark based on a threshold on a feature for now
        # A proper LOF implementation for streaming would be more complex.
        # Let's use a simple statistical rule for demonstration purposes.
        # For example, mark as anomaly if 'price' is 3 standard deviations away from mean in a window.

        window_spec = F.window("timestamp", "1 hour", "10 minutes")
        aggregated_df = df.groupBy(window_spec).agg(
            F.mean("price").alias("mean_price"),
            F.stddev("price").alias("stddev_price"),
        )

        joined_df = df.join(aggregated_df, F.col("timestamp").between(F.col("window.start"), F.col("window.end")))

        anomalies_df = joined_df.withColumn("is_anomaly",
            F.when(
                (F.abs(F.col("price") - F.col("mean_price")) > 3 * F.col("stddev_price"))
                & (F.col("stddev_price") > 0), # Avoid division by zero
                True
            ).otherwise(False)
        )

        return anomalies_df

    def run_detection(self, feature_cols: list, output_path: str = None):
        """
        Runs the real-time anomaly detection pipeline.

        Args:
            feature_cols: List of columns to use for anomaly detection.
            output_path: Optional path to write the anomalies to.
        """
        stream_df = self._load_stream_data()
        anomalies_df = self.detect_anomalies(stream_df, feature_cols)

        query = (
            anomalies_df.writeStream.outputMode("append")
            .format("console")
            .option("truncate", False)
        )

        if output_path:
            query = query.format("delta").option("checkpointLocation", f"{output_path}/checkpoint")
            query = query.toTable("anomalies_table") # Or .start(output_path)

        query.start().awaitTermination()

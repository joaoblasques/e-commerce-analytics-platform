from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from src.analytics.anomaly_detection import AnomalyDetector


def main():
    """Main function to run the anomaly detection example."""
    spark = SparkSession.builder.appName("AnomalyDetectionExample").getOrCreate()

    # Create a dummy DataFrame for demonstration (simulating a stream)
    data = [
        ("txn1", "user1", 100.0, 1, datetime(2025, 1, 1, 10, 0, 0)),
        ("txn2", "user1", 105.0, 1, datetime(2025, 1, 1, 10, 1, 0)),
        ("txn3", "user2", 1000.0, 10, datetime(2025, 1, 1, 10, 2, 0)),  # Anomaly
        ("txn4", "user3", 110.0, 1, datetime(2025, 1, 1, 10, 3, 0)),
        ("txn5", "user4", 95.0, 1, datetime(2025, 1, 1, 10, 4, 0)),
        ("txn6", "user5", 10.0, 1, datetime(2025, 1, 1, 10, 5, 0)),  # Anomaly
    ]
    schema = ["transaction_id", "user_id", "price", "quantity", "timestamp"]
    df = spark.createDataFrame(data, schema)

    # For a real streaming scenario, you would read from a stream source like Kafka or a Delta table
    # For this example, we'll just process the static DataFrame as a batch.
    # To simulate streaming, you would typically write this static data to a Delta table
    # and then read from that Delta table in streaming mode.

    # Initialize the AnomalyDetector
    detector = AnomalyDetector(
        spark, stream_source_path="dummy_path"
    )  # Path is not used for static DF

    print("\n--- Detecting Anomalies ---")
    feature_cols = ["price", "quantity"]
    anomalies_df = detector.detect_anomalies(df, feature_cols)

    print("Detected Anomalies:")
    anomalies_df.filter(F.col("is_anomaly") == True).show()

    print("Non-Anomalous Data:")
    anomalies_df.filter(F.col("is_anomaly") == False).show()


if __name__ == "__main__":
    main()

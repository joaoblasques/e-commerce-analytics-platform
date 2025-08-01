import click
from pyspark.sql import SparkSession

from src.analytics.anomaly_detection import AnomalyDetector


@click.group()
def anomaly():
    """Anomaly detection commands."""
    pass


@anomaly.command()
@click.option(
    "--stream-source-path",
    default="data/delta/transactions",
    help="Path to the streaming data source (e.g., Delta table).",
)
@click.option(
    "--feature-cols",
    required=True,
    help="Comma-separated list of feature columns for anomaly detection.",
)
@click.option(
    "--output-path",
    default=None,
    help="Optional path to write detected anomalies (e.g., Delta table).",
)
def detect(stream_source_path: str, feature_cols: str, output_path: str | None):
    """
    Detects anomalies in real-time streaming data.
    Example: --feature-cols "price,quantity" --output-path "data/delta/anomalies"
    """
    spark = SparkSession.builder.appName("AnomalyDetection").getOrCreate()
    detector = AnomalyDetector(spark, stream_source_path)
    cols_list = [col.strip() for col in feature_cols.split(",")]
    detector.run_detection(cols_list, output_path)
    print("Anomaly detection pipeline started.")


if __name__ == "__main__":
    anomaly()

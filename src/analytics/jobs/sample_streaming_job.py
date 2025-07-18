from src.analytics.jobs.base_job import SparkJob
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

class SampleStreamingJob(SparkJob):
    """
    A sample Spark streaming job that reads from Kafka, transforms data, and writes to console.
    """

    def __init__(self, app_name: str, master: str = "spark://spark-master:7077", kafka_bootstrap_servers: str = "kafka:29092", kafka_topic: str = "test_topic"):
        super().__init__(app_name, master)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)

    def run(self):
        self.logger.info(f"Starting {self.app_name} streaming job...")

        # Define schema for the incoming Kafka message value
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .load()

        # Parse JSON value and apply schema
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        self.logger.info("Streaming DataFrame schema:")
        parsed_df.printSchema()

        # Simple streaming transformation: count events per user
        transformed_df = self._transform_stream(parsed_df)

        # Write to console for demonstration
        query = transformed_df \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .trigger(processingTime="5 seconds") \
            .start()

        self.logger.info(f"{self.app_name} streaming job started. Waiting for termination...")
        query.awaitTermination()
        self.logger.info(f"{self.app_name} streaming job terminated.")

    def _transform_stream(self, df: DataFrame) -> DataFrame:
        """
        Performs a sample streaming transformation: counts events per user.
        """
        from pyspark.sql import functions as F
        return df.groupBy("user_id").agg(F.count("*").alias("event_count"))

if __name__ == "__main__":
    job = SampleStreamingJob("SampleStreamingJob")
    try:
        job.run()
    finally:
        job.stop()
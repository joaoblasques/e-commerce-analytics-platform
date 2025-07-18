from src.analytics.jobs.base_job import SparkJob
from pyspark.sql import DataFrame

class SampleBatchJob(SparkJob):
    """
    A sample Spark batch job that reads data, transforms it, and writes the output.
    """

    def __init__(self, app_name: str, master: str = "spark://spark-master:7077"):
        super().__init__(app_name, master)
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)

    def run(self):
        self.logger.info(f"Starting {self.app_name} batch job...")

        # Sample data (replace with actual data source in a real scenario)
        data = [
            ("user1", "productA", 10.0),
            ("user2", "productB", 20.0),
            ("user1", "productC", 15.0),
            ("user3", "productA", 5.0),
        ]
        columns = ["user_id", "product_id", "amount"]
        df = self.spark.createDataFrame(data, columns)

        self.logger.info("Original DataFrame schema:")
        df.printSchema()
        self.logger.info("Original DataFrame content:")
        df.show()

        # Simple transformation: calculate total amount per user
        transformed_df = self._transform_data(df)

        self.logger.info("Transformed DataFrame schema:")
        transformed_df.printSchema()
        self.logger.info("Transformed DataFrame content:")
        transformed_df.show()

        self.logger.info(f"{self.app_name} batch job completed.")

    def _transform_data(self, df: DataFrame) -> DataFrame:
        """
        Performs a sample transformation: calculates total amount per user.
        """
        from pyspark.sql import functions as F
        return df.groupBy("user_id").agg(F.sum("amount").alias("total_amount"))

if __name__ == "__main__":
    job = SampleBatchJob("SampleBatchJob")
    try:
        job.run()
    finally:
        job.stop()
from pyspark.sql import DataFrame

from src.analytics.jobs.base_job import SparkJob


class SampleBatchJob(SparkJob):
    """
    A sample Spark batch job that reads data, transforms it, and writes the output.
    """

    def __init__(self, app_name: str, master: str = "spark://spark-master:7077"):
        super().__init__(app_name, master)
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(
            self.__class__.__name__
        )

    def _run_job_logic(self):
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

        # Perform data validation
        self._validate_data(transformed_df)

        self.logger.info(f"{self.app_name} batch job completed.")

    def _transform_data(self, df: DataFrame) -> DataFrame:
        """
        Performs a sample transformation: calculates total amount per user.
        """
        from pyspark.sql import functions as F

        return df.groupBy("user_id").agg(F.sum("amount").alias("total_amount"))

    def _validate_data(self, df: DataFrame):
        """
        Performs basic data validation checks on the DataFrame.
        - Checks for null values in key columns.
        - Checks data types of columns.
        """
        self.logger.info("Starting data validation...")

        # Check for null values in 'user_id' and 'total_amount'
        null_check_cols = ["user_id", "total_amount"]
        for col_name in null_check_cols:
            if col_name in df.columns:
                null_count = df.filter(df[col_name].isNull()).count()
                if null_count > 0:
                    self.logger.warning(
                        f"Column '{col_name}' has {null_count} null values."
                    )
                else:
                    self.logger.info(f"Column '{col_name}' has no null values.")
            else:
                self.logger.warning(
                    f"Column '{col_name}' not found in DataFrame for null check."
                )

        # Check data types
        expected_types = {"user_id": "string", "total_amount": "double"}
        for col_name, expected_type in expected_types.items():
            if col_name in df.columns:
                actual_type = df.schema[col_name].dataType.simpleString()
                if actual_type != expected_type:
                    self.logger.warning(
                        f"Column '{col_name}' has unexpected type: {actual_type}, expected {expected_type}."
                    )
                else:
                    self.logger.info(
                        f"Column '{col_name}' has expected type: {actual_type}."
                    )
            else:
                self.logger.warning(
                    f"Column '{col_name}' not found in DataFrame for type check."
                )

        self.logger.info("Data validation completed.")


if __name__ == "__main__":
    job = SampleBatchJob("SampleBatchJob")
    try:
        job.run()
    finally:
        job.stop()

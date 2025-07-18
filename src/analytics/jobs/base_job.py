from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from src.utils.spark_utils import get_spark_session


class SparkJob(ABC):
    """
    Base class for Spark jobs, providing a standardized way to create and manage SparkSessions.
    """

    def __init__(self, app_name: str, master: str = "spark://spark-master:7077"):
        self.app_name = app_name
        self.master = master
        self.spark: SparkSession = get_spark_session(app_name, master)
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(
            self.__class__.__name__
        )

    def run(self):
        """
        Main method to be implemented by subclasses for job-specific logic.
        """
        import time

        start_time = time.time()
        self.logger.info(f"Starting {self.app_name} job...")
        try:
            self._run_job_logic()
        except Exception as e:
            self.logger.error(f"Error running {self.app_name} job: {e}", exc_info=True)
            raise
        finally:
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"Finished {self.app_name} job in {duration:.2f} seconds.")

    @abstractmethod
    def _run_job_logic(self):
        """
        Actual job logic to be implemented by subclasses.
        """
        raise NotImplementedError(
            "Subclasses must implement the '_run_job_logic' method."
        )

    def stop(self):
        """
        Stops the SparkSession.
        """
        if self.spark:
            self.spark.stop()
            self.logger.info(f"SparkSession for {self.app_name} stopped.")

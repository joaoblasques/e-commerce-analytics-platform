from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from src.utils.spark_utils import get_spark_session

class SparkJob(ABC):
    """
    Abstract base class for all Spark jobs.
    Provides common functionality like SparkSession initialization.
    """

    def __init__(self, app_name: str, master: str = "spark://spark-master:7077"):
        self.app_name = app_name
        self.master = master
        self.spark = self._initialize_spark_session()

    def _initialize_spark_session(self) -> SparkSession:
        """
        Initializes and returns a SparkSession.
        """
        return get_spark_session(self.app_name, self.master)

    @abstractmethod
    def run(self):
        """
        Abstract method to be implemented by concrete Spark jobs.
        Contains the main logic of the Spark job.
        """
        pass

    def stop(self):
        """
        Stops the SparkSession.
        """
        if self.spark:
            self.spark.stop()

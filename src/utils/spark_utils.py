from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "ECommerceAnalytics", master: str = "spark://spark-master:7077") -> SparkSession:
    """
    Provides a standardized SparkSession for the e-commerce analytics platform.
    """
    spark = SparkSession.builder         .appName(app_name)         .master(master)         .config("spark.sql.shuffle.partitions", "200")         .config("spark.sql.adaptive.enabled", "true")         .config("spark.executor.memory", "1g")         .config("spark.driver.memory", "1g")         .config("spark.executor.cores", "1")         .config("spark.dynamicAllocation.enabled", "true")         .config("spark.dynamicAllocation.minExecutors", "1")         .config("spark.dynamicAllocation.maxExecutors", "5")         .config("spark.shuffle.service.enabled", "true")         .config("spark.network.timeout", "300s")         .config("spark.rpc.message.maxSize", "256")         .enableHiveSupport()         .getOrCreate()
    return spark

from pyspark.sql import SparkSession


class PySparkLogger:
    def __init__(self, spark: SparkSession):
        self.log4jLogger = spark.sparkContext._jvm.org.apache.log4j

    def get_logger(self, name: str):
        return self.log4jLogger.LogManager.getLogger(name)
from pyspark.sql import SparkSession

from data_compaction_job.main import start_job

spark = SparkSession.builder \
    .appName("Data Compaction") \
    .getOrCreate()

start_job(spark)

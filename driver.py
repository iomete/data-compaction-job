from pyspark.sql import SparkSession

from data_compaction_job.config import get_config
from data_compaction_job.main import start_job

config = get_config("/etc/configs/application.conf")

spark = SparkSession.builder.appName("Data Compaction").getOrCreate()

start_job(spark, config)

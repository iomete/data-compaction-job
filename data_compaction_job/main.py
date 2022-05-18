"""Main module."""

from pyspark.sql import SparkSession

from data_compaction_job.sql_compaction import SqlCompaction


def start_job(spark: SparkSession):
    compaction = SqlCompaction(spark)
    compaction.run_compaction()

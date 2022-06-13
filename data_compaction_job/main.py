"""Main module."""

from data_compaction_job.logger import init_logger
from data_compaction_job.sql_compaction import SqlCompaction


def start_job(spark, config):
    init_logger()
    compaction = SqlCompaction(spark, config)
    compaction.run_compaction()

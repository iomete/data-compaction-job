#!/usr/bin/env python

"""Tests for `data_compaction_job` package."""

from data_compaction_job.main import start_job
from tests._spark_session import get_spark_session


def test_spark_session():
    # create test spark instance
    spark = get_spark_session()

    spark.sql(f"create table sample_table (id int, data varchar(64))")
    spark.sql(f"insert into sample_table (id, data) values (1, 'test1'), (2, 'test2')")

    # run target
    start_job(spark)
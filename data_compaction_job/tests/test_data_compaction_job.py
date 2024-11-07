#!/usr/bin/env python

"""Tests for `data_compaction_job` package."""

from data_compaction_job.config import get_config
from data_compaction_job.main import start_job
from data_compaction_job.tests._spark_session import get_spark_session


def test_spark_session():
    config = get_config("application.conf")

    # create test spark instance
    spark = get_spark_session()

    # Create tables
    spark.sql(f"create database if not exists default")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS default.copy_on_write_table (
        id BIGINT,
        name STRING,
        age INT,
        zipcode STRING,
        timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zipcode)
    TBLPROPERTIES (
        'write.update.mode' = 'copy-on-write',
        'write.merge.mode' = 'copy-on-write',
        'write.delete.mode' = 'copy-on-write'
    )""")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS default.merge_on_read_table (
        id BIGINT,
        name STRING,
        age INT,
        zipcode STRING,
        timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zipcode)
    TBLPROPERTIES (
        'write.update.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read',
        'write.delete.mode' = 'merge-on-read'
    )""")


    # Insert data
    spark.sql("""
    INSERT INTO default.copy_on_write_table (id, name, age, zipcode, timestamp)
VALUES
  (1, 'Alice', 30, '111111', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (2, 'Bob', 25, '111111', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (3, 'Charlie', 35, '111111', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (4, 'David', 28, '111111', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (5, 'Eve', 32, '111111', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (6, 'Frank', 40, '111111', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (7, 'Grace', 22, '111111', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (8, 'Hank', 45, '111111', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (9, 'Ivy', 38, '111111', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (10, 'Jack', 29, '111111', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (11, 'Alice', 30, '222222', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (12, 'Bob', 25, '222222', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (13, 'Charlie', 35, '222222', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (14, 'David', 28, '222222', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (15, 'Eve', 32, '222222', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (16, 'Frank', 40, '222222', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (17, 'Grace', 22, '222222', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (18, 'Hank', 45, '222222', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (19, 'Ivy', 38, '222222', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (20, 'Jack', 29, '222222', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (21, 'Alice', 30, '333333', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (22, 'Bob', 25, '333333', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (23, 'Charlie', 35, '333333', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (24, 'David', 28, '333333', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (25, 'Eve', 32, '333333', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (26, 'Frank', 40, '333333', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (27, 'Grace', 22, '333333', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (28, 'Hank', 45, '333333', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (29, 'Ivy', 38, '333333', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (30, 'Jack', 29, '333333', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (31, 'Alice', 30, '444444', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (32, 'Bob', 25, '444444', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (33, 'Charlie', 35, '444444', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (34, 'David', 28, '444444', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (35, 'Eve', 32, '444444', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (36, 'Frank', 40, '444444', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (37, 'Grace', 22, '444444', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (38, 'Hank', 45, '444444', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (39, 'Ivy', 38, '444444', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (40, 'Jack', 29, '444444', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (41, 'Alice', 30, '555555', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (42, 'Bob', 25, '555555', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (43, 'Charlie', 35, '555555', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (44, 'David', 28, '555555', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (45, 'Eve', 32, '555555', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (46, 'Frank', 40, '555555', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (47, 'Grace', 22, '555555', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (48, 'Hank', 45, '555555', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (49, 'Ivy', 38, '555555', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (50, 'Jack', 29, '555555', CAST('2023-10-19 19:00:00' AS TIMESTAMP))
    """)

    spark.sql("""
    INSERT INTO default.merge_on_read_table (id, name, age, zipcode, timestamp)
VALUES
  (1, 'Alice', 30, '111111', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (2, 'Bob', 25, '111111', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (3, 'Charlie', 35, '111111', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (4, 'David', 28, '111111', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (5, 'Eve', 32, '111111', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (6, 'Frank', 40, '111111', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (7, 'Grace', 22, '111111', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (8, 'Hank', 45, '111111', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (9, 'Ivy', 38, '111111', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (10, 'Jack', 29, '111111', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (11, 'Alice', 30, '222222', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (12, 'Bob', 25, '222222', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (13, 'Charlie', 35, '222222', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (14, 'David', 28, '222222', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (15, 'Eve', 32, '222222', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (16, 'Frank', 40, '222222', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (17, 'Grace', 22, '222222', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (18, 'Hank', 45, '222222', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (19, 'Ivy', 38, '222222', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (20, 'Jack', 29, '222222', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (21, 'Alice', 30, '333333', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (22, 'Bob', 25, '333333', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (23, 'Charlie', 35, '333333', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (24, 'David', 28, '333333', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (25, 'Eve', 32, '333333', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (26, 'Frank', 40, '333333', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (27, 'Grace', 22, '333333', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (28, 'Hank', 45, '333333', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (29, 'Ivy', 38, '333333', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (30, 'Jack', 29, '333333', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (31, 'Alice', 30, '444444', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (32, 'Bob', 25, '444444', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (33, 'Charlie', 35, '444444', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (34, 'David', 28, '444444', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (35, 'Eve', 32, '444444', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (36, 'Frank', 40, '444444', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (37, 'Grace', 22, '444444', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (38, 'Hank', 45, '444444', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (39, 'Ivy', 38, '444444', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (40, 'Jack', 29, '444444', CAST('2023-10-19 19:00:00' AS TIMESTAMP)),

  (41, 'Alice', 30, '555555', CAST('2023-10-10 10:00:00' AS TIMESTAMP)),
  (42, 'Bob', 25, '555555', CAST('2023-10-11 11:00:00' AS TIMESTAMP)),
  (43, 'Charlie', 35, '555555', CAST('2023-10-12 12:00:00' AS TIMESTAMP)),
  (44, 'David', 28, '555555', CAST('2023-10-13 13:00:00' AS TIMESTAMP)),
  (45, 'Eve', 32, '555555', CAST('2023-10-14 14:00:00' AS TIMESTAMP)),
  (46, 'Frank', 40, '555555', CAST('2023-10-15 15:00:00' AS TIMESTAMP)),
  (47, 'Grace', 22, '555555', CAST('2023-10-16 16:00:00' AS TIMESTAMP)),
  (48, 'Hank', 45, '555555', CAST('2023-10-17 17:00:00' AS TIMESTAMP)),
  (49, 'Ivy', 38, '555555', CAST('2023-10-18 18:00:00' AS TIMESTAMP)),
  (50, 'Jack', 29, '555555', CAST('2023-10-19 19:00:00' AS TIMESTAMP))
  """)

    # Post insert validation
    insert_cow_df = spark.sql("SELECT * FROM default.copy_on_write_table")
    insert_mor_df = spark.sql("SELECT * FROM default.merge_on_read_table")

    assert insert_cow_df.count() == insert_cow_df.count()
    assert insert_cow_df.subtract(insert_mor_df).count() == 0
    assert insert_mor_df.subtract(insert_cow_df).count() == 0

    # Update data
    spark.sql("""
    UPDATE default.copy_on_write_table
    SET age = age + 1
    WHERE (zipcode = '111111' AND id IN (1, 2)) OR
        (zipcode = '222222' AND id IN (11, 12)) OR
        (zipcode = '333333' AND id IN (21, 22)) OR
        (zipcode = '444444' AND id IN (31, 32)) OR
        (zipcode = '555555' AND id IN (41, 42))
    """)

    spark.sql("""
    UPDATE default.merge_on_read_table
    SET age = age + 1
    WHERE (zipcode = '111111' AND id IN (1, 2)) OR
        (zipcode = '222222' AND id IN (11, 12)) OR
        (zipcode = '333333' AND id IN (21, 22)) OR
        (zipcode = '444444' AND id IN (31, 32)) OR
        (zipcode = '555555' AND id IN (41, 42))
    """)

    # Post update validation
    update_cow_df = spark.sql("SELECT * FROM default.copy_on_write_table")
    update_mor_df = spark.sql("SELECT * FROM default.merge_on_read_table")

    assert update_cow_df.count() == update_mor_df.count()
    assert update_cow_df.subtract(update_mor_df).count() == 0
    assert update_mor_df.subtract(update_cow_df).count() == 0

    # Run compaction job
    start_job(spark, config)

    # Post compaction validation
    compaction_cow_df = spark.sql("SELECT * FROM default.copy_on_write_table")
    compaction_mor_df = spark.sql("SELECT * FROM default.merge_on_read_table")

    assert compaction_cow_df.count() == compaction_mor_df.count()
    assert compaction_cow_df.subtract(compaction_mor_df).count() == 0
    assert compaction_mor_df.subtract(compaction_cow_df).count() == 0

    assert compaction_cow_df.count() == update_cow_df.count()
    assert compaction_cow_df.subtract(update_cow_df).count() == 0
    assert update_cow_df.subtract(compaction_cow_df).count() == 0

    # Run compaction job again
    start_job(spark, config)

    # Post second compaction validation
    compaction_cow_df = spark.sql("SELECT * FROM default.copy_on_write_table")
    compaction_mor_df = spark.sql("SELECT * FROM default.merge_on_read_table")

    assert compaction_cow_df.count() == compaction_mor_df.count()
    assert compaction_cow_df.subtract(compaction_mor_df).count() == 0
    assert compaction_mor_df.subtract(compaction_cow_df).count() == 0

    assert compaction_cow_df.count() == update_cow_df.count()
    assert compaction_cow_df.subtract(update_cow_df).count() == 0
    assert update_cow_df.subtract(compaction_cow_df).count() == 0

if __name__ == '__main__':
    test_spark_session()

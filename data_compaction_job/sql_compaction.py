import logging
from time import time

from pyspark.sql import SparkSession

from data_compaction_job.utils import PySparkLogger

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

project_logger = logging.getLogger("src")
project_logger.setLevel(logging.DEBUG)

logger = logging.getLogger("SqlCompaction")
logger.setLevel(logging.INFO)


class SqlCompaction:
    def __init__(self, spark: SparkSession):
        self.spark = spark

        self.spark.sparkContext.setLogLevel("INFO")
        self.logger = PySparkLogger(spark).get_logger(__name__)

    def run_compaction(self):
        databases = self.spark.catalog.listDatabases()
        for database in databases:
            self.logger.info(f"Intorspecting database={database.name}")
            tables = self.spark.sql(f"show tables from {database.name}").collect()

            for table in tables:
                try:
                    self.process_table(database.name, table.tableName)
                except Exception as e:
                    self.logger.error(f"Error processing table={database.name}.{table.tableName} error={e}")


    def process_table(self, namespace, table_name):
        tableMeta = self.spark.sql(f"describe extended {namespace}.{table_name}").collect()

        # Skip, if not an `iceberg` table
        if not any(row.col_name == "Provider" and row.data_type == "iceberg" for row in tableMeta):
            return

        self.logger.info(f"table={namespace}.{table_name} type=iceberg. Starting data compaction...")

        self.expire_snapshots(namespace, table_name)
        self.remove_orphan_files(namespace, table_name)
        self.rewrite_data_files(namespace, table_name)
        self.rewrite_manifest(namespace, table_name)

    def expire_snapshots(self, namespace, table_name):
        self.logger.debug(f"expire_snapshots started, table={namespace}.{table_name}")
        start_time = time.perf_counter()

        self.spark.sql(f"CALL spark_catalog.system.expire_snapshots(table => '{namespace}.{table_name}')")

        end_time = time.perf_counter()
        self.logger.info(f"expire_snapshots finished! table={namespace}.{table_name} duration={end_time - start_time:0.4f} seconds")

    def remove_orphan_files(self, namespace, table_name):
        self.logger.debug(f"remove_orphan_files started, table={namespace}.{table_name}")
        start_time = time.perf_counter()

        self.spark.sql(f"CALL spark_catalog.system.remove_orphan_files(table => '{namespace}.{table_name}')")

        end_time = time.perf_counter()
        self.logger.info(f"remove_orphan_files finished! table={namespace}.{table_name} duration={end_time - start_time:0.4f} seconds")

    def rewrite_manifest(self, namespace, table_name):
        self.logger.debug(f"rewrite_manifest started, table={namespace}.{table_name}")
        start_time = time.perf_counter()

        self.spark.sql(f"CALL spark_catalog.system.rewrite_manifests(table => '{namespace}.{table_name}')")

        end_time = time.perf_counter()
        self.logger.info(f"rewrite_manifest finished! table={namespace}.{table_name} duration={end_time - start_time:0.4f} seconds")

    def rewrite_data_files(self, namespace, table_name):
        self.logger.debug(f"rewrite_data_files started, table={namespace}.{table_name}")
        start_time = time.perf_counter()

        self.spark.sql(f"CALL spark_catalog.system.rewrite_data_files(table => '{namespace}.{table_name}')")

        end_time = time.perf_counter()
        self.logger.info(f"rewrite_data_files finished! table={namespace}.{table_name} duration={end_time - start_time:0.4f} seconds")

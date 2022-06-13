import logging
import time

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class SqlCompaction:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run_compaction(self):
        databases = self.spark.catalog.listDatabases()
        for database in databases:
            logger.info(f"{database.name}:\t Intorspecting database")
            tables = self.spark.sql(f"show tables from {database.name}").collect()

            for table in tables:
                try:
                    self.process_table(database.name, table.tableName)
                except Exception as e:
                    logger.error(f"{database.name}.{table.tableName}:\t Error processing table, error={e}")


    def process_table(self, namespace, table_name):
        tableMeta = self.spark.sql(f"describe extended {namespace}.{table_name}").collect()

        # Skip, if not an `iceberg` table
        if not any(row.col_name == "Provider" and row.data_type == "iceberg" for row in tableMeta):
            return

        logger.info(f"{namespace}.{table_name}:\t with type=iceberg. Starting data compaction...")

        self.expire_snapshots(namespace, table_name)
        self.remove_orphan_files(namespace, table_name)
        self.rewrite_data_files(namespace, table_name)
        self.rewrite_manifest(namespace, table_name)

    def expire_snapshots(self, namespace, table_name):
        logger.debug(f"{namespace}.{table_name}:\t expire_snapshots started")
        start_time = time.time()

        self.spark.sql(f"CALL spark_catalog.system.expire_snapshots(table => '{namespace}.{table_name}')")

        end_time = time.time()
        logger.info(f"{namespace}.{table_name}:\t expire_snapshots finished! duration={end_time - start_time:0.2f} seconds")

    def remove_orphan_files(self, namespace, table_name):
        logger.debug(f"{namespace}.{table_name}:\t remove_orphan_files started")
        start_time = time.time()

        self.spark.sql(f"CALL spark_catalog.system.remove_orphan_files(table => '{namespace}.{table_name}')")

        end_time = time.time()
        logger.info(f"{namespace}.{table_name}:\t remove_orphan_files finished! duration={end_time - start_time:0.2f} seconds")

    def rewrite_manifest(self, namespace, table_name):
        logger.debug(f"{namespace}.{table_name}:\t rewrite_manifest started")
        start_time = time.time()

        self.spark.sql(f"CALL spark_catalog.system.rewrite_manifests(table => '{namespace}.{table_name}')")

        end_time = time.time()
        logger.info(f"{namespace}.{table_name}:\t rewrite_manifest finished! duration={end_time - start_time:0.2f} seconds")

    def rewrite_data_files(self, namespace, table_name):
        logger.debug(f"{namespace}.{table_name}:\t rewrite_data_files started")
        start_time = time.time()

        self.spark.sql(f"CALL spark_catalog.system.rewrite_data_files(table => '{namespace}.{table_name}')")

        end_time = time.time()
        logger.info(f"{namespace}.{table_name}:\t rewrite_data_files finished! duration={end_time - start_time:0.2f} seconds")

import logging
import time
import os
import requests


from pyspark.sql import SparkSession

from data_compaction_job.config import ApplicationConfig, ExpireSnapshotConfig, RemoveOrphanFilesConfig, \
    RewriteDataFilesConfig, RewriteManifestsConfig

logger = logging.getLogger(__name__)


from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession

class SqlCompaction:
    def __init__(self, spark: SparkSession, config: ApplicationConfig):
        self.spark = spark
        self.config = config

    def run_compaction(self):
        with ThreadPoolExecutor(max_workers=self.config.parallelism) as executor:
            futures = []
            for catalog in self.__get_catalogs():
                logger.info(f"[{catalog}] Intorspecting catalog")
                for database in self.__get_databases(catalog):
                    db_name = database.namespace
                    logger.info(f"[{db_name}] Intorspecting database")
                    tables = self.spark.sql(f"show tables from {catalog}.{db_name}").collect()
                    for table in tables:
                        futures.append(executor.submit(self.__process_table_if_iceberg, catalog, db_name, table))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing table, error={e}")

    def __process_table_if_iceberg(self, catalog, db_name, table):
        table_name = table.tableName
        try:
            tableMeta = self.spark.sql(f"describe extended {db_name}.{table_name}").collect()

            # Skip, if not an `iceberg` table
            if not any(row.col_name == "Provider" and row.data_type == "iceberg" for row in tableMeta):
                return

            message = f"[{db_name}.{table_name}] table compaction"
            timer(message)(self.__process_table)(catalog, db_name, table_name)

        except Exception as e:
            logger.error(f"[{db_name}.{table_name}] Error processing table, error={e}")

    def __process_table(self, catalog, database, table_name):
        self.__expire_snapshots(catalog, database, table_name, self.config.expire_snapshot)
        self.__remove_orphan_files(catalog, database, table_name, self.config.remove_orphan_files)
        self.__rewrite_data_files(catalog, database, table_name, self.config.rewrite_data_files)
        self.__rewrite_manifest(catalog, database, table_name, self.config.rewrite_manifests)

    def __expire_snapshots(self, catalog, database, table_name, expire_snapshot_config: ExpireSnapshotConfig):
        options = f"table => '{database}.{table_name}', retain_last => {expire_snapshot_config.retain_last}"
        self.spark.sql(f"CALL {catalog}.system.expire_snapshots({options})")

    def __remove_orphan_files(self, catalog, database, table_name, remove_orphan_files_config: RemoveOrphanFilesConfig):
        options = f"table => '{database}.{table_name}'"
        self.spark.sql(f"CALL {catalog}.system.remove_orphan_files({options})")

    def __rewrite_manifest(self, catalog, database, table_name, rewrite_manifests_config: RewriteManifestsConfig):
        options = f"table => '{database}.{table_name}'"
        if rewrite_manifests_config.use_caching is not None:
            options += f", use_caching => {rewrite_manifests_config.use_caching}"

        self.spark.sql(f"CALL {catalog}.system.rewrite_manifests({options})")

    def __rewrite_data_files(self, catalog, database, table_name, rewrite_data_files_config: RewriteDataFilesConfig):
        options = f"table => '{database}.{table_name}'"
        if rewrite_data_files_config.options:
            map = ', '.join(', '.join((f"'{k}'", f"'{v}'")) for (k, v) in rewrite_data_files_config.options.items())
            options += f", options => map({map})"

        self.spark.sql(f"CALL {catalog}.system.rewrite_data_files({options})")

    def __get_catalogs(self):
        sql_client = SqlClient()
        return sql_client.catalogs()

    def __get_databases(self, catalog):
        databases = self.spark.sql(f"show databases from {catalog}").collect()
        return databases


def timer(message: str):
    def timer_decorator(method):
        def timer_func(*args, **kw):
            logger.debug(f"{message} started")
            start_time = time.time()
            result = method(*args, **kw)
            duration = (time.time() - start_time)
            logger.info(f"{message} completed in {duration:0.2f} seconds")
            return result

        return timer_func

    return timer_decorator


class SqlClient:
    def __init__(self):
        self.base_url = os.getenv("SQL_API_ENDPOINT", "http://iom-core")

    def catalogs(self):
        response = requests.get(f"{self.base_url}/api/internal/sql/schema/catalogs")
        if response.status_code == 200:
            return set(response.json())
        else:
            response.raise_for_status()

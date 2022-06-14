import logging
import time
from typing import Any

from pyspark.sql import SparkSession

from data_compaction_job.config import ApplicationConfig, ExpireSnapshotConfig, RemoveOrphanFilesConfig, RewriteDataFilesConfig, RewriteManifestsConfig

logger = logging.getLogger(__name__)


class SqlCompaction:
    def __init__(self, spark: SparkSession, config: ApplicationConfig):
        self.spark = spark
        self.config = config

    def run_compaction(self):
        for database in self.spark.catalog.listDatabases():
            db_name = database.name
            logger.info(f"[{db_name}] Intorspecting database")

            tables = self.spark.sql(f"show tables from {db_name}").collect()

            max_table_name_length = max([len(table.tableName) for table in tables]) if len(tables) > 0 else 0
            for table in tables:
                table_name = table.tableName
                try:
                    tableMeta = self.spark.sql(f"describe extended {db_name}.{table_name}").collect()

                    # Skip, if not an `iceberg` table
                    if not any(row.col_name == "Provider" and row.data_type == "iceberg" for row in tableMeta):
                        continue

                    message = f"[{db_name}.{table_name: <{max_table_name_length}}] table compaction"
                    timer(message)(self.__process_table)(db_name, table_name)

                except Exception as e:
                    logger.error(f"[{db_name}.{table_name: <{max_table_name_length}}] Error processing table, error={e}")

    def __process_table(self, namespace, table_name):
        self.__expire_snapshots(namespace, table_name, self.config.expire_snapshot)
        self.__remove_orphan_files(namespace, table_name, self.config.remove_orphan_files)
        self.__rewrite_data_files(namespace, table_name, self.config.rewrite_data_files)
        self.__rewrite_manifest(namespace, table_name, self.config.rewrite_manifests)

    def __expire_snapshots(self, namespace, table_name, expire_snapshot_config: ExpireSnapshotConfig):
        options = f"table => '{namespace}.{table_name}', retain_last => {expire_snapshot_config.retain_last}"
        self.spark.sql(f"CALL spark_catalog.system.expire_snapshots({options})")

    def __remove_orphan_files(self, namespace, table_name, remove_orphan_files_config: RemoveOrphanFilesConfig):
        options = f"table => '{namespace}.{table_name}'"
        self.spark.sql(f"CALL spark_catalog.system.remove_orphan_files({options})")

    def __rewrite_manifest(self, namespace, table_name, rewrite_manifests_config: RewriteManifestsConfig):
        options = f"table => '{namespace}.{table_name}'"
        if rewrite_manifests_config.use_caching is not None:
            options += f", use_caching => {rewrite_manifests_config.use_caching}"

        self.spark.sql(f"CALL spark_catalog.system.rewrite_manifests({options})")

    def __rewrite_data_files(self, namespace, table_name, rewrite_data_files_config: RewriteDataFilesConfig):
        options = f"table => '{namespace}.{table_name}'"
        if rewrite_data_files_config.options:
            map = ', '.join(', '.join((f"'{k}'",f"'{v}'")) for (k,v) in rewrite_data_files_config.options.items())
            options += f", options => map({map})"

        self.spark.sql(f"CALL spark_catalog.system.rewrite_data_files({options})")


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

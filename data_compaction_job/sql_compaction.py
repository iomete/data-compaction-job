import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import cache

import requests

from data_compaction_job.config import ApplicationConfig, RewriteManifestsConfig
from stats_emitter import emit_stats, init_emitter

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
            catalog  = self.__get_catalog()
            logger.info(f"Starting table optimisation for catalog: {catalog}")

            databases = self.__get_databases(catalog)
            logger.info(f"Databases in catalog '{catalog}' considered for optimisation : {databases}")

            db_table_mapping = defaultdict(list)
            for database in databases:
                logger.info(f"Introspecting database: {database}")
                tables = self.__get_tables(catalog, database)
                logger.info(f"Tables in database '{database}' considered for optimisation : {tables}")
                if tables:
                    db_table_mapping[database] = tables

            init_emitter(self.spark)
            for database in databases:
                for table in db_table_mapping[database]:
                    futures.append(executor.submit(self.__process_table_if_iceberg, catalog, database, table))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing table, error={e}")

    def __process_table_if_iceberg(self, catalog, db_name, table):
        try:
            table_meta = self.spark.sql(f"describe extended {db_name}.{table}").collect()

            # Skip, if not an `iceberg` table
            if not any(row.col_name == "Provider" and row.data_type == "iceberg" for row in table_meta):
                return

            message = f"[{db_name}.{table}] table compaction"
            timer(message)(self.__process_table)(catalog, db_name, table)

        except Exception as e:
            logger.error(f"[{db_name}.{table}] Error processing table, error={e}")

    def __process_table(self, catalog, database, table_name):
        self.__expire_snapshots(catalog, database, table_name)
        self.__remove_orphan_files(catalog, database, table_name)
        self.__rewrite_data_files(catalog, database, table_name)
        self.__rewrite_manifest(catalog, database, table_name)

    @emit_stats("EXPIRE_SNAPSHOTS")
    def __expire_snapshots(self, catalog, database, table_name):
        timestamp = datetime.now() - timedelta(minutes=5)
        retain_last =int(self.__get_final_config_for_table(database,
                                                           table_name,
                                                           "expire_snapshot",
                                                           "retain_last")
                         or self.config.expire_snapshot.retain_last)
        options = (f"table => '{database}.{table_name}',"
                   f" retain_last => {retain_last},"
                   f" older_than => TIMESTAMP '{timestamp}'")
        query = f"CALL {catalog}.system.expire_snapshots({options})"
        result = self.spark.sql(query).collect()
        return result, query

    @emit_stats("REMOVE_ORPHAN_FILES")
    def __remove_orphan_files(self, catalog, database, table_name):
        days = int(self.__get_final_config_for_table(database,
                                                     table_name,
                                                     "remove_orphan_files",
                                                     "older_than_days")
                   or self.config.remove_orphan_files.older_than_days)
        timestamp = datetime.now(timezone.utc) - timedelta(days=days)
        options = f"table => '{database}.{table_name}', older_than => TIMESTAMP '{timestamp}'"
        query = f"CALL {catalog}.system.remove_orphan_files({options})"
        result = self.spark.sql(query).collect()
        return result, query

    @emit_stats("REWRITE_MANIFESTS")
    def __rewrite_manifest(self, catalog, database, table_name):
        options = f"table => '{database}.{table_name}'"
        use_caching = (self.__get_final_config_for_table(database,
                                                     table_name,
                                                     "rewrite_manifest",
                                                     "use_caching")
                    or self.config.rewrite_manifests.use_caching)
        if use_caching:
            use_caching = str(use_caching).lower()
            options += f", use_caching => {use_caching}"
        query = f"CALL {catalog}.system.rewrite_manifests({options})"
        result = self.spark.sql(query).collect()
        return result, query

    @emit_stats("REWRITE_DATA_FILES")
    def __rewrite_data_files(self, catalog, database, table_name):
        strategy = (self.__get_final_config_for_table(database,
                                                     table_name,
                                                     "rewrite_data_files",
                                                     "strategy")
                    or self.config.rewrite_data_files.strategy)
        sort_order = (self.__get_final_config_for_table(database,
                                                     table_name,
                                                     "rewrite_data_files",
                                                     "sort_order")
                    or self.config.rewrite_data_files.sort_order)
        rewrite_options = (self.__get_final_config_for_table(database,
                                                     table_name,
                                                     "rewrite_data_files",
                                                     "options")
                    or self.config.rewrite_data_files.options)
        where = (self.__get_final_config_for_table(database,
                                                     table_name,
                                                     "rewrite_data_files",
                                                     "where")
                    or self.config.rewrite_data_files.where)

        options = f"table => '{database}.{table_name}'"

        if strategy and strategy == "sort":
            options += f", strategy => {strategy}, sort_order => {sort_order}"
        if rewrite_options:
            option_map = ', '.join(', '.join((f"'{k}'", f"'{v}'")) for (k, v) in rewrite_options.items())
            options += f", options => map({option_map})"
        if where:
            options += f", where => {where}"

        query = f"CALL {catalog}.system.rewrite_data_files({options})"
        result = self.spark.sql(query).collect()
        return result, query

    def __get_catalog(self):
        catalog = self.config.catalog
        sql_client = SqlClient()
        available_catalogs = sql_client.catalogs()
        if catalog not in available_catalogs:
            logger.error(f"Catalog not found: {catalog}. Available catalogs: {available_catalogs}.")
            raise Exception(f"Catalog not found for optimisation: {catalog}")
        return catalog

    def __get_databases(self, catalog):
        available_databases = [database.namespace for database
                               in self.spark.sql(f"show databases from {catalog}").collect()]
        if self.config.include_exclude.databases:
            return [database for database in self.config.include_exclude.databases if database in available_databases]
        else:
            return available_databases

    def __get_tables(self, catalog, database):
        available_tables = [table.tableName for table
                            in self.spark.sql(f"show tables from {catalog}.{database}").collect()]
        if database in self.__get_table_includes():
            tables = [table for table
                      in self.__get_table_includes()[database]
                      if table in available_tables]
        elif database in self.__get_table_excludes():
            tables = [table for table
                      in available_tables
                      if table not in self.__get_table_excludes()[database]]
        else:
            tables = available_tables
        return tables

    @cache
    def __get_table_excludes(self):
        mapping = defaultdict(list)
        for table in self.config.include_exclude.table_exclude:
            table_split = table.split('.')
            if len(table_split) != 2:
                logger.warning(f"Please provide table in format <database>.<table> instead of {table}")
            else:
                mapping[table_split[0]].append(table_split[1])
        return mapping

    @cache
    def __get_table_includes(self):
        mapping = defaultdict(list)
        for table in self.config.include_exclude.table_include:
            table_split = table.split('.')
            if len(table_split) != 2:
                logger.warning(f"Please provide table in format <database>.<table> instead of {table}")
            else:
                mapping[table_split[0]].append(table_split[1])
        return mapping

    def __get_final_config_for_table(self, database, table, operation, config_name):
        if (self.config.table_overrides
                and f"{database}.{table}" in self.config.table_overrides
                and operation in self.config.table_overrides.get(f"{database}.{table}")
                and config_name in self.config.table_overrides[f"{database}.{table}"][operation]):
            return self.config.table_overrides[f"{database}.{table}"][operation][config_name]
        else:
            return None


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

import logging
import time
from datetime import datetime, timezone
from functools import wraps

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
def emit_stats(operation: str):
    """Emits metric for the table optimisation operation that is being performed."""

    def decorator(func):

        @wraps(func)
        def wrapper_func(*args, **kwargs):
            #pre-execute
            start_time = time.time()

            #execute
            try:
                metrics, sql = func(*args, **kwargs)

            except Exception as e:
                #post-execute error scenario
                end_time = time.time()
                spark: SparkSession = args[0].spark
                spark.sql(f"""INSERT INTO spark_catalog.iomete_system_db.table_optimisation_run_errors VALUES (
                      '{spark.sparkContext.applicationId}',
                      '{args[1]}',
                      '{args[2]}',
                      '{args[3]}',
                      '{operation}',
                      '{str(e)}',
                      TIMESTAMP '{datetime.fromtimestamp(start_time ,timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}',
                      TIMESTAMP '{datetime.fromtimestamp(end_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}'
                    )""")
                logger.error(f"[{args[2]}.{args[3]}] Error running {operation} on table, error={e}")
            else:
                #post-execute happy scenario
                end_time = time.time()

                if operation == "REMOVE_ORPHAN_FILES":
                    metric_map = f"'removed file count', '{len(metrics)}'"
                else:
                    metric_map = ", ".join([f"'{key}', '{value}'" for key, value in metrics[0].asDict().items()])

                spark: SparkSession = args[0].spark
                sql = sql.replace("'", "''")
                spark.sql(f"""INSERT INTO spark_catalog.iomete_system_db.table_optimisation_run_metrics VALUES (
      '{spark.sparkContext.applicationId}',
      '{args[1]}',
      '{args[2]}',
      '{args[3]}',
      '{operation}',
      '{sql}',
      MAP({metric_map}),
      TIMESTAMP '{datetime.fromtimestamp(start_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}',
      TIMESTAMP '{datetime.fromtimestamp(end_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}'
    )""")

        return wrapper_func

    return decorator

def init_emitter(spark: SparkSession):
    spark.sql("create DATABASE IF NOT EXISTS spark_catalog.iomete_system_db")
    spark.sql("""create TABLE IF NOT EXISTS spark_catalog.iomete_system_db.table_optimisation_run_metrics (
  spark_app_id VARCHAR(255),
  catalog_name VARCHAR(255),
  database_name VARCHAR(255),
  table_name VARCHAR(255),
  operation VARCHAR(50),
  query STRING,
  metrics MAP<STRING, STRING>,
  start_time TIMESTAMP,
  end_time TIMESTAMP
  )
  TBLPROPERTIES (
  'commit.retry.num-retries' = '200'
  )""")
    spark.sql("""create TABLE IF NOT EXISTS spark_catalog.iomete_system_db.table_optimisation_run_errors(
  spark_app_id VARCHAR(255),
  catalog_name VARCHAR(255),
  database_name VARCHAR(255),
  table_name VARCHAR(255),
  operation VARCHAR(50),
  error STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP
  )
  TBLPROPERTIES (
  'commit.retry.num-retries' = '200'
  )""")
    spark.sql(" ALTER TABLE spark_catalog.iomete_system_db.table_optimisation_run_metrics SET TBLPROPERTIES ('commit.retry.num-retries' = '200') ")
    spark.sql(" ALTER TABLE spark_catalog.iomete_system_db.table_optimisation_run_errors SET TBLPROPERTIES ('commit.retry.num-retries' = '200') ")
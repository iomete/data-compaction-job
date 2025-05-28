import logging
import time
from datetime import datetime, timezone
from functools import wraps
from threading import Lock
from typing import List, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType

logger = logging.getLogger(__name__)

class StatsBatcher:
    
    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.metrics_batch: List[Dict[str, Any]] = []
        self.errors_batch: List[Dict[str, Any]] = []
        self.lock = Lock()
        
    def add_metric(self, spark_app_id: str, catalog_name: str, database_name: str, 
                   table_name: str, operation: str, query: str, metrics: Dict[str, str], 
                   start_time: datetime, end_time: datetime):
        with self.lock:
            self.metrics_batch.append({
                'spark_app_id': spark_app_id,
                'catalog_name': catalog_name,
                'database_name': database_name,
                'table_name': table_name,
                'operation': operation,
                'query': query,
                'metrics': metrics,
                'start_time': start_time,
                'end_time': end_time
            })
            
            if len(self.metrics_batch) >= self.batch_size:
                self._flush_metrics_batch()
    
    def add_error(self, spark_app_id: str, catalog_name: str, database_name: str, 
                  table_name: str, operation: str, error: str, 
                  start_time: datetime, end_time: datetime):
        with self.lock:
            self.errors_batch.append({
                'spark_app_id': spark_app_id,
                'catalog_name': catalog_name,
                'database_name': database_name,
                'table_name': table_name,
                'operation': operation,
                'error': error,
                'start_time': start_time,
                'end_time': end_time
            })
            
            if len(self.errors_batch) >= self.batch_size:
                self._flush_errors_batch()
    
    def _flush_metrics_batch(self):
        if not self.metrics_batch or not hasattr(self, 'spark'):
            return
            
        try:
            metrics_schema = StructType([
                StructField("spark_app_id", StringType(), True),
                StructField("catalog_name", StringType(), True),
                StructField("database_name", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("operation", StringType(), True),
                StructField("query", StringType(), True),
                StructField("metrics", MapType(StringType(), StringType()), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True)
            ])
            df = self.spark.createDataFrame(self.metrics_batch, schema=metrics_schema)
            df.write.mode("append").insertInto("spark_catalog.iomete_system_db.table_optimisation_run_metrics")
            
            logger.info(f"Flushed {len(self.metrics_batch)} metric records to database")
            self.metrics_batch.clear()
            
        except Exception as e:
            logger.error(f"Error flushing metrics batch: {e}")
            # Keep the batch for retry or manual handling
    
    def _flush_errors_batch(self):
        if not self.errors_batch or not hasattr(self, 'spark'):
            return
            
        try:
            errors_schema = StructType([
                StructField("spark_app_id", StringType(), True),
                StructField("catalog_name", StringType(), True),
                StructField("database_name", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("operation", StringType(), True),
                StructField("error", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True)
            ])

            df = self.spark.createDataFrame(self.errors_batch, schema=errors_schema)
            df.write.mode("append").insertInto("spark_catalog.iomete_system_db.table_optimisation_run_errors")
            
            logger.info(f"Flushed {len(self.errors_batch)} error records to database")
            self.errors_batch.clear()
            
        except Exception as e:
            logger.error(f"Error flushing errors batch: {e}")
            # Keep the batch for retry or manual handling
    
    def flush_all(self):
        """Flush all remaining records in both batches."""
        with self.lock:
            if self.metrics_batch:
                self._flush_metrics_batch()
            if self.errors_batch:
                self._flush_errors_batch()
    
    def set_spark_session(self, spark: SparkSession):
        """Set the Spark session for database operations."""
        self.spark = spark

# Global batcher instance
_stats_batcher = StatsBatcher()

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

                _stats_batcher.add_error(
                    spark_app_id=spark.sparkContext.applicationId,
                    catalog_name=args[1],
                    database_name=args[2],
                    table_name=args[3],
                    operation=operation,
                    error=str(e),
                    start_time=datetime.fromtimestamp(start_time, timezone.utc),
                    end_time=datetime.fromtimestamp(end_time, timezone.utc)
                )
                logger.error(f"[{args[2]}.{args[3]}] Error running {operation} on table, error={e}")
            else:
                #post-execute happy scenario
                end_time = time.time()

                if operation == "REMOVE_ORPHAN_FILES":
                    metrics_map = {'removed file count': str(len(metrics))}
                else:
                    metrics_map = {key: str(value) for key, value in metrics[0].asDict().items()}

                spark: SparkSession = args[0].spark
                _stats_batcher.add_metric(
                    spark_app_id=spark.sparkContext.applicationId,
                    catalog_name=args[1],
                    database_name=args[2],
                    table_name=args[3],
                    operation=operation,
                    query=sql,
                    metrics=metrics_map,
                    start_time=datetime.fromtimestamp(start_time, timezone.utc),
                    end_time=datetime.fromtimestamp(end_time, timezone.utc)
                )

        return wrapper_func

    return decorator

def init_emitter(spark: SparkSession, batch_size: int = 100):
    global _stats_batcher
    _stats_batcher = StatsBatcher(batch_size=batch_size)
    _stats_batcher.set_spark_session(spark)
    
    # Create the necessary database and tables
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

def close_emitter(spark: SparkSession):
    global _stats_batcher
    if _stats_batcher:
        _stats_batcher.flush_all()
        logger.info("Stats emitter closed and all batches flushed")
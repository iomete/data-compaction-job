from pyspark.sql import SparkSession
import tempfile

jar_dependencies = [
    "org.apache.iceberg:iceberg-spark3-runtime:0.13.1"
]

packages = ",".join(jar_dependencies)
print("packages: {}".format(packages))

lakehouse_dir = tempfile.mkdtemp(prefix="iom-lakehouse")

print(f"Lakehouse dir: {lakehouse_dir}")


def get_spark_session():
    spark = SparkSession.builder \
        .appName("Integration Test") \
        .master("local") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", lakehouse_dir) \
        .config("spark.sql.warehouse.dir", lakehouse_dir) \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.legacy.createHiveTableByDefault", "false") \
        .config("spark.sql.sources.default", "iceberg") \
        .getOrCreate()

    return spark

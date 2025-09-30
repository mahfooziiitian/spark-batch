import os

from pyspark.sql import SparkSession

warehouse_location = os.environ["SPARK_WAREHOUSE"]
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]

spark = (
    SparkSession.builder.appName("WarehouseExample")
    .config("spark.sql.warehouse.dir", warehouse_location)
    # .config("spark.executor.memory", "2g")
    # .config("spark.driver.memory", "2g")
    .getOrCreate()
)

# Optional: Log Spark version and warehouse directory
print(f"Spark version: {spark.version}")
print(f"Warehouse directory: {spark.conf.get('spark.sql.warehouse.dir')}")

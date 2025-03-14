import os
import sys

from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    data_home = os.environ["DATA_HOME"]
    builder = (
        SparkSession.builder.appName("versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.local.dir", f"{data_home}/Processing/Batch/Spark/temp")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Specify the Delta table path
    # delta_table_path = f"{data_home}/FileData/Delta/manifest/manifest-table"
    delta_table_path = f"{data_home}/FileData/Delta/partition/partition-table"

    # Read the manifest file using the DeltaTable API
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    manifest_details = delta_table.history().select("version", "timestamp", "operation", "operationParameters") \
        .filter("operation = 'WRITE'").collect()

    # Print details of the manifest file for each write operation
    for entry in manifest_details:
        version = entry.version
        timestamp = entry.timestamp
        operation = entry.operation
        parameters = entry.operationParameters
        print(f"Version: {version}, Timestamp: {timestamp}, Operation: {operation}, Parameters: {parameters}")

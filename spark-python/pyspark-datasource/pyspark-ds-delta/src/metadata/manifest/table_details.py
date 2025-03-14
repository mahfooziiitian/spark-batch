import os
import sys

from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    builder = (
        SparkSession.builder.appName("table_details")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    data_home = os.environ["DATA_HOME"]

    delta_table_path = f"{data_home}/FileData/Delta/partition/partition-table"

    # Create a DeltaTable instance
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Describe the Delta table
    delta_table_details = delta_table.detail()

    # Show the details
    delta_table_details.show(truncate=False)

    # Stop the Spark session
    spark.stop()

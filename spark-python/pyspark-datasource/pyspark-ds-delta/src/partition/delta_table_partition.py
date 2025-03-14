import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    data_home = os.environ["DATA_HOME"]
    builder = (
        SparkSession.builder.appName("versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.local.dir", f"{data_home}/Processing/Batch/Spark/temp")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Example DataFrame with data
    data = [("John", "New York", 25),
            ("Alice", "San Francisco", 30),
            ("Bob", "Los Angeles", 28),
            ("Eve", "Chicago", 35)]

    columns = ["name", "city", "age"]
    df = spark.createDataFrame(data, columns)

    # Write the DataFrame to a Delta table with partitioning by "city"
    (df.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("city")
     .save(f"{data_home}/FileData/Delta/partition/partition-table"))

    # Read the Delta table
    delta_table = DeltaTable.forPath(spark, f"{data_home}/FileData/Delta/partition/partition-table")

    # Display the schema and metadata, including partitioning information
    print("Delta Table Schema:")
    print(delta_table.toDF().schema)

    print("\nDelta Table Metadata:")
    print(delta_table.detail())

    # Update data in the Delta table
    update_data = [("John", "New York", 26), ("Bob", "Los Angeles", 29)]
    update_df = spark.createDataFrame(update_data, columns)

    delta_table.alias("old_data").merge(
        update_df.alias("new_data"),
        "old_data.name = new_data.name"
    ).whenMatchedUpdateAll().execute()

    # Read the updated Delta table
    updated_df = spark.read.format("delta").load(f"{data_home}/FileData/Delta/partition/partition-table")
    updated_df.show()

    # Vacuum Delta table to remove unnecessary files
    delta_table.vacuum(0)

    # Optionally optimize Delta table to improve performance
    # delta_table.optimize()

    # Closing the Spark session
    spark.stop()

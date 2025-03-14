import os
import sys

import pyspark
from delta import DeltaTable
from delta import configure_spark_with_delta_pip

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    builder = (
        pyspark.sql.SparkSession.builder.appName("schema_merge")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Example DataFrame with initial schema
    initial_data = [("John", 25), ("Alice", 30)]
    columns = ["name", "age"]
    initial_df = spark.createDataFrame(initial_data, columns)

    # Write the initial data to a Delta table
    data_home = os.environ["DATA_HOME"]

    initial_df.write.format("delta").mode("overwrite").save(f"{data_home}/FileData/Delta/schema/schema_merge")

    # Simulate new data with an additional column
    new_data = [("Bob", 28, "Male"), ("Eve", 35, "Female")]
    new_columns = ["name", "age", "gender"]
    new_df = spark.createDataFrame(new_data, new_columns)

    # Append the new data to the existing Delta table
    delta_table = DeltaTable.forPath(spark, f"{data_home}/FileData/Delta/schema/schema_merge")
    delta_table.alias("old_data").merge(
        new_df.alias("new_data"),
        "old_data.name = new_data.name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # Read the merged Delta table
    merged_df = spark.read.format("delta").load(f"{data_home}/FileData/Delta/schema/schema_merge")
    merged_df.show()

    delta_table.history().show()

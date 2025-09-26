import os
import sys
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["DERBY_HOME"]
    builder = (
        pyspark.sql.SparkSession.builder.appName("vaccum")
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
    data_path = (
        Path(os.environ.get("DATA_PATH", "/tmp/delta"))
        / "file_data"
        / "delta"
        / "vaccum"
    )

    # Create a dummy Delta table for demonstration
    data = [
        ("1", "A", 10),
        ("2", "B", 20),
        ("3", "C", 30),
    ]
    columns = ["id", "name", "value"]
    df = spark.createDataFrame(data, columns)
    df.write.format("delta").mode("overwrite").save(f"{data_path}")

    # Perform some updates to create multiple versions
    data_update = [
        ("1", "A_updated", 15),
        ("4", "D", 40),
    ]
    df_update = spark.createDataFrame(data_update, columns)
    df_update.write.format("delta").mode("append").save(f"{data_path}")

    # Check the history of the table
    print("Delta table history before vacuum:")
    spark.sql(f"DESCRIBE HISTORY delta.`{data_path}`").show()

    # Vacuum the Delta table, retaining data for
    # the last 0 hours (this will remove all old versions)
    spark.sql(f"VACUUM delta.`{data_path}` RETAIN 168 HOURS")

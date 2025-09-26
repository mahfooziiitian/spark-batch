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

    time_travel_data = Path(os.environ["DATA_HOME"]) / "data" / "delta" / "time_travel"
    time_travel_data.mkdir(parents=True, exist_ok=True)
    table_path = str(time_travel_data / "events")

    # Create a Delta table

    yesterday = spark.sql(
        "SELECT CAST(date_sub(current_date(), 1) AS STRING)"
    ).collect()[0][0]

    df = (
        spark.read.format("delta")
        .option("timestampAsOf", yesterday)
        .load(f"{table_path}")
    )

    df.createOrReplaceTempView("my_table_yesterday")
    spark.sql(f"""
    MERGE INTO delta.`{table_path}` target
      USING my_table_yesterday source
      ON source.userId = target.userId
      WHEN MATCHED THEN UPDATE SET *
    """)

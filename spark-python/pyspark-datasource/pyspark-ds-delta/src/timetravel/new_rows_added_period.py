import os
import sys

import pyspark
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

    last_week = spark.sql("SELECT CAST(date_sub(current_date(), 7) AS STRING)").collect()[0][0]
    df = spark.read.format("delta").option("timestampAsOf", last_week).load("/tmp/delta/events")
    last_week_count = df.select("userId").distinct().count()
    count = spark.read.format("delta").load("/tmp/delta/events").select("userId").distinct().count()
    new_customers_count = count - last_week_count

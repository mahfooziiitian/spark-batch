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

    yesterday = spark.sql("SELECT CAST(date_sub(current_date(), 1) AS STRING)").collect()[0][0]
    df = spark.read.format("delta").option("timestampAsOf", yesterday).load("/tmp/delta/events")
    df.createOrReplaceTempView("my_table_yesterday")
    spark.sql('''
    MERGE INTO delta.`/tmp/delta/events` target
      USING my_table_yesterday source
      ON source.userId = target.userId
      WHEN MATCHED THEN UPDATE SET *
    ''')

import os

import pyspark
from delta import configure_spark_with_delta_pip, DeltaTable

if __name__ == '__main__':
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    builder = (
        pyspark.sql.SparkSession.builder.appName("versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
        .config("spark.databricks.delta.allowArbitraryProperties.enabled","true")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    tableName = "compaction.file_compaction"
    spark.sql(f"DESCRIBE EXTENDED  {tableName}").show(truncate=False)
    spark.sql(f"OPTIMIZE {tableName}")

    spark.sql(f"DESCRIBE EXTENDED  {tableName}").show(truncate=False)

    # Replace the original table with the optimized one
    spark.sql(f"ALTER TABLE {tableName} SET TBLPROPERTIES ('delta.optimize.optimizeWrite' = 'true', "
              f"'delta.autoOptimize.autoCompact'='true')")

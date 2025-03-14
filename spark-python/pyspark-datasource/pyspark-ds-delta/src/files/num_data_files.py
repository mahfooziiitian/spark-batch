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
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    tableName = "compaction.file_compaction"
    # Get the number of data files using DeltaTable API
    delta_table = DeltaTable.forName(spark, tableName)
    num_data_files = delta_table.detail()

    print(f"Number of data files in {tableName}: {num_data_files.count()}")


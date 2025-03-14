import os
import sys

import pyspark
from delta import configure_spark_with_delta_pip
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"
os.environ["PYSPARK_PYTHON"] = sys.executable

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

    spark.sql("Create database if not exists versions")
    tableName = "versions.delta_version"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tableName} (
          id INT,
          firstName STRING,
          middleName STRING,
          lastName STRING,
          gender STRING,
          birthDate TIMESTAMP,
          ssn STRING,
          salary INT
        ) USING DELTA
    """)
    df = spark.read.format("delta").option("versionAsOf", 0).table(tableName)

    df.show(truncate=False)

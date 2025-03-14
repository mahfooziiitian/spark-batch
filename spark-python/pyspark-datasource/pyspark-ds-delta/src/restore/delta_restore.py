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
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    table_name = "versions.delta_version"
    last_successful_version = spark.sql(f"SELECT max(version) -1 as previousVersion  FROM (DESCRIBE HISTORY {table_name} )").collect()[0][0]
    print(last_successful_version)

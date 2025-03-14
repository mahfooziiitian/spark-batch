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

    DeltaTable.create(spark) \
        .tableName("default.delta_table_builder_partition_p") \
        .addColumn("id", "INT") \
        .addColumn("firstName", "STRING") \
        .addColumn("middleName", "STRING") \
        .addColumn("lastName", "STRING", comment="surname") \
        .addColumn("gender", "STRING") \
        .addColumn("birthDate", "TIMESTAMP") \
        .addColumn("ssn", "STRING") \
        .addColumn("salary", "INT") \
        .partitionedBy("gender") \
        .execute()


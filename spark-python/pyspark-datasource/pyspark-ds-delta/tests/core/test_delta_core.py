import os
import sys

import pytest
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"
os.environ["PYSPARK_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    data_home = os.environ["DATA_HOME"]
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("delta_core")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.local.dir", f"{data_home}/Processing/Batch/Spark/temp")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


def test_create_delta_table(spark):
    (DeltaTable.createIfNotExists(spark)
        .tableName("default.people10m")
        .addColumn("id", "INT")
        .addColumn("firstName", "STRING")
        .addColumn("middleName", "STRING")
        .addColumn("lastName", "STRING", comment="surname")
        .addColumn("gender", "STRING")
        .addColumn("birthDate", "TIMESTAMP")
        .addColumn("ssn", "STRING")
        .addColumn("salary", "INT")
        .execute())

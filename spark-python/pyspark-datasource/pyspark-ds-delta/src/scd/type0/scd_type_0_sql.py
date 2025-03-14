"""
scd type 0
Ignore any changes and audit the changes.

"""
import json
import os

import requests
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def get_sample(spark: SparkSession, url: str, sample_view: str):
    # Get Url
    resp = requests.get(url)
    json_data = json.loads(resp.text)
    data = spark.sparkContext.parallelize(json_data)

    # Set Schema
    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address_street", StringType(), True),
            StructField("address_city", StringType(), True),
            StructField("address_country", StringType(), True),
            StructField("email", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    # Convert Date Columns
    df = df.withColumn("start_date", col("start_date").cast(DateType()))
    df = df.withColumn("end_date", col("end_date").cast(DateType()))

    # Rename Column (for later SCD's)
    df = df.withColumnRenamed("id", "employee_id")

    # Additional column for SCD Type 3
    df = df.withColumn("previous_country", lit(None).cast(StringType()))

    df.createOrReplaceTempView(sample_view)

    return


def main():
    url = "https://raw.githubusercontent.com/cwilliams87/Blog/main/07-2021/sampleEmployees"
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]

    builder = (
        SparkSession.builder.appName("versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    sample_view = "employee_sample"
    get_sample(spark, url, sample_view)

    table_name = "scd.employees"

    spark.sql("CREATE DATABASE IF NOT EXISTS scd")

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    spark.sql(f"""
            CREATE TABLE {table_name} 
            USING delta 
            AS SELECT * FROM {sample_view}
        """)


if __name__ == "__main__":
    main()

import os

import pyspark
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql.types import *

from scd.type1.scd_type_1 import init_scd_type_1

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
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    src_table_name = "scd.employees"
    table_name = "scd.scd_type_1"
    stage_table_name = "scd_1_temp"

    init_scd_type_1(spark, src_table_name, table_name)
    spark.sql(f"SELECT * FROM {table_name} WHERE employee_id = 2").show(truncate=False)

    # Create dataset
    dataForDF = [
        (
            2,
            "Stu",
            "Sand",
            "Male",
            "83570 Fairview Way",
            "Chicago",
            "United States",
            "ssand1@imdb.com",
            "Solicitor",
        )
    ]

    # Create Schema structure
    schema = StructType(
        [
            StructField("employee_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address_street", StringType(), True),
            StructField("address_city", StringType(), True),
            StructField("address_country", StringType(), True),
            StructField("email", StringType(), True),
            StructField("job_title", StringType(), True),
        ]
    )
    # Create as Dataframe
    scd1Temp = spark.createDataFrame(dataForDF, schema)

    # Preview dataset
    scd1Temp.show(truncate=False)

    scd1Temp.createOrReplaceTempView(stage_table_name)

    spark.sql(f"""
        MERGE INTO {table_name} as src
        USING {stage_table_name} as stg
            ON src.employee_id = stg.employee_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    spark.sql(f"SELECT * FROM {table_name} WHERE employee_id = 2").show(truncate=False)

    spark.sql(f"DESCRIBE HISTORY {table_name}").show(truncate=False)

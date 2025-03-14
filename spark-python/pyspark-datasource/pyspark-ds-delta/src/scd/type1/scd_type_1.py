"""
SCD Type 1 - Overwrite

In SCD Type 1, when an update occurs, the existing record is overwritten with the new values,
and the previous data is lost.
This strategy doesn't maintain any history of changes.
It overwrites the changes.
"""
import os

from delta import DeltaTable, configure_spark_with_delta_pip
import pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession


def init_scd_type_1(spark: SparkSession, source_table: str, stage_table: str):
    spark.sql(f"DROP TABLE IF EXISTS {stage_table}")
    spark.sql(
        f"""
        CREATE TABLE {stage_table}
            USING delta 
        AS
        SELECT 
            employee_id, 
            first_name, 
            last_name, 
            gender, 
            address_street, 
            address_city, 
            address_country, 
            email, 
            job_title 
        FROM 
            {source_table}
    """
    )


if __name__ == "__main__":
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

    # Set default database
    spark.catalog.setCurrentDatabase("scd")

    # Create dataframe from HIVE db scd
    scdType1DF = spark.table(table_name)

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
            "mahfooz@imdb.com",
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

    # Convert table to Delta
    deltaTable = DeltaTable.forName(spark, table_name)

    # Merge Delta table with new dataset
    (
        deltaTable.alias("original1")
        # Merge using the following conditions
        .merge(
            scd1Temp.alias("updates1"), "original1.employee_id = updates1.employee_id"
        )
        # When matched UPDATE ALL values
        .whenMatchedUpdateAll()
        # When not matched INSERT ALL rows
        .whenNotMatchedInsertAll()
        # Execute
        .execute()
    )
    deltaTable.toDF().orderBy("employee_id").where(
        "first_name = 'Stu' AND last_name = 'Sand'"
    ).show(truncate=False)
    deltaTable.history().show(truncate=False)

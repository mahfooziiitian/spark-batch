"""

History will be added as a new column.

"""
import os

import pyspark
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def scd_type3_init(spark: SparkSession, src_table: str, staging_table: str):
    spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
    spark.sql(
        f"""
        CREATE TABLE scd.scdType3 USING delta AS
            SELECT 
                employee_id, 
                first_name, 
                last_name, 
                gender, 
                address_country 
            FROM {src_table}
            ORDER BY employee_id
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
    # tables
    src_table = "scd.employees"
    staging_table = "scd.scdType3"

    # Set default database
    spark.catalog.setCurrentDatabase("scd")
    scd_type3_init(spark, src_table, staging_table)

    # Create dataframe from HIVE db scd
    scdType3DF = spark.table("scd.scdType3")

    # Display dataframe
    scdType3DF.orderBy("employee_id").show(truncate=False)

    # Create dataset
    dataForDF = [
        (9, "Maximo", "Moxon", "Male", "Canada"),
        (10, "Augueste", "Dimeloe", "Female", "France"),
        (11, "Austina", "Wimbury", "Male", "Germany"),
        (501, "Steven", "Smithson", "Male", "France"),
    ]

    # Create Schema structure
    schema = StructType(
        [
            StructField("employee_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address_country", StringType(), True),
        ]
    )

    # Create as Dataframe
    scd3Temp = spark.createDataFrame(dataForDF, schema)

    # Preview dataset
    scd3Temp.show(truncate=False)

    # Set autoMerge to True
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    changingColumn = "address_country"

    # Create ChangeRows table (union of rows to amend and new rows to insert)
    changeRowsDF = spark.sql(
        f"""
      SELECT scdType3New.*, scdType3.{changingColumn} AS previous_{changingColumn} FROM scdType3New
      INNER JOIN scdType3
      ON scdType3.employee_id = scdType3New.employee_id
      AND scdType3.{changingColumn} <> scdType3New.{changingColumn}
      UNION
      SELECT scdType3New.*, null AS previous_{changingColumn} FROM scdType3New
      LEFT JOIN scdType3
      ON scdType3.employee_id = scdType3New.employee_id
      WHERE scdType3.employee_id IS NULL
    """
    )

    changeRowsDF.show(truncate=False)

    # COMMAND ----------

    # Convert table to Delta
    deltaTable = DeltaTable.forName(spark, "scdType3")

    # Merge Delta table with new dataset
    (
        deltaTable.alias("original3")
        # Merge using the following conditions
        .merge(
            changeRowsDF.alias("updates3"),
            "original3.employee_id = updates3.employee_id",
        )
        # When matched UPDATE these values
        .whenMatchedUpdateAll()
        # When not matched INSERT ALL rows
        .whenNotMatchedInsertAll()
        # Execute
        .execute()
    )

    # Check table for changed rows
    spark.sql("SELECT * FROM scdType3 WHERE employee_id IN (9,10,11,501)").show(
        truncate=False
    )

    # Check Delta history
    spark.sql("DESCRIBE HISTORY scdType3").show(truncate=False)

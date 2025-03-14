"""SCD TYPE 2 - ADD A NEW ROW (WITH ACTIVE ROW INDICATORS OR DATES) A Type 2 SCD is probably one of the most common
examples to easily preserve history in a dimension table and is commonly used throughout any Data
Warehousing/Modelling architecture. Active rows can be indicated with a boolean flag or a start and end date. In this
example from the table above, all active rows can be displayed simply by returning a query where the end date is
null. In SCD Type 2, when an update occurs, a new record is added with the updated values, while the existing record
remains unchanged. This approach keeps track of historical changes by introducing an additional column, typically a
"start_date" and "end_date," to denote the validity period of each version of the data.

Active records:

SELECT * FROM type2Table WHERE end_date IS NULL

SELECT 
        null AS id, 
        employee_id, 
        first_name, 
        last_name, 
        gender, 
        address_street, 
        address_city, 
        address_country, 
        email, 
        job_title, 
        current_date AS start_date, 
        null AS end_date
    FROM scdType2NEW
UNION ALL
    SELECT
        id, 
        employee_id, 
        first_name, 
        last_name, gender, 
        address_street, 
        address_city, 
        address_country, 
        email, 
        job_title, 
        start_date, 
        end_date
FROM scdType2
WHERE employee_id IN (SELECT employee_id FROM scdType2NEW)


-- Merge scdType2NEW dataset into existing
MERGE INTO scdType2
USING
 
-- Update table with rows to match (new and old referenced as seen in example above)
( SELECT
null AS id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, current_date AS start_date, null AS end_date
FROM scdType2NEW
UNION ALL
SELECT
id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, start_date, end_date
FROM scdType2
WHERE employee_id IN
(SELECT employee_id FROM scdType2NEW)
) scdChangeRows
 
-- based on the following column(s)
ON scdType2.id = scdChangeRows.id
 
-- if there is a match do this…
WHEN MATCHED THEN
UPDATE SET scdType2.end_date = current_date()
 
-- if there is no match insert new row
WHEN NOT MATCHED THEN INSERT *

-- Order nulls in DF to the end and recreate row numbers (as delta does not currently support auto incrementals)
INSERT OVERWRITE scdType2
SELECT ROW_NUMBER() OVER (ORDER BY id NULLS LAST) - 1 AS id, 
employee_id, first_name, last_name, gender, address_street, 
address_city, address_country, email, job_title, start_date, end_date
FROM scdType2

"""
import os

import pyspark
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col, current_date
from pyspark.sql.types import *


def scd_type2_init(spark: SparkSession, src_table: str, staging_table: str):
    spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
    spark.sql(
        f"""CREATE TABLE {staging_table} USING delta AS
        SELECT (employee_id - 1) AS id, 
        employee_id, 
        first_name, 
        last_name,
        gender,
        address_street, 
        address_city, 
        address_country, 
        email, job_title, 
        start_date, 
        end_date 
        FROM {src_table}
        ORDER BY employee_id"""
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

    # source table
    src_table = "scd.employees"
    staging_table = "scd.scdType2"

    scd_type2_init(spark, src_table, staging_table)

    # Set default database
    spark.catalog.setCurrentDatabase("scd")

    # Create dataframe from HIVE db scd
    scdType2DF = spark.table(staging_table)

    scdType2DF.orderBy("employee_id").show(truncate=False)

    # Check to see which rows are inactive (have an end date)
    scdType2DF.where("end_date <> null").orderBy("employee_id").show(truncate=False)

    # Create dataset
    dataForDF = [
        (
            None,
            6,
            "Fred",
            "Flintoff",
            "Male",
            "3686 Dogwood Road",
            "Phoenix",
            "United States",
            "fflintoft5@unblog.fr",
            "Financial Advisor",
        ),
        (
            None,
            21,
            "Hilary",
            "Thompson",
            "Female",
            "4 Oxford Pass",
            "San Diego",
            "United States",
            "hcasillisk@washington.edu",
            "Senior Sales Associate",
        ),
    ]

    # Create Schema structure
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
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
    scd2Temp = spark.createDataFrame(dataForDF, schema)

    # Preview dataset
    scd2Temp.show(truncate=False)

    # Create list of selected employee_id's
    empList = scd2Temp.select(collect_list(scd2Temp["employee_id"])).collect()[0][0]

    # Select columns in new dataframe to merge
    scdChangeRows = scd2Temp.selectExpr(
        "null AS id",
        "employee_id",
        "first_name",
        "last_name",
        "gender",
        "address_street",
        "address_city",
        "address_country",
        "email",
        "job_title",
        "current_date() AS start_date",
        "null AS end_date",
    )

    # Union join queries to match incoming rows with existing
    scdChangeRows = scdChangeRows.unionByName(
        scdType2DF.where(col("employee_id").isin(empList)), allowMissingColumns=True
    )
    # Preview results
    scdChangeRows.show(truncate=False)

    # Convert table to Delta
    deltaTable = DeltaTable.forName(spark, "scdType2")

    # Merge Delta table with new dataset
    (
        deltaTable.alias("original2")
        # Merge using the following conditions
        .merge(scdChangeRows.alias("updates2"), "original2.id = updates2.id")
        # When matched UPDATE ALL values
        .whenMatchedUpdate(set={"original2.end_date": current_date()})
        # When not matched INSERT ALL rows
        .whenNotMatchedInsertAll()
        # Execute
        .execute()
    )

    scdType2DF.selectExpr(
        "ROW_NUMBER() OVER (ORDER BY id NULLS LAST) - 1 AS id",
        "employee_id",
        "first_name",
        "last_name",
        "gender",
        "address_street",
        "address_city",
        "address_country",
        "email",
        "job_title",
        "start_date",
        "end_date",
    ).write.insertInto(tableName="scdType2", overwrite=True)

    spark.sql("SELECT * FROM scdType2 WHERE employee_id = 6 OR employee_id = 21").show(
        truncate=False
    )

    # Check Delta history
    spark.sql("DESCRIBE HISTORY scdType2").show(truncate=False)

"""
Surrogate Key (Unique identifier)
Effective Date (e.g. ValidFrom)
Expiration Date (e.g. ValidTo)
Current Row Indicator (e.g. isActive)
Step 1: Staging the Dimension Changes
Detect Changes component is a central mechanism for determining the updates and inserts for changed records. It compares an incoming data set to a target data set and determines if the records are Identical, Changed, New, or Deleted by using a list of comparison columns configured within the component
Each record from the new data set is evaluated and assigned an Indicator Field in the output of the Detect Changes component - 'I' for Identical, 'C' for Changed, 'N' for New and 'D' for Deleted.

Step 2: Finalizing the Dimension Changes
"""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

if __name__ == "__main__":
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["DERBY_HOME"]

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
    # source table
    src_dimension_table = "scd.employees"
    dimension_table = "scd.scd_type2_end_date_sql"

    spark.catalog.setCurrentDatabase("scd")

    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {dimension_table} USING delta AS
            SELECT (employee_id - 1) AS id, 
            employee_id, 
            first_name, 
            last_name,
            gender,
            address_street, 
            address_city, 
            address_country, 
            email, 
            job_title, 
            start_date, 
            end_date 
            FROM {src_dimension_table}
            ORDER BY employee_id"""
    )

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
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("employee_id", T.IntegerType(), True),
            T.StructField("first_name", T.StringType(), True),
            T.StructField("last_name", T.StringType(), True),
            T.StructField("gender", T.StringType(), True),
            T.StructField("address_street", T.StringType(), True),
            T.StructField("address_city", T.StringType(), True),
            T.StructField("address_country", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("job_title", T.StringType(), True),
        ]
    )

    # Create as Dataframe
    scd2Temp = spark.createDataFrame(dataForDF, schema)
    scd2Temp.withColumn("start_date", F.current_date()).withColumn(
        "end_date", F.lit(None)
    ).createOrReplaceTempView("scd_type2_staging")

    # Union the change data
    spark.sql(
        f"""
        MERGE INTO {dimension_table} as src
        USING (
                SELECT * FROM scd_type2_staging
                UNION
                SELECT * FROM {dimension_table} where employee_id in (SELECT employee_id FROM scd_type2_staging)
            ) as stg
            ON src.id = stg.id
        WHEN MATCHED THEN UPDATE SET src.end_date = current_date()
        WHEN NOT MATCHED THEN INSERT *
    """
    )

    spark.sql(
        f"""
        INSERT OVERWRITE {dimension_table}
        SELECT ROW_NUMBER() OVER (ORDER BY id NULLS LAST) - 1 AS id, 
            employee_id, 
            first_name, 
            last_name, 
            gender, 
            address_street, 
            address_city, 
            address_country, 
            email, 
            job_title, 
            start_date, 
            end_date
        FROM {dimension_table}
    """
    )

    spark.sql(
        f"SELECT * FROM {dimension_table} WHERE employee_id = 6 OR employee_id = 21"
    ).show(truncate=False)

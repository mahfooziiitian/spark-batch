"""

History will be added as a new row.

"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == '__main__':
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

    # Create the main dimension table (current state)
    dimension_table = "scd.dim_scd_type2"
    spark.sql(f"""
        CREATE TABLE {dimension_table}
        (
          id INT,               -- Surrogate key
          natural_key STRING,   -- Business key
          attribute STRING,     -- Dimension attribute
          start_date DATE,      -- Effective start date
          end_date DATE,        -- Effective end date
          is_current BOOLEAN    -- Flag to indicate the current record
        )
        USING DELTA
    """)

    # Inserting sample data into table
    spark.sql(f"""
        INSERT INTO {dimension_table} VALUES
        (1, 'key_1', 'Attribute_1', '2023-01-01', '9999-12-31', true),
        (2, 'key_2', 'Attribute_2', '2023-01-01', '9999-12-31', true),
        (3, 'key_3', 'Attribute_3', '2023-01-01', '9999-12-31', true)
    """)

    # Creating staging table
    staging_dimension_table = "scd.dim_scd_type2_stage"
    spark.sql(f"""
        CREATE TABLE {staging_dimension_table}
        (
          id INT,
          natural_key STRING,
          attribute STRING,
          start_date DATE,      -- Effective start date
          end_date DATE,        -- Effective end date
        )
        USING DELTA
    """)

    spark.sql(f"""
        INSERT INTO {staging_dimension_table} 
        VALUES 
        (1, 'key_1', 'Attribute_1_new',current_date(), null),
        (4, 'key_4', 'Attribute_4',current_date(), null)
    """)
    spark.sql(f"SELECT * FROM {dimension_table}").show(truncate=False)

    # Implement SCD Type 2 using Delta Lake MERGE INTO statement
    spark.sql(f"""
        MERGE INTO {dimension_table} AS target
        USING {staging_dimension_table} AS source
            ON target.natural_key = source.natural_key
        WHEN MATCHED THEN
          UPDATE SET target.end_date = source.start_date - 1, target.is_current = false
        WHEN NOT MATCHED THEN
          INSERT (id, natural_key, attribute, start_date, end_date, is_current)
          VALUES (source.id, source.natural_key, source.attribute, current_date(), '9999-12-31', true);
    """)

    spark.sql(f"SELECT * FROM {dimension_table}").show(truncate=False)
    spark.stop()



"""
Slowly Changing Dimensions (SCD), SCD Type 4 is an approach that maintains a separate history table to store historical
changes for dimension records while keeping the main dimension table free from any historical data.
This method uses surrogate keys and start/end date columns to track changes over time.
"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == "__main__":
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

    # Create the main dimension table
    dimension_table = "scd.dimension_type4"
    spark.sql(f"DROP TABLE {dimension_table}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dimension_table}
        (
          dim_id INTEGER,  -- Surrogate key
          natural_key STRING,  -- Business key
          attribute STRING,
          start_date DATE,
          end_date DATE,
          is_current BOOLEAN
        )
        USING DELTA
    """
    )

    # Create the historical changes table
    historical_dimension_table = "scd.dimension_type4_hist"
    spark.sql(f"DROP TABLE {historical_dimension_table}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {historical_dimension_table}
        (
          dim_id INTEGER,  -- Surrogate key
          natural_key STRING,  -- Business key
          attribute STRING,
          start_date DATE,
          end_date DATE,
          is_current BOOLEAN
        )
        USING DELTA
    """
    )

    # data for the initial main dimension table
    spark.sql(
        f"""
        INSERT INTO {dimension_table} VALUES
        (1, 'key_1', 'Attribute_1', '2023-01-01', '9999-12-31', true),
        (2, 'key_2', 'Attribute_2', '2023-01-01', '9999-12-31', true),
        (3, 'key_3', 'Attribute_3', '2023-01-01', '9999-12-31', true);
    """
    )

    #  data for the update to the main dimension table (new changes)
    spark.sql(
        f"""
        INSERT INTO {historical_dimension_table} VALUES
        (1, 'key_1', 'Attribute_1_new', '2023-07-21', '9999-12-31', true),
        (4, 'key_4', 'Attribute_4', '2023-07-21', '9999-12-31', true)
    """
    )

    # Implement SCD Type 4 using Delta Lake MERGE INTO statement
    spark.sql(
        f"""
        MERGE INTO {dimension_table} AS target
        USING (
            SELECT 
                *, ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY start_date DESC) AS rn 
            FROM {dimension_table}
            ) AS source
        ON target.dim_id = source.dim_id
        WHEN MATCHED AND source.rn = 1 THEN
          UPDATE SET target.end_date = source.start_date - 1, target.is_current = false
        WHEN NOT MATCHED THEN
          INSERT (dim_id, natural_key, attribute, start_date, end_date, is_current) 
          VALUES (source.dim_id, source.natural_key, source.attribute, source.start_date, '9999-12-31', true);
    """
    )

    # historical changes

    spark.sql(
        f"""
        MERGE INTO {historical_dimension_table} AS target
        USING (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY start_date DESC) AS rn 
            FROM {dimension_table}
        ) AS source
        ON target.dim_id = source.dim_id AND source.rn = 1
        WHEN NOT MATCHED THEN
          -- Insert new historical changes from the source table into the historical changes table
          INSERT (dim_id, natural_key, attribute, start_date, end_date, is_current) 
          VALUES (source.dim_id, source.natural_key, source.attribute, source.start_date, source.end_date, source.is_current)
    """
    )

    spark.sql(f"SELECT * FROM {dimension_table}").show(truncate=False)
    spark.sql(f"SELECT * FROM {historical_dimension_table}").show(truncate=False)

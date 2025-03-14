"""
main_dimension_table representing the current state of the dimension and staging_dimension_table containing the new updates.
The end_date column is set to '9999-12-31' by default in the main dimension table to indicate the current state of the dimension.
The staging table contains the updated rows, and we use the MERGE statement to apply SCD Type 6 logic.
The MERGE statement checks whether there is a match between the primary key id in the main_dimension_table and the staging_dimension_table
If a match is found and the status has changed, it updates the end_date of the current row to the current date.
If there is no match (new records), they are inserted into the main dimension table with the default '9999-12-31' end_date.
"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == "__main__":
    warehouse_location = os.environ["SPARK_WAREHOUSE"]
    derby_home = os.environ["derby.system.home"]
    builder = (
        SparkSession.builder.appName("scd_typ6_sql")
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
    #  Create the main dimension table (current state)
    main_dimension_table = "scd.main_dimension_table"
    spark.sql(
        f"""
        CREATE TABLE {main_dimension_table}
        USING DELTA
        AS
            SELECT 1 AS id, 'John Doe' AS name, 'Active' AS status, '9999-12-31' AS end_date
        UNION ALL
            SELECT 2 AS id, 'Jane Smith' AS name, 'Active' AS status, '9999-12-31' AS end_date
        UNION ALL
            SELECT 3 AS id, 'Bob Johnson' AS name, 'Active' AS status, '9999-12-31' AS end_date
    """
    )

    # Create a staging table for the update data
    staging_dimension_table = "scd.staging_dimension_table"
    spark.sql(
        f"""
        CREATE TABLE {staging_dimension_table}
        USING DELTA
        AS
            SELECT 1 AS id, 'John Doe' AS name, 'Inactive' AS status
        UNION ALL
            SELECT 2 AS id, 'Jane Smith' AS name, 'Active' AS status
        UNION ALL
            SELECT 4 AS id, 'Alice Brown' AS name, 'Active' AS status
    """
    )

    # Merge the staging data into the main dimension table with SCD Type 6 logic
    spark.sql(
        f"""
        MERGE INTO {main_dimension_table} AS target
        USING {staging_dimension_table} AS source
            ON target.id = source.id
        WHEN MATCHED AND target.status != source.status THEN
          UPDATE SET target.end_date = current_date()
        WHEN NOT MATCHED THEN
          INSERT (id, name, status, end_date) VALUES (source.id, source.name, source.status, '9999-12-31');
    """
    )

    #  View the updated main dimension table
    spark.sql(f"SELECT * FROM {main_dimension_table}").show(truncate=False)

    # View the historical changes table (SCD Type 6)
    spark.sql(f"SELECT * FROM {main_dimension_table} VERSION AS OF 0").show(
        truncate=False
    )

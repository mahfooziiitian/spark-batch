"""
A new dimension will be added
"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def scd_typ4_init(spark: SparkSession, src_table: str, staging_table: str):
    spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
    spark.sql(
        f"""
        CREATE TABLE {staging_table} USING delta AS
        SELECT employee_id, first_name, 
        last_name, gender, address_street, 
        address_city, address_country, email, 
        job_title FROM {src_table}
        ORDER BY employee_id
        LIMIT 5
    """
    )


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

    src_table = "scd.employees"
    destination_table = "scd.employees"

    # Select employees table (ordered by id column)
    spark.sql(f"SELECT * FROM {destination_table} ORDER BY employee_id")

    spark.sql("DROP VIEW IF EXISTS scdType4NEW")

    # Create View to merge

    spark.sql(
        f"""CREATE VIEW scdType4NEW AS SELECT col1 AS employee_id, col2 AS first_name, col3 AS last_name, 
    col4 AS gender, col5 AS address_street, col6 AS address_city, col7 AS address_country, col8 AS email, 
    col9 AS job_title FROM VALUES(1, 'Rae', 'Maith', 'Male', '6877 Roth Hill', 'Sioux City', 'United States', 
    'rmaith0@wisc.org','VP Quality Control'), (2, 'Stuart', 'Sand', 'Male', '83570 Fairview Way', 'Chicago', 
    'United States', 'ssand1@imdb.com', 'Paralegal') """
    )

    # Preview results
    spark.sql("SELECT * FROM scdType4NEW")

    spark.sql(
        f"""
        MERGE INTO {destination_table}
        USING scdType4NEW
            ON scdType4.employee_id = scdType4NEW.employee_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    )

    # Check table for changed rows
    spark.sql(f"SELECT * FROM {destination_table}")

    # Check Delta history
    spark.sql(f"DESCRIBE HISTORY {destination_table}")

    # View current and previous versions
    spark.sql(
        f"""SELECT 0 AS version, *  FROM {destination_table} 
        VERSION AS OF 0
        WHERE employee_id = 1 OR employee_id = 2
        UNION ALL
        SELECT 1 AS version, * FROM {destination_table} VERSION AS OF 1
        WHERE employee_id = 1 OR employee_id = 2"""
    ).show(truncate=False)

    # The table can be restored to a previous version
    spark.sql(f"RESTORE TABLE {destination_table} TO VERSION AS OF 0")

    spark.sql(f"SELECT * FROM {destination_table}").show(truncate=False)

    spark.sql("DROP VIEW IF EXISTS scdType4NEW")

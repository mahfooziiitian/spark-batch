import os

import pyspark
from delta import configure_spark_with_delta_pip

if __name__ == "__main__":
    builder = (
        pyspark.sql.SparkSession.builder.appName("delta_app")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    table_path = (
        "file:///"
        + (os.environ["DATA_HOME"].replace("\\", "/"))
        + "/FileData/Delta/delta-table-sql"
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Drop table
    # spark.sql(f"DROP TABLE delta.`{table_path}`")

    # Write data
    # spark.sql(f"CREATE TABLE delta.`{table_path}` USING DELTA AS SELECT col1 as id FROM VALUES 0,1,2,3,4")

    # Read data
    spark.sql(f"""SELECT * FROM delta.`{table_path}`""").show(truncate=False)

    # Update table data

    # Overwrite
    spark.sql(
        f"""INSERT OVERWRITE delta.`{table_path}` SELECT col1 as id FROM VALUES 5,6,7,8,9"""
    )

    # Conditional update without overwrite
    # Update every even value by adding 100 to it
    spark.sql(
        f"""UPDATE delta.`{table_path}` 
            SET id = id + 100
            WHERE
            id % 2 == 0;
    """
    )

    # Delete very even value
    spark.sql(f"""DELETE FROM delta.`{table_path}` WHERE id % 2 == 0""")

    # Upsert(merge) new data

    spark.sql(
        f"""CREATE TEMP VIEW newData
        AS
            SELECT
                col1 AS id
            FROM
                VALUES 1, 3, 5, 7, 9, 11, 13, 15, 17, 19"""
    )

    spark.sql(
        f"""
        MERGE INTO delta.`{table_path}` AS oldData
        USING newData
        ON
            oldData.id = newData.id
        WHEN MATCHED
            THEN
                UPDATE SET id = newData.id
        WHEN NOT MATCHED
            THEN
                INSERT(id) VALUES(newData.id)"""
    )

    spark.sql(f"""SELECT * FROM delta.`{table_path}`""").show(truncate=False)

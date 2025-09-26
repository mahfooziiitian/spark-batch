"""
ACID transactions on Spark: Serializable isolation levels ensure that readers never see inconsistent data.
"""

import os
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if __name__ == "__main__":
    warehouse_location = Path(os.environ["SPARK_WAREHOUSE"])
    derby_home = Path(os.environ["DERBY_HOME"])
    data_home = Path(os.environ["DATA_HOME"])
    spark_local_dir = data_home / "processing" / "batch" / "spark" / "temp"
    data_dir = data_home / "file_data" / "parquet" / "reported_crimes"
    data_dir.mkdir(parents=True, exist_ok=True)
    spark_local_dir.mkdir(parents=True, exist_ok=True)
    derby_home.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName("versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.local.dir", spark_local_dir.as_posix())
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{derby_home}'")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    deltaTable = DeltaTable.convertToDelta(
        spark,
        f"parquet.`{data_dir.as_posix()}`",
    )

    deltaTable.history().show(truncate=False)
    # Update a row in the Delta Lake table
    deltaTable.update(
        condition="id = '12345'",
        set={"primary_type": "'THEFT'", "description": "'RETAIL THEFT'"},
    )
    print("After update:")
    deltaTable.toDF().show(truncate=False)

    # Delete a row from the Delta Lake table
    deltaTable.delete(condition="id = '12345'")
    print("After delete:")
    deltaTable.toDF().show(truncate=False)

    # Insert a new row into the Delta Lake table
    new_data = spark.createDataFrame(
        [
            (
                "12345",
                "HZ123456",
                "01/01/2023 12:00:00 AM",
                "01XX W DIVISION ST",
                "0810",
                "THEFT",
                "RETAIL THEFT",
                "GROCERY FOOD STORE",
                True,
                False,
                "1234",
                "012",
                "01",
                "01",
                "08",
                "1164000",
                "1907000",
                "2023",
                "03/01/2023 03:00:00 PM",
                "41.9037",
                "-87.6798",
                "(41.903719999, -87.679820000)",
            )
        ],
        [
            "id",
            "case_number",
            "date",
            "block",
            "iucr",
            "primary_type",
            "description",
            "location_description",
            "arrest",
            "domestic",
            "beat",
            "district",
            "ward",
            "community_area",
            "fbi_code",
            "x_coordinate",
            "y_coordinate",
            "year",
            "updated_on",
            "latitude",
            "longitude",
            "location",
        ],
    )
    deltaTable.alias("old_data").merge(
        new_data.alias("new_data"), "old_data.id = new_data.id"
    ).whenMatchedUpdate

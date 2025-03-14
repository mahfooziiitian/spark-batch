import os

import pyspark
from delta import configure_spark_with_delta_pip

if __name__ == "__main__":
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_sql_versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    table_path = (
        "file:///"
        + (os.environ["DATA_HOME"].replace("\\", "/"))
        + "/FileData/Delta/delta-sql-version"
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Write data
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS delta.`{table_path}` USING DELTA AS SELECT col1 as id FROM VALUES 0,1,2,3,4"
    )

    # Read data

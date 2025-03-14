import os
from delta import configure_spark_with_delta_pip

from delta.tables import *
import pyspark

if __name__ == "__main__":
    builder = (
        pyspark.sql.SparkSession.builder.appName("versioning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    file_path = (
        "file:///"
        + (os.environ["DATA_HOME"].replace("\\", "/"))
        + "/FileData/Delta/versioning/upsert_table"
    )

    # Create a table
    data = spark.range(0, 5)
    data.write.format("delta").save(file_path)

    # Read data
    df = spark.read.format("delta").load(file_path)
    df.show()

    # Update table data
    data = spark.range(5, 10)
    data.write.format("delta").mode("overwrite").save(file_path)

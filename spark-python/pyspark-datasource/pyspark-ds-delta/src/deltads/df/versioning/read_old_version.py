import os

import pyspark
from delta import configure_spark_with_delta_pip

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
        + "/FileData/Delta/versioning/version_table"
    )

    df = spark.range(0, 20)

    df.write.mode("append").format("delta").save(file_path)

    df = spark.read.format("delta").option("versionAsOf", 0).load(file_path)
    df.show()

    df = spark.range(40, 50)
    df.write.mode("append").format("delta").save(file_path)
    df = spark.read.format("delta").option("versionAsOf", 1).load(file_path)
    df.show()

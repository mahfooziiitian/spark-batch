import pyspark
from delta import configure_spark_with_delta_pip

if __name__ == "__main__":
    builder = (
        pyspark.sql.SparkSession.builder.appName("delta_app")
        # .config('spark.jars.packages', 'io.delta:delta-core_2.13:2.3.0')
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    #  Tables created with a specified LOCATION are considered unmanaged by the metastore.
    spark.sql(
        """
        CREATE TABLE default.people10m
        USING DELTA
        LOCATION '/tmp/delta/people10m'"""
    )

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
    streamingDf = spark.readStream.format("rate").load()
    stream = (
        streamingDf.selectExpr("value as id")
        .writeStream.format("delta")
        .option("checkpointLocation", "/tmp/checkpoint")
        .start("/tmp/delta-table")
    )

    stream2 = (
        spark.readStream.format("delta")
        .load("/tmp/delta-table")
        .writeStream.format("console")
        .start()
    )

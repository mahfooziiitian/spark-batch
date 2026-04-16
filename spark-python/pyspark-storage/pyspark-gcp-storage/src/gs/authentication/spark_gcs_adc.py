import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    gcs_connector_jar = os.environ.get(
        "GCS_CONNECTOR_JAR",
        "/opt/spark/jars/gcs-connector-hadoop3-latest.jar")

    spark = (SparkSession.builder
             .appName("gcs-read-adc")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars", gcs_connector_jar)
             .config("spark.hadoop.fs.gs.impl",
                     "com.google.cloud.hadoop.fs.gcs"
                     ".GoogleHadoopFileSystem")
             .config("spark.hadoop.google.cloud.auth"
                     ".service.account.enable", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "gs://my-bucket/input.csv")
    df = spark.read.option("header", True).csv(input_path)
    df.show(truncate=False)

    spark.stop()

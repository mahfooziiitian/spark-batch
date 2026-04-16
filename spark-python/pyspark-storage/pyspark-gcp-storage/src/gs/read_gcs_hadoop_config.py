import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    gcs_connector_jar = os.environ.get(
        "GCS_CONNECTOR_JAR",
        "/opt/spark/jars/gcs-connector-hadoop3-latest.jar")

    spark = (SparkSession.builder
             .appName("gcs-read-hadoop-config")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars", gcs_connector_jar)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.enable", "true")
    sc._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""))

    input_path = os.environ.get("INPUT_PATH", "gs://my-bucket/input.csv")
    output_path = os.environ.get("OUTPUT_PATH", "gs://my-bucket/output")

    df = (spark.read
          .option("inferSchema", True)
          .option("header", True)
          .csv(input_path))
    df.show(truncate=False)

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

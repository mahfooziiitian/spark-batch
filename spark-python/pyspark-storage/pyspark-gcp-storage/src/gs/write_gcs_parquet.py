import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':
    gcs_connector_jar = os.environ.get(
        "GCS_CONNECTOR_JAR",
        "/opt/spark/jars/gcs-connector-hadoop3-latest.jar")
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

    spark = (SparkSession.builder
             .appName("gcs-write-parquet")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars", gcs_connector_jar)
             .config("spark.hadoop.fs.gs.impl",
                     "com.google.cloud.hadoop.fs.gcs"
                     ".GoogleHadoopFileSystem")
             .config("spark.hadoop.google.cloud.auth"
                     ".service.account.enable", "true")
             .config("spark.hadoop.google.cloud.auth"
                     ".service.account.json.keyfile", keyfile)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "gs://my-bucket/input/sample.csv")
    output_path = os.environ.get("OUTPUT_PATH", "gs://my-bucket/output/department_salaries")

    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(input_path))
    df.show(truncate=False)

    dept_avg = (df
                .groupBy("department")
                .agg(F.avg("salary").alias("avg_salary")))
    dept_avg.show(truncate=False)

    dept_avg.write.mode("overwrite").parquet(output_path)
    print(f"Parquet written to {output_path}")

    spark.stop()

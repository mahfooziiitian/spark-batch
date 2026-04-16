import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "/usr/lib/jvm/java-8-openjdk-amd64")
    hadoop_aws = "3.3.4"
    master = os.environ.get("SPARK_MASTER", "local[*]")

    spark = (SparkSession
             .builder
             .appName("write_s3_parquet")
             .master(master)
             .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
             .config("spark.hadoop.fs.s3a.access.key", "test")
             .config("spark.hadoop.fs.s3a.secret.key", "test")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "s3a://spark-demo/input/sample.csv")
    output_path = os.environ.get("OUTPUT_PATH", "s3a://spark-demo/output/dept_salary")

    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(input_path))

    print("Input data:")
    df.show(truncate=False)

    result = (df
              .groupBy("department")
              .agg(F.avg("salary").alias("avg_salary")))

    print("Aggregated result:")
    result.show(truncate=False)

    (result.write
     .mode("overwrite")
     .parquet(output_path))

    print(f"Parquet written to {output_path}")

    spark.stop()

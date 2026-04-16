import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    hadoop_aws = "3.3.4"
    master = os.environ.get("SPARK_MASTER", "local[*]")
    input_path = os.environ.get("INPUT_PATH", "s3a://spark-demo/input/sample.csv")

    spark = (SparkSession
             .builder
             .appName("read_s3_spark_config")
             .master(master)
             .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
             .config("spark.hadoop.fs.s3a.access.key", "test")
             .config("spark.hadoop.fs.s3a.secret.key", "test")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    df.show(truncate=False)

    spark.stop()

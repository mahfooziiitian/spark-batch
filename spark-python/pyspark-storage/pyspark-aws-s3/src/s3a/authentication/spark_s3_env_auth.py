import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    hadoop_aws = "3.3.4"
    spark = (SparkSession.builder
             .appName("aws-s3-env-auth")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "s3a://my-bucket/input.csv")
    df = spark.read.option("header", True).csv(input_path)
    df.show(truncate=False)

    spark.stop()

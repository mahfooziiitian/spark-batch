import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    hadoop_aws = "3.3.4"
    spark = (SparkSession.builder
             .appName("aws-s3-read-spark-config")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .config("spark.hadoop.fs.s3a.access.key",
                     os.environ.get("AWS_ACCESS_KEY_ID", ""))
             .config("spark.hadoop.fs.s3a.secret.key",
                     os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "s3a://my-bucket/input.csv")
    output_path = os.environ.get("OUTPUT_PATH", "s3a://my-bucket/output")

    df = spark.read.option("header", True).csv(input_path)
    df.show(truncate=False)

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

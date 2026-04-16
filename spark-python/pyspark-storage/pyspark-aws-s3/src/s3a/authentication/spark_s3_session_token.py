import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    hadoop_aws = "3.3.4"
    spark = (SparkSession.builder
             .appName("aws-s3-session-token")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
             .config("spark.hadoop.fs.s3a.access.key",
                     os.environ.get("AWS_ACCESS_KEY_ID", ""))
             .config("spark.hadoop.fs.s3a.secret.key",
                     os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
             .config("spark.hadoop.fs.s3a.session.token",
                     os.environ.get("AWS_SESSION_TOKEN", ""))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "s3a://my-bucket/input.csv")
    df = spark.read.option("header", True).csv(input_path)
    df.show(truncate=False)

    spark.stop()

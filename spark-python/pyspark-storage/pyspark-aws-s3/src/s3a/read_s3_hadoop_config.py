import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
    hadoop_aws = "3.3.4"
    spark = (SparkSession.builder
             .appName("aws-s3-read-hadoop-config")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    input_path = os.environ.get("INPUT_PATH", "s3a://my-bucket/input.csv")
    output_path = os.environ.get("OUTPUT_PATH", "s3a://my-bucket/output")

    df = (spark.read
          .option("inferSchema", True)
          .option("header", True)
          .csv(input_path))
    df.show(truncate=False)

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

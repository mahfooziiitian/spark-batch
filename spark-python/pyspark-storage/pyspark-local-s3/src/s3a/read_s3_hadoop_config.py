import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    hadoop_aws = "3.3.4"
    master = os.environ.get("SPARK_MASTER", "local[*]")
    input_path = os.environ.get("INPUT_PATH", "s3a://spark-demo/input/sample.csv")

    spark = (SparkSession.builder
             .appName("read_s3_hadoop_config")
             .master(master)
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:4566")
    hadoop_conf.set("fs.s3a.access.key", "test")
    hadoop_conf.set("fs.s3a.secret.key", "test")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(input_path))

    df.show(truncate=False)

    spark.stop()

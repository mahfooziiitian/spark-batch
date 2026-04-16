import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "/usr/lib/jvm/java-8-openjdk-amd64")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    hadoop_aws = "3.3.4"
    spark = (SparkSession.builder
             .appName("minio-read-spark-config")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
             .config("spark.hadoop.fs.s3a.access.key", access_key)
             .config("spark.hadoop.fs.s3a.secret.key", secret_key)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
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

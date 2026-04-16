import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    hadoop_aws = "3.3.4"
    spark = (SparkSession.builder
             .appName("aws-s3-assumed-role")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
             .config("spark.hadoop.fs.s3a.assumed.role.arn",
                     os.environ.get("AWS_ROLE_ARN",
                                    "arn:aws:iam::123456789012:role/my-role"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.environ.get("INPUT_PATH", "s3a://my-bucket/input.csv")
    df = spark.read.option("header", True).csv(input_path)
    df.show(truncate=False)

    spark.stop()

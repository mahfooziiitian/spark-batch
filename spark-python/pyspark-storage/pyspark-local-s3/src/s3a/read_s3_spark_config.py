from pyspark.sql import SparkSession

if __name__ == '__main__':
    hadoop_aws = "3.2.2"
    spark = (SparkSession
             .builder
             .appName("spark_localstack_demo")
             .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
             .config("spark.hadoop.fs.s3a.access.key", "test")
             .config("spark.hadoop.fs.s3a.secret.key", "test")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a'
                                                                     '.SimpleAWSCredentialsProvider')
             .getOrCreate())

    # On EMR you can use `s3` instead of `s3a`
    spark.read.csv('s3a://otdm-magna-dev-landing/requests.csv').show()

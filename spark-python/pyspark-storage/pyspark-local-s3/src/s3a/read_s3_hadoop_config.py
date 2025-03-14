from pyspark.sql import SparkSession

if __name__ == '__main__':
    hadoop_aws = "3.2.2"
    spark = (SparkSession.builder
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")
             .getOrCreate())
    sc = spark.sparkContext

    accessKeyId = 'test'
    secretAccessKey = 'test'
    sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', accessKeyId)
    sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', secretAccessKey)
    sc._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
    sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://localhost:4566')
    sc._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a'
                                                                         '.SimpleAWSCredentialsProvider')

    df = (spark.read
          .option("inferSchema", True)
          .option("header", True)
          .csv("s3a://otdm-magna-dev-landing/requests.csv"))

    df.show(truncate=False)

    (df.write.format("csv")
     .option("header", True)
     .save("s3a://otdm-magna-dev-landing/request", mode="overwrite"))

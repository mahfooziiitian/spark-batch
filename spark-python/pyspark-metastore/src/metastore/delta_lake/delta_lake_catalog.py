from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("DeltaCatalog")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.warehouse.dir", "s3://my-bucket/delta")
    .config("spark.databricks.delta.catalog.enabled", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .getOrCreate()
)

# Optional: Set log level for easier debugging
spark.sparkContext.setLogLevel("WARN")

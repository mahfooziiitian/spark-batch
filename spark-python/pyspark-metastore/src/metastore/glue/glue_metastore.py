import os

from pyspark.sql import SparkSession

# Get warehouse dir from environment or use default
warehouse_dir = os.getenv("SPARK_WAREHOUSE", "s3://my-bucket/warehouse")

spark = (
    SparkSession.builder.appName("GlueCatalog")
    # Hive Metastore via AWS Glue
    .config("spark.sql.catalogImplementation", "hive")
    .config(
        "spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    .config("spark.sql.warehouse.dir", warehouse_dir)
    # Iceberg Glue Catalog
    .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .enableHiveSupport()
    .getOrCreate()
)

# Optional: Print Spark config for debugging
print("Spark configuration:")
for item in spark.sparkContext.getConf().getAll():
    print(item)

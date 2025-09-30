from pyspark.sql import SparkSession


def create_spark_session(app_name="IcebergCatalog"):
    """
    Create and configure a SparkSession with Iceberg Hive and Hadoop catalogs.
    """
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            # Iceberg Hive Catalog
            .config(
                "spark.sql.catalog.my_iceberg", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.my_iceberg.type", "hive")
            .config("spark.sql.catalog.my_iceberg.uri", "thrift://metastore:9083")
            .config("spark.sql.catalog.my_iceberg.warehouse", "s3://my-bucket/iceberg")
            .config("spark.sql.defaultCatalog", "my_iceberg")
            # Iceberg Hadoop Catalog
            .config(
                "spark.sql.catalog.iceberg_hadoop",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.iceberg_hadoop.type", "hadoop")
            .config(
                "spark.sql.catalog.iceberg_hadoop.warehouse", "s3://my-bucket/iceberg"
            )
            .getOrCreate()
        )
        print("SparkSession created successfully.")
        return spark
    except Exception as e:
        print(f"Error creating SparkSession: {e}")
        raise


# Usage
spark = create_spark_session()

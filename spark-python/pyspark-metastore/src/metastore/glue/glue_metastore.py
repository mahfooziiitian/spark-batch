import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_glue_spark_session() -> SparkSession:
    warehouse_dir = os.environ.get("SPARK_WAREHOUSE", "s3://my-bucket/warehouse")
    aws_region = os.environ.get("AWS_REGION", "us-east-1")

    return (
        SparkSession.builder.appName("glue-catalog-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.hadoop.aws.region", aws_region)
        # Iceberg Glue Catalog
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.glue.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue.warehouse", warehouse_dir)
        .enableHiveSupport()
        .getOrCreate()
    )


def print_glue_config(spark: SparkSession) -> None:
    print("\n=== Glue / AWS Configuration ===")
    conf = spark.sparkContext.getConf()
    keys = [
        "spark.sql.catalogImplementation",
        "spark.hadoop.hive.metastore.client.factory.class",
        "spark.sql.warehouse.dir",
        "spark.hadoop.aws.region",
        "spark.sql.catalog.glue",
        "spark.sql.catalog.glue.catalog-impl",
        "spark.sql.catalog.glue.io-impl",
    ]
    for key in keys:
        print(f"  {key} = {conf.get(key, 'not set')}")


def demonstrate_glue_databases(spark: SparkSession) -> None:
    print("\n=== Glue Databases ===")
    spark.sql("SHOW CATALOGS").show(truncate=False)
    spark.sql("SHOW DATABASES").show(truncate=False)

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS glue_demo_db "
        "COMMENT 'Demo database registered in AWS Glue Data Catalog'"
    )
    spark.sql("USE glue_demo_db")
    spark.sql("SHOW DATABASES").show(truncate=False)


def demonstrate_glue_tables(spark: SparkSession) -> None:
    print("\n=== Glue Tables ===")
    warehouse_dir = os.environ.get("SPARK_WAREHOUSE", "s3://my-bucket/warehouse")
    spark.sql("USE glue_demo_db")

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS web_events (
            event_id STRING,
            user_id STRING,
            action STRING,
            event_ts TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '{warehouse_dir}/glue_demo_db/web_events'
    """)

    spark.sql("""
        INSERT INTO web_events VALUES
        ('e1', 'u100', 'page_view',  TIMESTAMP '2024-07-01 10:00:00'),
        ('e2', 'u101', 'click',      TIMESTAMP '2024-07-01 10:05:00'),
        ('e3', 'u100', 'purchase',   TIMESTAMP '2024-07-01 10:15:00'),
        ('e4', 'u102', 'page_view',  TIMESTAMP '2024-07-01 10:20:00')
    """)

    spark.sql("SHOW TABLES").show(truncate=False)
    spark.sql("SELECT * FROM web_events ORDER BY event_ts").show(truncate=False)

    summary = (
        spark.table("web_events")
        .groupBy("action")
        .agg(
            F.count("*").alias("event_count"),
        )
    )
    summary.show(truncate=False)


def demonstrate_glue_iceberg(spark: SparkSession) -> None:
    print("\n=== Iceberg Tables via Glue Catalog ===")

    spark.sql("CREATE DATABASE IF NOT EXISTS glue.iceberg_demo_db")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue.iceberg_demo_db.orders (
            order_id LONG,
            customer STRING,
            total DOUBLE,
            order_date DATE
        )
        USING iceberg
    """)

    spark.sql("""
        INSERT INTO glue.iceberg_demo_db.orders VALUES
        (1001, 'Alice', 250.00, DATE '2024-07-01'),
        (1002, 'Bob',   125.50, DATE '2024-07-01'),
        (1003, 'Carol', 340.75, DATE '2024-07-02')
    """)

    spark.sql("SHOW TABLES IN glue.iceberg_demo_db").show(truncate=False)
    spark.sql("SELECT * FROM glue.iceberg_demo_db.orders").show(truncate=False)

    # Iceberg metadata queries
    spark.sql("SELECT * FROM glue.iceberg_demo_db.orders.snapshots").show(
        truncate=False
    )


def demonstrate_cross_account_access(spark: SparkSession) -> None:
    # To access a Glue Data Catalog in a different AWS account, set
    # the catalog ID to the target account number. The executing role
    # must have a cross-account IAM policy granting glue:* permissions
    # on the target account's catalog.
    print("\n=== Cross-Account Glue Access (config example) ===")
    target_account_id = os.environ.get("GLUE_CROSS_ACCOUNT_ID", "123456789012")

    print(f"  Target account ID: {target_account_id}")
    print("  Required config:")
    print(f"    spark.hadoop.hive.metastore.glue.catalogid = {target_account_id}")
    print(
        "    spark.hadoop.hive.metastore.client.factory.class = "
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    print(
        "  The executing IAM role needs a policy allowing glue:* on the target account."
    )


def cleanup(spark: SparkSession) -> None:
    print("\n=== Cleanup ===")
    spark.sql("DROP TABLE IF EXISTS glue_demo_db.web_events")
    spark.sql("DROP DATABASE IF EXISTS glue_demo_db CASCADE")
    spark.sql("DROP TABLE IF EXISTS glue.iceberg_demo_db.orders PURGE")
    spark.sql("DROP DATABASE IF EXISTS glue.iceberg_demo_db CASCADE")
    print("Cleanup complete.")


def main() -> None:
    spark = create_glue_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print_glue_config(spark)
        demonstrate_glue_databases(spark)
        demonstrate_glue_tables(spark)
        demonstrate_glue_iceberg(spark)
        demonstrate_cross_account_access(spark)
    finally:
        cleanup(spark)
        spark.stop()


if __name__ == "__main__":
    main()

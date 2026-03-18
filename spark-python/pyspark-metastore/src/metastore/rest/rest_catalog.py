import os

from pyspark.sql import SparkSession

REST_CATALOG_URI = os.environ.get(
    "REST_CATALOG_URI", "https://metastore-api.example.com"
)
REST_CATALOG_WAREHOUSE = os.environ.get(
    "REST_CATALOG_WAREHOUSE", "s3://my-bucket/warehouse"
)
REST_CATALOG_TOKEN = os.environ.get("REST_CATALOG_TOKEN", "bearer-token")
REST_SHUFFLE_PARTITIONS = os.environ.get("REST_SHUFFLE_PARTITIONS", "200")
REST_EXECUTOR_MEMORY = os.environ.get("REST_EXECUTOR_MEMORY", "4g")
REST_DRIVER_MEMORY = os.environ.get("REST_DRIVER_MEMORY", "2g")


def create_spark_session():
    spark = (
        SparkSession.builder.appName("RESTCatalog")
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.type", "rest")
        .config("spark.sql.catalog.rest.uri", REST_CATALOG_URI)
        .config("spark.sql.catalog.rest.warehouse", REST_CATALOG_WAREHOUSE)
        .config("spark.sql.catalog.rest.credential", REST_CATALOG_TOKEN)
        .config("spark.sql.shuffle.partitions", REST_SHUFFLE_PARTITIONS)
        .config("spark.executor.memory", REST_EXECUTOR_MEMORY)
        .config("spark.driver.memory", REST_DRIVER_MEMORY)
        .enableHiveSupport()
        .getOrCreate()
    )
    print("SparkSession created successfully.")
    return spark


def demonstrate_rest_catalog_browse(spark):
    print("=== REST Catalog Browse ===")

    print("\n-- Namespaces in REST catalog --")
    spark.sql("SHOW NAMESPACES IN rest").show(truncate=False)

    print("\n-- Tables in rest.default --")
    spark.sql("SHOW TABLES IN rest.default").show(truncate=False)

    tables_df = spark.sql("SHOW TABLES IN rest.default")
    first = tables_df.first()
    if first:
        table_name = first["tableName"]
        print(f"\n-- Describe rest.default.{table_name} --")
        spark.sql(f"DESCRIBE TABLE rest.default.{table_name}").show(truncate=False)

        print("\n-- Extended table info --")
        spark.sql(f"DESCRIBE EXTENDED rest.default.{table_name}").show(truncate=False)
    else:
        print("No tables found to describe.")


def demonstrate_rest_catalog_operations(spark):
    print("=== REST Catalog Operations (Iceberg) ===")

    print("\n-- Create namespace --")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.demo")

    print("\n-- Create Iceberg table --")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS rest.demo.events (
            event_id    INT,
            event_type  STRING,
            user_id     INT,
            amount      DOUBLE,
            event_ts    TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_ts))
    """)

    print("\n-- Insert data --")
    spark.sql("""
        INSERT INTO rest.demo.events VALUES
            (1, 'purchase', 101, 29.99,  TIMESTAMP '2024-01-15 10:30:00'),
            (2, 'refund',   102, 15.50,  TIMESTAMP '2024-01-15 11:00:00'),
            (3, 'purchase', 103, 99.00,  TIMESTAMP '2024-01-16 09:15:00'),
            (4, 'purchase', 101, 45.00,  TIMESTAMP '2024-01-16 14:20:00'),
            (5, 'refund',   104, 12.75,  TIMESTAMP '2024-01-17 08:45:00')
    """)

    print("\n-- Query data --")
    spark.sql("""
        SELECT event_type, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total
        FROM rest.demo.events
        GROUP BY event_type
    """).show()

    print("\n-- Iceberg time travel: snapshot history --")
    spark.sql("SELECT * FROM rest.demo.events.snapshots").show(truncate=False)

    print("\n-- Insert more data for a new snapshot --")
    spark.sql("""
        INSERT INTO rest.demo.events VALUES
            (6, 'purchase', 105, 200.00, TIMESTAMP '2024-01-18 12:00:00')
    """)

    print("\n-- Query previous snapshot (first snapshot) --")
    snapshots = spark.sql(
        "SELECT snapshot_id FROM rest.demo.events.snapshots ORDER BY committed_at"
    ).collect()
    if len(snapshots) >= 1:
        first_snapshot = snapshots[0]["snapshot_id"]
        print(f"Reading snapshot {first_snapshot}:")
        spark.sql(f"""
            SELECT * FROM rest.demo.events VERSION AS OF {first_snapshot}
        """).show()


def demonstrate_rest_authentication(spark):
    print("=== REST Catalog Authentication ===")

    redacted_token = (
        REST_CATALOG_TOKEN[:4] + "****" if len(REST_CATALOG_TOKEN) > 4 else "****"
    )

    print("\n-- Bearer token configuration --")
    print(f"  spark.sql.catalog.rest.credential = {redacted_token}")

    print("\n-- OAuth2 configuration options --")
    oauth2_configs = {
        "spark.sql.catalog.rest.oauth2-server-uri": "https://auth.example.com/oauth/token",
        "spark.sql.catalog.rest.credential": "<client-id>:<client-secret>",
        "spark.sql.catalog.rest.scope": "catalog",
        "spark.sql.catalog.rest.oauth2.token": "<access-token>",
    }
    for key, val in oauth2_configs.items():
        print(f"  {key} = {val}")

    print("\n-- Active catalog configuration (redacted) --")
    catalog_configs = {
        "uri": REST_CATALOG_URI,
        "warehouse": REST_CATALOG_WAREHOUSE,
        "credential": redacted_token,
        "shuffle.partitions": REST_SHUFFLE_PARTITIONS,
        "executor.memory": REST_EXECUTOR_MEMORY,
        "driver.memory": REST_DRIVER_MEMORY,
    }
    for key, val in catalog_configs.items():
        print(f"  {key}: {val}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        demonstrate_rest_authentication(spark)
        demonstrate_rest_catalog_browse(spark)
        demonstrate_rest_catalog_operations(spark)
    finally:
        print("\n=== Cleanup ===")
        spark.sql("DROP TABLE IF EXISTS rest.demo.events")
        spark.sql("DROP NAMESPACE IF EXISTS rest.demo")
        print("Cleanup complete.")
        spark.stop()


if __name__ == "__main__":
    main()

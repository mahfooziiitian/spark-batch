import os

from pyspark.sql import SparkSession

HIVE_METASTORE_URI = os.environ.get(
    "HIVE_METASTORE_URI", "thrift://hive-metastore:9083"
)
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://my-bucket/iceberg")
POSTGRES_JDBC_URL = os.environ.get(
    "POSTGRES_JDBC_URL", "jdbc:postgresql://db:5432/mydb"
)

CATALOGS = ["hive", "iceberg", "postgres"]


def create_spark_session():
    return (
        SparkSession.builder.appName("MultiCatalog")
        .config("spark.sql.catalog.hive", "org.apache.spark.sql.hive.HiveCatalog")
        .config("spark.sql.catalog.hive.uri", HIVE_METASTORE_URI)
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE)
        .config(
            "spark.sql.catalog.postgres",
            "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
        )
        .config("spark.sql.catalog.postgres.url", POSTGRES_JDBC_URL)
        .getOrCreate()
    )


def query_catalog(spark, catalog, query):
    spark.sql(f"USE CATALOG {catalog}")
    return spark.sql(query)


def demonstrate_catalog_listing(spark):
    print("=== Catalog Listing ===")

    print("\n-- Available catalogs --")
    spark.sql("SHOW CATALOGS").show(truncate=False)

    for catalog in CATALOGS:
        print(f"\n-- Databases in '{catalog}' catalog --")
        try:
            spark.sql(f"SHOW DATABASES IN {catalog}").show(truncate=False)
        except Exception as e:
            print(f"  Could not list databases in {catalog}: {e}")

        print(f"-- Tables in '{catalog}' catalog (default db) --")
        try:
            spark.sql(f"SHOW TABLES IN {catalog}.default").show(truncate=False)
        except Exception as e:
            print(f"  Could not list tables in {catalog}.default: {e}")


def demonstrate_cross_catalog_join(spark):
    print("=== Cross-Catalog Join ===")

    print("\n-- Reading from Hive catalog --")
    hive_df = spark.sql("""
        SELECT customer_id, customer_name, region
        FROM hive.sales_db.customers
    """)
    hive_df.show(5)

    print("\n-- Reading from Iceberg catalog --")
    iceberg_df = spark.sql("""
        SELECT customer_id, order_id, amount, order_date
        FROM iceberg.analytics.orders
    """)
    iceberg_df.show(5)

    print("\n-- Cross-catalog JOIN --")
    joined_df = spark.sql("""
        SELECT
            c.customer_name,
            c.region,
            COUNT(o.order_id)        AS order_count,
            ROUND(SUM(o.amount), 2)  AS total_spent
        FROM hive.sales_db.customers c
        JOIN iceberg.analytics.orders o
            ON c.customer_id = o.customer_id
        GROUP BY c.customer_name, c.region
        ORDER BY total_spent DESC
    """)
    joined_df.show(10)

    print("\n-- Execution plan for cross-catalog join --")
    joined_df.explain(True)


def demonstrate_catalog_switching(spark):
    print("=== Catalog Switching ===")

    print("\n-- Switch to Hive catalog --")
    spark.sql("USE CATALOG hive")
    print(f"Current catalog: {spark.sql('SELECT current_catalog()').first()[0]}")
    spark.sql("SHOW DATABASES").show(truncate=False)
    spark.sql("SHOW TABLES IN sales_db").show(truncate=False)

    print("\n-- Switch to Iceberg catalog --")
    spark.sql("USE CATALOG iceberg")
    print(f"Current catalog: {spark.sql('SELECT current_catalog()').first()[0]}")
    spark.sql("SHOW DATABASES").show(truncate=False)
    spark.sql("SHOW TABLES IN analytics").show(truncate=False)

    print("\n-- Switch to Postgres catalog --")
    spark.sql("USE CATALOG postgres")
    print(f"Current catalog: {spark.sql('SELECT current_catalog()').first()[0]}")
    spark.sql("SHOW NAMESPACES").show(truncate=False)


def demonstrate_catalog_migration(spark):
    print("=== Catalog Migration (CTAS Pattern) ===")

    print("\n-- Migrate Hive table → Iceberg table --")
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.migrated")

    spark.sql("""
        CREATE TABLE iceberg.migrated.customers
        USING iceberg
        AS SELECT * FROM hive.sales_db.customers
    """)
    print("Created iceberg.migrated.customers from hive.sales_db.customers")

    print("\n-- Verify migrated data --")
    source_count = spark.sql(
        "SELECT COUNT(*) AS cnt FROM hive.sales_db.customers"
    ).first()["cnt"]
    target_count = spark.sql(
        "SELECT COUNT(*) AS cnt FROM iceberg.migrated.customers"
    ).first()["cnt"]
    print(f"Source rows: {source_count}, Target rows: {target_count}")

    spark.sql("SELECT * FROM iceberg.migrated.customers").show(5)

    print("\n-- Migrate with transformation --")
    spark.sql("""
        CREATE TABLE iceberg.migrated.customers_by_region
        USING iceberg
        PARTITIONED BY (region)
        AS SELECT
            customer_id,
            UPPER(customer_name) AS customer_name,
            region,
            current_timestamp()  AS migrated_at
        FROM hive.sales_db.customers
    """)
    print("Created partitioned iceberg.migrated.customers_by_region")

    spark.sql("SELECT * FROM iceberg.migrated.customers_by_region").show(5)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        demonstrate_catalog_listing(spark)
        demonstrate_catalog_switching(spark)
        demonstrate_cross_catalog_join(spark)
        demonstrate_catalog_migration(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

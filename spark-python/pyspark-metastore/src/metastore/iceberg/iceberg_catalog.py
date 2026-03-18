import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Matches infra/iceberg/ docker-compose environment variables
REST_CATALOG_URI = os.environ.get("REST_CATALOG_URI", "http://localhost:8181")
REST_CATALOG_WAREHOUSE = os.environ.get("REST_CATALOG_WAREHOUSE", "s3://warehouse/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
if os.environ.get("JAVA_HOME_11"):
    os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]

def create_spark_session(app_name="IcebergCatalog"):
    ICEBERG_VERSION = "1.5.0"
    SCALA_VERSION = "2.12"
    SPARK_COMPAT = "3.5"

    # Iceberg + AWS bundle JARs — downloaded automatically when running locally;
    # pre-installed in infra/common/Dockerfile for Docker-based runs.
    packages = ",".join([
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_COMPAT}_{SCALA_VERSION}:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    ])

    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # REST catalog (primary) — backed by infra/iceberg/ REST + MinIO
        .config(
            "spark.sql.catalog.my_iceberg",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.my_iceberg.type", "rest")
        .config("spark.sql.catalog.my_iceberg.uri", REST_CATALOG_URI)
        .config("spark.sql.catalog.my_iceberg.warehouse", REST_CATALOG_WAREHOUSE)
        .config(
            "spark.sql.catalog.my_iceberg.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.my_iceberg.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.my_iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.my_iceberg.s3.access-key-id", os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.sql.catalog.my_iceberg.s3.secret-access-key", os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.sql.catalog.my_iceberg.s3.region", AWS_REGION)
        .config("spark.sql.catalog.my_iceberg.client.region", AWS_REGION)
        .config("spark.sql.defaultCatalog", "my_iceberg")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession created — REST catalog: {REST_CATALOG_URI}")
    return spark


def demonstrate_iceberg_table_lifecycle(spark):
    print("\n=== Iceberg Table Lifecycle ===")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")
    spark.sql("DROP TABLE IF EXISTS demo.employees PURGE")
    spark.sql("""
        CREATE TABLE demo.employees (
            id INT,
            name STRING,
            department STRING,
            salary DOUBLE
        ) USING iceberg
    """)

    spark.sql("""
        INSERT INTO demo.employees VALUES
        (1, 'Alice', 'Engineering', 95000.0),
        (2, 'Bob', 'Marketing', 72000.0),
        (3, 'Charlie', 'Engineering', 88000.0)
    """)

    print("Initial data:")
    spark.sql("SELECT * FROM demo.employees ORDER BY id").show()

    spark.sql("UPDATE demo.employees SET salary = 100000.0 WHERE name = 'Alice'")
    print("After UPDATE (Alice salary):")
    df = spark.table("demo.employees").filter(F.col("name") == "Alice")
    df.show()

    print("All employees:")
    spark.sql("SELECT * FROM demo.employees ORDER BY id").show()

    spark.sql("DELETE FROM demo.employees WHERE name = 'Bob'")
    print("After DELETE (Bob):")
    spark.sql("SELECT * FROM demo.employees ORDER BY id").show()

    print("Table history:")
    spark.sql("SELECT * FROM demo.employees.history").show(truncate=False)


def demonstrate_time_travel(spark):
    print("\n=== Iceberg Time Travel ===")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")
    spark.sql("DROP TABLE IF EXISTS demo.events PURGE")
    spark.sql("""
        CREATE TABLE demo.events (
            id INT,
            event_type STRING,
            ts TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        INSERT INTO demo.events VALUES
        (1, 'click', TIMESTAMP '2024-01-01 10:00:00')
    """)
    spark.sql("""
        INSERT INTO demo.events VALUES
        (2, 'view', TIMESTAMP '2024-01-02 11:00:00')
    """)
    spark.sql("""
        INSERT INTO demo.events VALUES
        (3, 'purchase', TIMESTAMP '2024-01-03 12:00:00')
    """)

    print("All snapshots:")
    snapshots_df = spark.sql("SELECT * FROM demo.events.snapshots")
    snapshots_df.show(truncate=False)

    first_snapshot_id = (
        snapshots_df.orderBy("committed_at").select("snapshot_id").first()[0]
    )
    print(f"Data at snapshot {first_snapshot_id}:")
    spark.sql(f"SELECT * FROM demo.events VERSION AS OF {first_snapshot_id}").show()

    print("Current data:")
    spark.sql("SELECT * FROM demo.events ORDER BY id").show()


def demonstrate_schema_evolution(spark):
    print("\n=== Iceberg Schema Evolution ===")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")
    spark.sql("DROP TABLE IF EXISTS demo.products")
    spark.sql("""
        CREATE TABLE demo.products (
            id INT,
            name STRING,
            price DOUBLE
        ) USING iceberg
    """)

    spark.sql("""
        INSERT INTO demo.products VALUES (1, 'Widget', 9.99)
    """)

    spark.sql("ALTER TABLE demo.products ADD COLUMNS (category STRING, weight DOUBLE)")
    print("After ADD COLUMNS:")
    spark.sql("DESCRIBE TABLE demo.products").show(truncate=False)

    spark.sql("ALTER TABLE demo.products RENAME COLUMN weight TO weight_kg")
    print("After RENAME COLUMN weight -> weight_kg:")
    spark.sql("DESCRIBE TABLE demo.products").show(truncate=False)

    spark.sql("""
        INSERT INTO demo.products VALUES (2, 'Gadget', 19.99, 'Electronics', 0.5)
    """)
    print("Data after schema evolution:")
    spark.sql("SELECT * FROM demo.products ORDER BY id").show()


def demonstrate_partition_evolution(spark):
    print("\n=== Iceberg Partition Evolution ===")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")
    spark.sql("DROP TABLE IF EXISTS demo.logs PURGE")
    spark.sql("""
        CREATE TABLE demo.logs (
            id INT,
            event_date DATE,
            level STRING,
            message STRING
        ) USING iceberg
        PARTITIONED BY (event_date)
    """)

    spark.sql("""
        INSERT INTO demo.logs VALUES
        (1, DATE '2024-01-15', 'INFO', 'Service started'),
        (2, DATE '2024-02-20', 'ERROR', 'Connection failed')
    """)

    print("Original data (partitioned by event_date):")
    spark.sql("SELECT * FROM demo.logs ORDER BY id").show(truncate=False)

    spark.sql(
        "ALTER TABLE demo.logs REPLACE PARTITION FIELD event_date WITH month(event_date)"
    )
    print("Partition spec evolved to month(event_date).")

    spark.sql("""
        INSERT INTO demo.logs VALUES
        (3, DATE '2024-03-10', 'WARN', 'High memory usage')
    """)

    print("Data after partition evolution:")
    spark.sql("SELECT * FROM demo.logs ORDER BY id").show(truncate=False)

    print("Data files (shows partition info):")
    spark.sql(
        "SELECT file_path, partition, record_count FROM demo.logs.files"
    ).show(truncate=False)


def demonstrate_maintenance(spark):
    print("\n=== Iceberg Table Maintenance ===")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")
    spark.sql("DROP TABLE IF EXISTS demo.metrics PURGE")
    spark.sql("""
        CREATE TABLE demo.metrics (
            id INT,
            value DOUBLE
        ) USING iceberg
    """)
    for i in range(5):
        spark.sql(f"INSERT INTO demo.metrics VALUES ({i}, {i * 1.1})")

    print("Snapshots before maintenance:")
    spark.sql("SELECT snapshot_id, committed_at FROM demo.metrics.snapshots").show(
        truncate=False
    )

    spark.sql("""
        CALL my_iceberg.system.expire_snapshots(
            table => 'demo.metrics',
            older_than => TIMESTAMP '2099-01-01 00:00:00',
            retain_last => 2
        )
    """)
    print("Snapshots after expire_snapshots (retain_last=2):")
    spark.sql("SELECT snapshot_id, committed_at FROM demo.metrics.snapshots").show(
        truncate=False
    )

    try:
        spark.sql("""
            CALL my_iceberg.system.remove_orphan_files(
                table => 'demo.metrics'
            )
        """)
        print("Orphan files removed.")
    except Exception as e:
        print(f"remove_orphan_files skipped (expected in some local environments): {e.__class__.__name__}: {e}")  # noqa: E501

    spark.sql("""
        CALL my_iceberg.system.rewrite_data_files(
            table => 'demo.metrics'
        )
    """)
    print("Data files compacted.")


def main():
    spark = create_spark_session()

    try:
        print("\n=== Catalog Information ===")
        spark.sql("SHOW CATALOGS").show()

        # REST catalog requires a namespace before listing tables
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo")
        spark.sql("SHOW NAMESPACES").show()
        spark.sql("SHOW TABLES IN demo").show()

        demonstrate_iceberg_table_lifecycle(spark)
        demonstrate_time_travel(spark)
        demonstrate_schema_evolution(spark)
        demonstrate_partition_evolution(spark)
        demonstrate_maintenance(spark)

    finally:
        for table in ["employees", "events", "products", "logs", "metrics"]:
            try:
                spark.sql(f"DROP TABLE IF EXISTS demo.{table} PURGE")
            except Exception:
                pass
        try:
            spark.sql("DROP NAMESPACE IF EXISTS demo")
        except Exception:
            pass
        spark.stop()


if __name__ == "__main__":
    main()

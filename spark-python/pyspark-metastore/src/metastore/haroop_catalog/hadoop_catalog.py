import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(warehouse_path=None, app_name="HadoopCatalog"):
    if warehouse_path is None:
        warehouse_path = os.environ.get(
            "HADOOP_WAREHOUSE", "hdfs://namenode:8020/warehouse"
        )

    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.catalog.hadoop_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
        .config("spark.sql.catalog.hadoop_catalog.warehouse", warehouse_path)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.defaultCatalog", "hadoop_catalog")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession started with HadoopCatalog and Iceberg extensions.")
    return spark


def demonstrate_hadoop_catalog_tables(spark):
    print("\n=== Hadoop Catalog Tables ===")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.demo")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS hadoop_catalog.demo.sensors (
            sensor_id INT,
            location STRING,
            temperature DOUBLE,
            reading_time TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        INSERT INTO hadoop_catalog.demo.sensors VALUES
        (1, 'warehouse-A', 22.5, TIMESTAMP '2024-01-15 08:00:00'),
        (2, 'warehouse-B', 18.3, TIMESTAMP '2024-01-15 08:05:00'),
        (3, 'warehouse-A', 23.1, TIMESTAMP '2024-01-15 09:00:00')
    """)

    print("Sensor data:")
    spark.sql("SELECT * FROM hadoop_catalog.demo.sensors ORDER BY sensor_id").show()

    avg_temp = spark.table("hadoop_catalog.demo.sensors").select(
        F.avg("temperature").alias("avg_temp")
    )
    print("Average temperature:")
    avg_temp.show()

    print("Tables in hadoop_catalog.demo:")
    spark.sql("SHOW TABLES IN hadoop_catalog.demo").show()


def demonstrate_iceberg_on_hadoop(spark):
    print("\n=== Iceberg on Hadoop: Time Travel & Schema Evolution ===")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.demo")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS hadoop_catalog.demo.metrics (
            id INT,
            metric_name STRING,
            value DOUBLE
        ) USING iceberg
    """)

    spark.sql("INSERT INTO hadoop_catalog.demo.metrics VALUES (1, 'cpu', 45.2)")
    spark.sql("INSERT INTO hadoop_catalog.demo.metrics VALUES (2, 'memory', 78.9)")
    spark.sql("INSERT INTO hadoop_catalog.demo.metrics VALUES (3, 'disk', 55.0)")

    print("Snapshots:")
    snapshots_df = spark.sql("SELECT * FROM hadoop_catalog.demo.metrics.snapshots")
    snapshots_df.show(truncate=False)

    first_snapshot_id = (
        snapshots_df.orderBy("committed_at").select("snapshot_id").first()[0]
    )
    print(f"Data at first snapshot ({first_snapshot_id}):")
    spark.sql(
        f"SELECT * FROM hadoop_catalog.demo.metrics VERSION AS OF {first_snapshot_id}"
    ).show()

    print("Current data:")
    spark.sql("SELECT * FROM hadoop_catalog.demo.metrics ORDER BY id").show()

    spark.sql(
        "ALTER TABLE hadoop_catalog.demo.metrics ADD COLUMNS (host STRING, region STRING)"
    )
    print("Schema after evolution:")
    spark.sql("DESCRIBE TABLE hadoop_catalog.demo.metrics").show(truncate=False)

    spark.sql("""
        INSERT INTO hadoop_catalog.demo.metrics VALUES
        (4, 'network', 12.5, 'host-01', 'us-east')
    """)
    print("Data after schema evolution:")
    spark.sql("SELECT * FROM hadoop_catalog.demo.metrics ORDER BY id").show()


def demonstrate_namespace_management(spark):
    print("\n=== Namespace Management ===")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.test_ns_alpha")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.test_ns_beta")

    print("All namespaces in hadoop_catalog:")
    spark.sql("SHOW NAMESPACES IN hadoop_catalog").show()

    spark.sql("""
        CREATE TABLE IF NOT EXISTS hadoop_catalog.test_ns_alpha.temp_tbl (
            id INT
        ) USING iceberg
    """)
    print("Tables in test_ns_alpha:")
    spark.sql("SHOW TABLES IN hadoop_catalog.test_ns_alpha").show()

    spark.sql("DROP TABLE IF EXISTS hadoop_catalog.test_ns_alpha.temp_tbl PURGE")
    spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.test_ns_alpha")
    spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.test_ns_beta")
    print("Namespaces after cleanup:")
    spark.sql("SHOW NAMESPACES IN hadoop_catalog").show()


def compare_with_hive_catalog(spark):
    print("\n=== Hadoop vs Hive Catalog Comparison ===")
    comparison = [
        ("Metadata Storage", "Filesystem (warehouse path)", "Hive Metastore (RDBMS)"),
        (
            "External Dependency",
            "None (self-contained)",
            "Requires Hive Metastore service",
        ),
        ("Atomic Rename (HDFS)", "Yes", "Yes"),
        (
            "Atomic Rename (S3)",
            "No — risk of partial updates",
            "No — same S3 limitation",
        ),
        ("Multi-Writer Safety", "Single-writer only", "Lock-based via metastore"),
        (
            "Service Discovery",
            "Requires shared filesystem access",
            "Centralized thrift endpoint",
        ),
        ("Namespace Support", "Directory-based", "Database-based in metastore"),
        (
            "Best For",
            "Simple/standalone deployments",
            "Multi-tenant / production environments",
        ),
    ]

    df = spark.createDataFrame(comparison, ["Aspect", "Hadoop Catalog", "Hive Catalog"])
    df.show(truncate=False)


def main():
    spark = create_spark_session()

    try:
        print("\n=== Catalog Information ===")
        spark.sql("SHOW CATALOGS").show()
        spark.sql("SHOW DATABASES").show()
        spark.sql("SHOW TABLES").show()

        demonstrate_hadoop_catalog_tables(spark)
        demonstrate_iceberg_on_hadoop(spark)
        demonstrate_namespace_management(spark)
        compare_with_hive_catalog(spark)

    finally:
        spark.sql("DROP TABLE IF EXISTS hadoop_catalog.demo.sensors PURGE")
        spark.sql("DROP TABLE IF EXISTS hadoop_catalog.demo.metrics PURGE")
        spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.demo")
        spark.stop()


if __name__ == "__main__":
    main()

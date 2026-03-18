# HiveServer2 vs Direct Hive Metastore:
#   - Direct metastore (thrift://host:9083): Spark talks to the metastore DB directly.
#     Best for batch Spark jobs that need full catalog access without an intermediary.
#   - HiveServer2 (host:10000): Spark connects through HiveServer2's JDBC/ODBC interface.
#     Use when you need centralized auth (Kerberos/LDAP), query auditing,
#     or compatibility with tools that speak the HiveServer2 protocol (Beeline, JDBC clients).

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(hiveserver2_url: str) -> SparkSession:
    host = os.environ.get("HIVESERVER2_HOST", "localhost")
    port = os.environ.get("HIVESERVER2_PORT", "10000")

    return (
        SparkSession.builder.appName("hive-server2-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("hive.server2.thrift.bind.host", host)
        .config("hive.server2.thrift.port", port)
        .config("spark.sql.hive.thriftServer.singleSession", "true")
        .config("spark.hive.metastore.uris", hiveserver2_url)
        .enableHiveSupport()
        .getOrCreate()
    )


def demonstrate_hiveserver2_queries(spark: SparkSession) -> None:
    print("\n=== HiveServer2 Queries ===")
    spark.sql("SHOW CATALOGS").show(truncate=False)
    spark.sql("SHOW DATABASES").show(truncate=False)

    spark.sql("CREATE DATABASE IF NOT EXISTS hs2_demo_db")
    spark.sql("USE hs2_demo_db")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS events (
            event_id INT,
            event_name STRING,
            event_time TIMESTAMP,
            payload STRING
        )
        STORED AS PARQUET
    """)

    spark.sql("""
        INSERT INTO events VALUES
        (1, 'login',    TIMESTAMP '2024-06-01 08:30:00', '{"user":"alice"}'),
        (2, 'purchase', TIMESTAMP '2024-06-01 09:15:00', '{"item":"widget","qty":2}'),
        (3, 'logout',   TIMESTAMP '2024-06-01 10:00:00', '{"user":"alice"}')
    """)

    spark.sql("SHOW TABLES").show(truncate=False)
    spark.sql("SELECT * FROM events ORDER BY event_time").show(truncate=False)


def demonstrate_beeline_equivalent(spark: SparkSession) -> None:
    """SQL operations that mirror common Beeline commands."""
    print("\n=== Beeline-Equivalent Operations ===")
    spark.sql("USE hs2_demo_db")

    spark.sql("DESCRIBE FORMATTED events").show(100, truncate=False)
    spark.sql("SHOW COLUMNS IN events").show(truncate=False)

    df = (
        spark.table("events")
        .filter(F.col("event_name") != "logout")
        .groupBy("event_name")
        .agg(
            F.count("*").alias("total"),
        )
    )
    print("Event summary (excluding logout):")
    df.show(truncate=False)

    spark.sql("SET spark.sql.hive.thriftServer.singleSession").show(truncate=False)


def cleanup(spark: SparkSession) -> None:
    print("\n=== Cleanup ===")
    spark.sql("DROP TABLE IF EXISTS hs2_demo_db.events")
    spark.sql("DROP DATABASE IF EXISTS hs2_demo_db CASCADE")
    print("Cleanup complete.")


def main() -> None:
    hiveserver2_url = os.environ.get("HIVESERVER2_URL", "thrift://localhost:10000")

    spark = create_spark_session(hiveserver2_url)
    spark.sparkContext.setLogLevel("WARN")

    try:
        demonstrate_hiveserver2_queries(spark)
        demonstrate_beeline_equivalent(spark)
    finally:
        cleanup(spark)
        spark.stop()


if __name__ == "__main__":
    main()

# Hive LLAP (Low Latency Analytical Processing):
#   Available in Hive 2.0+. LLAP daemons run as persistent services that cache data
#   in-memory and execute fragments of Hive queries, enabling sub-second response times
#   for interactive queries on Hive-managed tables.
#   Prerequisites: LLAP daemons must be running in the cluster (managed by YARN or
#   manually via `hive --service llap`). Typically used with HiveServer2 in interactive
#   mode on platforms like Hortonworks HDP / Cloudera CDP.

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session_with_llap() -> SparkSession:
    llap_hosts = os.environ.get("LLAP_DAEMON_HOSTS", "localhost")
    metastore_uri = os.environ.get("HIVE_METASTORE_URI", "thrift://localhost:9083")

    return (
        SparkSession.builder.appName("hive-llap-demo")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.hive.metastore.uris", metastore_uri)
        .config("hive.metastore.uris", metastore_uri)
        .config("spark.sql.hive.llap.daemon.service.hosts", llap_hosts)
        .config("hive.llap.execution.mode", "auto")
        .config("hive.execution.engine", "tez")
        .config("hive.execution.mode", "llap")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


def demonstrate_llap_query(spark: SparkSession) -> None:
    print("\n=== LLAP Query Demo ===")
    spark.sql("SHOW CATALOGS").show(truncate=False)
    spark.sql("SHOW DATABASES").show(truncate=False)

    spark.sql("CREATE DATABASE IF NOT EXISTS llap_demo_db")
    spark.sql("USE llap_demo_db")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            sensor_id STRING,
            reading_value DOUBLE,
            reading_time TIMESTAMP
        )
        STORED AS ORC
    """)

    spark.sql("""
        INSERT INTO sensor_readings VALUES
        ('sensor-01', 23.5, TIMESTAMP '2024-06-01 08:00:00'),
        ('sensor-01', 24.1, TIMESTAMP '2024-06-01 09:00:00'),
        ('sensor-02', 18.7, TIMESTAMP '2024-06-01 08:00:00'),
        ('sensor-02', 19.3, TIMESTAMP '2024-06-01 09:00:00'),
        ('sensor-03', 30.0, TIMESTAMP '2024-06-01 08:00:00')
    """)

    # LLAP caches columnar data in-memory, making repeated analytical queries fast
    spark.sql("SHOW TABLES").show(truncate=False)
    spark.sql("SELECT * FROM sensor_readings ORDER BY sensor_id, reading_time").show(
        truncate=False
    )

    summary = (
        spark.table("sensor_readings")
        .groupBy("sensor_id")
        .agg(
            F.avg("reading_value").alias("avg_reading"),
            F.min("reading_value").alias("min_reading"),
            F.max("reading_value").alias("max_reading"),
            F.count("*").alias("num_readings"),
        )
    )
    print("Sensor summary (benefits from LLAP caching on repeated runs):")
    summary.show(truncate=False)


def cleanup(spark: SparkSession) -> None:
    print("\n=== Cleanup ===")
    spark.sql("DROP TABLE IF EXISTS llap_demo_db.sensor_readings")
    spark.sql("DROP DATABASE IF EXISTS llap_demo_db CASCADE")
    print("Cleanup complete.")


def main() -> None:
    spark = create_spark_session_with_llap()
    spark.sparkContext.setLogLevel("WARN")

    try:
        demonstrate_llap_query(spark)
    finally:
        cleanup(spark)
        spark.stop()


if __name__ == "__main__":
    main()

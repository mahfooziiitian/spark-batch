import os
from pathlib import Path
import shutil
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: F401


def create_spark_session() -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "local[*]")
    warehouse = os.environ.get("SPARK_WAREHOUSE", "spark-warehouse")
    return (
        SparkSession.builder.appName("SparkWarehouseDemo")
        .master(master)
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def demonstrate_warehouse_dir(spark: SparkSession) -> None:
    print("=" * 60)
    print("DEMO: Warehouse Directory Configuration")
    print("=" * 60)

    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    abs_path = Path(warehouse_dir).resolve()
    print(f"Spark version: {spark.version}")
    print(f"Configured warehouse dir: {warehouse_dir}")
    print(f"Absolute path: {abs_path}")
    print(f"Directory exists: {abs_path.is_dir()}")

    print("\nSHOW CATALOGS:")
    spark.sql("SHOW CATALOGS").show(truncate=False)
    print("SHOW DATABASES:")
    spark.sql("SHOW DATABASES").show(truncate=False)


def demonstrate_managed_table(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Managed Table")
    print("=" * 60)

    data = [(1, "Alice", 85.5), (2, "Bob", 92.0), (3, "Cathy", 78.3)]
    df = spark.createDataFrame(data, ["id", "name", "score"])
    df.write.mode("overwrite").saveAsTable("wh_managed_scores")
    print("Created managed table 'wh_managed_scores'")

    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    table_path = (Path(warehouse_dir) / "wh_managed_scores").as_posix()
    print(f"Data stored at: {table_path}")
    print(f"Path exists: {os.path.isdir(table_path)}")

    spark.sql("INSERT INTO wh_managed_scores VALUES (4, 'Dave', 88.1)")
    print("\nAfter INSERT:")
    spark.sql("SELECT * FROM wh_managed_scores ORDER BY id").show()

    print("SHOW TABLES:")
    spark.sql("SHOW TABLES").show(truncate=False)

    spark.sql("DROP TABLE IF EXISTS wh_managed_scores")
    print(f"After DROP: path exists = {os.path.isdir(table_path)}")


def demonstrate_external_table(spark: SparkSession, external_path: str) -> None:
    print("\n" + "=" * 60)
    print("DEMO: External Table")
    print("=" * 60)

    data = [(1, "sensor_a", 23.4), (2, "sensor_b", 19.8), (3, "sensor_c", 31.2)]
    df = spark.createDataFrame(data, ["id", "sensor", "reading"])
    df.write.mode("overwrite").parquet(external_path)
    print(f"Wrote parquet data to: {external_path}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS wh_external_sensors (
            id INT, sensor STRING, reading DOUBLE
        )
        USING parquet
        LOCATION '{external_path}'
    """)
    print("Created external table 'wh_external_sensors'")
    spark.sql("SELECT * FROM wh_external_sensors").show()

    spark.sql("DESCRIBE EXTENDED wh_external_sensors").show(truncate=False)

    spark.sql("DROP TABLE IF EXISTS wh_external_sensors")
    data_survives = os.path.isdir(external_path)
    print(f"After DROP: external data still exists = {data_survives}")
    # External table DROP only removes metadata; data files remain at LOCATION


def demonstrate_partition_table(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Partitioned Table")
    print("=" * 60)

    data = [
        (1, "Alice", "Engineering", 2024),
        (2, "Bob", "Marketing", 2024),
        (3, "Cathy", "Engineering", 2023),
        (4, "Dave", "Marketing", 2023),
        (5, "Eve", "Sales", 2024),
    ]
    df = spark.createDataFrame(data, ["id", "name", "dept", "year"])

    df.write.mode("overwrite").partitionBy("year", "dept").saveAsTable(
        "wh_partitioned_employees"
    )
    print(
        "Created partitioned table 'wh_partitioned_employees' (partitioned by year, dept)"
    )

    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    table_path = (Path(warehouse_dir) / "wh_partitioned_employees").as_posix()
    print(f"\nPartition layout at: {table_path}")
    if os.path.isdir(table_path):
        for root, dirs, files in os.walk(table_path):
            depth = root.replace(table_path, "").count(os.sep)
            indent = "  " * depth
            print(f"{indent}{os.path.basename(root)}/")

    print("\nSHOW PARTITIONS:")
    spark.sql("SHOW PARTITIONS wh_partitioned_employees").show(truncate=False)

    print("Query single partition (year=2024):")
    spark.sql("SELECT * FROM wh_partitioned_employees WHERE year = 2024").show()

    spark.sql("DROP TABLE IF EXISTS wh_partitioned_employees")
    print("Cleaned up partitioned table")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    demonstrate_warehouse_dir(spark)
    demonstrate_managed_table(spark)

    tmp_dir = tempfile.mkdtemp(prefix="spark_warehouse_demo_")
    external_path = os.path.join(tmp_dir, "external_sensors")
    try:
        demonstrate_external_table(spark, external_path)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    demonstrate_partition_table(spark)

    print("\n--- All warehouse demos complete ---")
    spark.stop()


if __name__ == "__main__":
    main()

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(metastore_uri: str, warehouse_dir: str) -> SparkSession:
    return (
        SparkSession.builder.appName("hive-remote-metastore")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.hive.metastore.uris", metastore_uri)
        .config("hive.metastore.uris", metastore_uri)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .enableHiveSupport()
        .getOrCreate()
    )


def demonstrate_hive_databases(spark: SparkSession) -> None:
    print("\n=== Hive Databases ===")
    spark.sql("SHOW CATALOGS").show(truncate=False)
    spark.sql("SHOW DATABASES").show(truncate=False)

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS demo_db COMMENT 'Demo database for testing'"
    )
    spark.sql("USE demo_db")
    print("Current database:", spark.catalog.currentDatabase())
    spark.sql("SHOW DATABASES").show(truncate=False)


def demonstrate_hive_tables(spark: SparkSession) -> None:
    print("\n=== Hive Tables ===")
    spark.sql("USE demo_db")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS employees (
            id INT,
            name STRING,
            department STRING,
            salary DOUBLE
        )
        STORED AS PARQUET
    """)

    spark.sql("""
        INSERT INTO employees VALUES
        (1, 'Alice', 'Engineering', 95000.0),
        (2, 'Bob', 'Marketing', 78000.0),
        (3, 'Carol', 'Engineering', 102000.0),
        (4, 'Dave', 'Sales', 68000.0)
    """)

    spark.sql("SHOW TABLES").show(truncate=False)
    spark.sql("SELECT * FROM employees").show(truncate=False)
    spark.sql("DESCRIBE EXTENDED employees").show(truncate=False)

    avg_salary = (
        spark.table("employees")
        .groupBy("department")
        .agg(
            F.avg("salary").alias("avg_salary"),
            F.count("*").alias("headcount"),
        )
    )
    avg_salary.show(truncate=False)


def demonstrate_partitioned_table(spark: SparkSession) -> None:
    print("\n=== Partitioned Hive Table ===")
    spark.sql("USE demo_db")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS sales (
            order_id INT,
            product STRING,
            amount DOUBLE
        )
        PARTITIONED BY (sale_year INT, sale_month INT)
        STORED AS PARQUET
    """)

    spark.sql("""
        INSERT INTO sales PARTITION (sale_year=2024, sale_month=1) VALUES
        (1, 'Widget', 150.0), (2, 'Gadget', 250.0)
    """)
    spark.sql("""
        INSERT INTO sales PARTITION (sale_year=2024, sale_month=2) VALUES
        (3, 'Widget', 175.0), (4, 'Sprocket', 300.0)
    """)

    spark.sql("SHOW PARTITIONS sales").show(truncate=False)
    spark.sql("MSCK REPAIR TABLE sales")
    spark.sql("SELECT * FROM sales").show(truncate=False)


def demonstrate_hive_table_properties(spark: SparkSession) -> None:
    print("\n=== Hive Table Properties ===")
    spark.sql("USE demo_db")

    spark.sql("""
        ALTER TABLE employees SET TBLPROPERTIES (
            'owner' = 'data-team',
            'retention_days' = '90',
            'classification' = 'internal'
        )
    """)
    spark.sql("SHOW TBLPROPERTIES employees").show(truncate=False)


def cleanup(spark: SparkSession) -> None:
    print("\n=== Cleanup ===")
    spark.sql("DROP TABLE IF EXISTS demo_db.sales")
    spark.sql("DROP TABLE IF EXISTS demo_db.employees")
    spark.sql("DROP DATABASE IF EXISTS demo_db CASCADE")
    print("Cleanup complete.")


def main() -> None:
    metastore_uri = os.environ.get("HIVE_METASTORE_URI", "thrift://localhost:9083")
    warehouse_dir = os.environ.get("SPARK_WAREHOUSE", "/user/hive/warehouse")

    spark = create_spark_session(metastore_uri, warehouse_dir)
    spark.sparkContext.setLogLevel("WARN")

    try:
        demonstrate_hive_databases(spark)
        demonstrate_hive_tables(spark)
        demonstrate_partitioned_table(spark)
        demonstrate_hive_table_properties(spark)
    finally:
        cleanup(spark)
        spark.stop()


if __name__ == "__main__":
    main()

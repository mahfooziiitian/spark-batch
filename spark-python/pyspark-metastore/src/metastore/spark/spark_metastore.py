import os
import shutil
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: F401

from metastore.catalog_metadata import print_catalog_metadata


def create_spark_session() -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "local[*]")
    warehouse = os.environ.get("SPARK_WAREHOUSE", "spark-warehouse")
    return (
        SparkSession.builder.appName("SparkMetastoreDemo")
        .master(master)
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def show_catalogs(spark: SparkSession) -> None:
    print("Available catalogs:")
    spark.sql("SHOW CATALOGS").show(truncate=False)


def show_databases(spark: SparkSession, catalog: str = "") -> None:
    if catalog:
        print(f"Databases in catalog '{catalog}':")
        spark.sql(f"SHOW DATABASES IN {catalog}").show(truncate=False)
    else:
        print("Databases in default catalog:")
        spark.sql("SHOW DATABASES").show(truncate=False)


def show_tables(spark: SparkSession, database: str = "") -> None:
    if database:
        print(f"Tables in database '{database}':")
        spark.sql(f"SHOW TABLES IN {database}").show(truncate=False)
    else:
        print("Tables in current database:")
        spark.sql("SHOW TABLES").show(truncate=False)


def drop_table_if_exists(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def create_sample_table(spark: SparkSession) -> None:
    data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
    df = spark.createDataFrame(data, ["id", "name"])
    df.write.mode("overwrite").saveAsTable("my_table")


def demonstrate_warehouse(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Warehouse Directory")
    print("=" * 60)

    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    print(f"Configured warehouse dir: {warehouse_dir}")
    abs_path = os.path.abspath(warehouse_dir)
    print(f"Absolute path: {abs_path}")
    print(f"Directory exists: {os.path.isdir(abs_path)}")


def demonstrate_managed_vs_external(spark: SparkSession, tmp_dir: str) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Managed vs External Tables")
    print("=" * 60)

    data = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    df = spark.createDataFrame(data, ["id", "value"])

    df.write.mode("overwrite").saveAsTable("managed_demo")
    print("\nCreated MANAGED table 'managed_demo'")
    spark.sql("DESCRIBE EXTENDED managed_demo").show(truncate=False)

    external_path = os.path.join(tmp_dir, "external_data")
    df.write.mode("overwrite").parquet(external_path)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS external_demo (id INT, value STRING)
        USING parquet
        LOCATION '{external_path}'
    """)
    print(f"Created EXTERNAL table 'external_demo' at {external_path}")
    spark.sql("DESCRIBE EXTENDED external_demo").show(truncate=False)

    print("\nQuery both tables:")
    spark.sql("SELECT 'managed' as source, * FROM managed_demo").show()
    spark.sql("SELECT 'external' as source, * FROM external_demo").show()

    spark.sql("DROP TABLE IF EXISTS managed_demo")
    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    managed_path = os.path.join(os.path.abspath(warehouse_dir), "managed_demo")
    print(f"After DROP MANAGED: data dir exists = {os.path.isdir(managed_path)}")

    spark.sql("DROP TABLE IF EXISTS external_demo")
    print(f"After DROP EXTERNAL: data dir exists = {os.path.isdir(external_path)}")
    # External table data persists after DROP — only metadata is removed


def demonstrate_catalog_api(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Spark Catalog Python API")
    print("=" * 60)

    spark.sql("CREATE DATABASE IF NOT EXISTS api_demo_db")
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "val"])
    df.write.mode("overwrite").saveAsTable("api_demo_db.api_table")

    print("\nlistCatalogs():")
    for c in spark.catalog.listCatalogs():
        print(f"  - {c.name}")

    print("\nlistDatabases():")
    for db in spark.catalog.listDatabases():
        print(f"  - {db.name}")

    print("\nlistTables('api_demo_db'):")
    for t in spark.catalog.listTables("api_demo_db"):
        print(f"  - {t.name} (type={t.tableType}, isTemp={t.isTemporary})")

    print(
        f"\ntableExists('api_demo_db.api_table'): "
        f"{spark.catalog.tableExists('api_demo_db.api_table')}"
    )
    print(
        f"tableExists('api_demo_db.nonexistent'): "
        f"{spark.catalog.tableExists('api_demo_db.nonexistent')}"
    )

    print(f"\ncurrentCatalog(): {spark.catalog.currentCatalog()}")
    print(f"currentDatabase(): {spark.catalog.currentDatabase()}")

    spark.sql("DROP TABLE IF EXISTS api_demo_db.api_table")
    spark.sql("DROP DATABASE IF EXISTS api_demo_db CASCADE")
    print("Cleaned up api_demo_db")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark version:", spark.version)
    print("Catalog metadata:", print_catalog_metadata(spark))

    show_catalogs(spark)
    default_catalog = spark.conf.get("spark.sql.defaultCatalog", "spark_catalog")
    show_databases(spark, default_catalog)
    show_tables(spark)

    create_sample_table(spark)
    print("\nSample table query:")
    spark.sql("SELECT * FROM spark_catalog.default.my_table").show()
    drop_table_if_exists(spark, "my_table")

    demonstrate_warehouse(spark)

    tmp_dir = tempfile.mkdtemp(prefix="spark_metastore_demo_")
    try:
        demonstrate_managed_vs_external(spark, tmp_dir)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    demonstrate_catalog_api(spark)

    print("\n--- All spark metastore demos complete ---")
    spark.stop()


if __name__ == "__main__":
    main()

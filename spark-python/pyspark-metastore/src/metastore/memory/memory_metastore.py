import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: F401

from metastore.catalog_metadata import print_catalog_metadata


def create_spark_session() -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "local[*]")
    warehouse = os.environ.get("SPARK_WAREHOUSE", "spark-warehouse")
    return (
        SparkSession.builder.appName("MemoryMetastoreDemo")
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


def create_sample_database(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Create Sample Database and Tables")
    print("=" * 60)

    spark.sql("CREATE DATABASE IF NOT EXISTS demo_db")
    spark.sql("USE demo_db")
    print("Created and switched to database 'demo_db'")

    employees = spark.createDataFrame(
        [
            (1, "Alice", "Engineering"),
            (2, "Bob", "Marketing"),
            (3, "Cathy", "Engineering"),
        ],
        ["id", "name", "department"],
    )
    employees.write.mode("overwrite").saveAsTable("demo_db.employees")
    print("Created table 'demo_db.employees'")

    departments = spark.createDataFrame(
        [("Engineering", 50), ("Marketing", 30), ("Sales", 20)],
        ["name", "headcount"],
    )
    departments.write.mode("overwrite").saveAsTable("demo_db.departments")
    print("Created table 'demo_db.departments'")

    show_tables(spark, "demo_db")


def demonstrate_catalog_operations(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Full Catalog Lifecycle")
    print("=" * 60)

    spark.sql("CREATE DATABASE IF NOT EXISTS lifecycle_db")
    spark.sql("USE lifecycle_db")
    print("Created database 'lifecycle_db'")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lifecycle_db.products (
            id INT,
            name STRING,
            price DOUBLE
        )
    """)
    print("Created table 'lifecycle_db.products'")

    spark.sql("INSERT INTO lifecycle_db.products VALUES (1, 'Widget', 9.99)")
    spark.sql("INSERT INTO lifecycle_db.products VALUES (2, 'Gadget', 19.99)")
    spark.sql("INSERT INTO lifecycle_db.products VALUES (3, 'Doohickey', 4.99)")
    print("\nInserted 3 rows:")
    spark.sql("SELECT * FROM lifecycle_db.products").show()

    print("DESCRIBE TABLE:")
    spark.sql("DESCRIBE TABLE lifecycle_db.products").show(truncate=False)

    print("DESCRIBE EXTENDED:")
    spark.sql("DESCRIBE TABLE EXTENDED lifecycle_db.products").show(truncate=False)

    spark.sql("DROP TABLE IF EXISTS lifecycle_db.products")
    spark.sql("USE default")
    spark.sql("DROP DATABASE IF EXISTS lifecycle_db CASCADE")
    print("Cleaned up: dropped table 'products' and database 'lifecycle_db'")


def demonstrate_temp_views(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Temp Views vs Replace Temp Views")
    print("=" * 60)

    data = spark.createDataFrame(
        [(1, "alpha"), (2, "beta"), (3, "gamma")], ["id", "label"]
    )

    data.createTempView("my_temp_view")
    print("Created temp view 'my_temp_view':")
    spark.sql("SELECT * FROM my_temp_view").show()

    try:
        data.createTempView("my_temp_view")
    except Exception as e:
        print(f"Expected error on duplicate createTempView: {type(e).__name__}")

    updated = spark.createDataFrame([(10, "delta"), (20, "epsilon")], ["id", "label"])
    updated.createOrReplaceTempView("my_temp_view")
    print("Replaced temp view with new data:")
    spark.sql("SELECT * FROM my_temp_view").show()

    print("Temp views in catalog (isTemporary=True):")
    for t in spark.catalog.listTables():
        if t.isTemporary:
            print(f"  - {t.name}")

    spark.catalog.dropTempView("my_temp_view")
    print("Dropped temp view 'my_temp_view'")

    # In-memory metastore: all metadata (databases, tables, temp views)
    # is lost when the SparkSession stops. There is no persistent storage.


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark version:", spark.version)
    print("Catalog metadata:", print_catalog_metadata(spark))

    show_catalogs(spark)
    default_catalog = spark.conf.get("spark.sql.defaultCatalog", "spark_catalog")
    show_databases(spark, default_catalog)
    show_tables(spark)

    create_sample_database(spark)
    demonstrate_catalog_operations(spark)
    demonstrate_temp_views(spark)

    # Clean up demo_db
    spark.sql("USE default")
    spark.sql("DROP TABLE IF EXISTS demo_db.employees")
    spark.sql("DROP TABLE IF EXISTS demo_db.departments")
    spark.sql("DROP DATABASE IF EXISTS demo_db CASCADE")
    print("\nCleaned up all demo databases and tables")

    spark.stop()


if __name__ == "__main__":
    main()

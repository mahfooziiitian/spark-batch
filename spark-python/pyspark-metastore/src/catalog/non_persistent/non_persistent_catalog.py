import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: F401


def create_spark_session() -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "local[*]")
    warehouse = os.environ.get("SPARK_WAREHOUSE", "spark-warehouse")
    return (
        SparkSession.builder.appName("NonPersistentCatalogDemo")
        .master(master)
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def demonstrate_temp_view(spark: SparkSession) -> None:
    print("=" * 60)
    print("DEMO: Temporary Views")
    print("=" * 60)

    data = [("Alice", 30), ("Bob", 25), ("Cathy", 35)]
    df = spark.createDataFrame(data, ["name", "age"])

    df.createTempView("people_temp")
    print("\nCreated temp view 'people_temp':")
    spark.sql("SELECT * FROM people_temp").show()

    print("Temp view visible in catalog:")
    for t in spark.catalog.listTables():
        if t.isTemporary:
            print(f"  - {t.name} (temporary={t.isTemporary})")

    # createTempView raises error if view already exists;
    # createOrReplaceTempView overwrites safely
    updated = spark.createDataFrame([("Dave", 40), ("Eve", 28)], ["name", "age"])
    updated.createOrReplaceTempView("people_temp")
    print("\nReplaced temp view with new data:")
    spark.sql("SELECT * FROM people_temp").show()

    spark.catalog.dropTempView("people_temp")
    print("Dropped temp view 'people_temp'")


def demonstrate_global_temp_view(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Global Temporary Views")
    print("=" * 60)

    data = [(1, "product_a", 19.99), (2, "product_b", 29.99)]
    df = spark.createDataFrame(data, ["id", "name", "price"])

    df.createGlobalTempView("products_global")

    # Global temp views live in the 'global_temp' database
    print("\nQuery global temp view (requires 'global_temp' prefix):")
    spark.sql("SELECT * FROM global_temp.products_global").show()

    print("Tables in global_temp database:")
    spark.sql("SHOW TABLES IN global_temp").show(truncate=False)

    spark.catalog.dropGlobalTempView("products_global")
    print("Dropped global temp view 'products_global'")


def demonstrate_cache(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: DataFrame Caching")
    print("=" * 60)

    data = [(i, f"item_{i}", float(i * 10)) for i in range(1, 6)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    df.createOrReplaceTempView("items_view")

    print(
        "\nBefore caching - is 'items_view' cached?",
        spark.catalog.isCached("items_view"),
    )

    spark.catalog.cacheTable("items_view")
    print(
        "After caching  - is 'items_view' cached?", spark.catalog.isCached("items_view")
    )

    print("\nCached table query:")
    spark.sql("SELECT * FROM items_view").show()

    spark.catalog.uncacheTable("items_view")
    print(
        "After uncaching - is 'items_view' cached?",
        spark.catalog.isCached("items_view"),
    )

    spark.catalog.dropTempView("items_view")
    print("Dropped temp view 'items_view'")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark version:", spark.version)
    print(
        "Catalog implementation:",
        spark.conf.get("spark.sql.catalogImplementation", "in-memory"),
    )

    spark.sql("SHOW CATALOGS").show(truncate=False)

    demonstrate_temp_view(spark)
    demonstrate_global_temp_view(spark)
    demonstrate_cache(spark)

    print("\n--- All non-persistent catalog demos complete ---")
    spark.stop()


if __name__ == "__main__":
    main()

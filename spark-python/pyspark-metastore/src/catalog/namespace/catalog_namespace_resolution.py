import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: F401


def create_spark_session() -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "local[*]")
    warehouse = os.environ.get("SPARK_WAREHOUSE", "spark-warehouse")
    return (
        SparkSession.builder.appName("CatalogNamespaceResolution")
        .master(master)
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def demonstrate_three_level_namespace(spark: SparkSession) -> None:
    print("=" * 60)
    print("DEMO: Three-Level Namespace (catalog.database.table)")
    print("=" * 60)

    catalog = spark.catalog.currentCatalog()
    spark.sql("CREATE DATABASE IF NOT EXISTS ns_demo_db")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ns_demo_db.ns_demo_table (
            id INT, name STRING
        )
    """)
    spark.sql("INSERT INTO ns_demo_db.ns_demo_table VALUES (1, 'Alice'), (2, 'Bob')")

    # Three-level: catalog.database.table
    print(f"\nFully qualified ({catalog}.ns_demo_db.ns_demo_table):")
    spark.sql(f"SELECT * FROM {catalog}.ns_demo_db.ns_demo_table").show()

    # Two-level: database.table (uses current catalog)
    print("Two-level (ns_demo_db.ns_demo_table):")
    spark.sql("SELECT * FROM ns_demo_db.ns_demo_table").show()

    # One-level: table (uses current catalog + current database)
    spark.sql("USE ns_demo_db")
    print("One-level after USE ns_demo_db (ns_demo_table):")
    spark.sql("SELECT * FROM ns_demo_table").show()

    spark.sql("USE default")
    spark.sql("DROP TABLE IF EXISTS ns_demo_db.ns_demo_table")
    spark.sql("DROP DATABASE IF EXISTS ns_demo_db CASCADE")
    print("Cleaned up ns_demo_db")


def demonstrate_context_switching(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Context Switching (USE CATALOG / USE DATABASE)")
    print("=" * 60)

    spark.sql("CREATE DATABASE IF NOT EXISTS ctx_db_a")
    spark.sql("CREATE DATABASE IF NOT EXISTS ctx_db_b")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ctx_db_a.users (id INT, name STRING)
    """)
    spark.sql("INSERT INTO ctx_db_a.users VALUES (1, 'User_A')")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ctx_db_b.users (id INT, name STRING)
    """)
    spark.sql("INSERT INTO ctx_db_b.users VALUES (2, 'User_B')")

    print(f"\nCurrent catalog: {spark.catalog.currentCatalog()}")
    print(f"Current database: {spark.catalog.currentDatabase()}")

    spark.sql("USE ctx_db_a")
    print("\nAfter USE ctx_db_a — 'SELECT * FROM users' resolves to ctx_db_a.users:")
    spark.sql("SELECT * FROM users").show()

    spark.sql("USE ctx_db_b")
    print("After USE ctx_db_b — 'SELECT * FROM users' resolves to ctx_db_b.users:")
    spark.sql("SELECT * FROM users").show()

    spark.sql("USE default")
    spark.sql("DROP TABLE IF EXISTS ctx_db_a.users")
    spark.sql("DROP TABLE IF EXISTS ctx_db_b.users")
    spark.sql("DROP DATABASE IF EXISTS ctx_db_a CASCADE")
    spark.sql("DROP DATABASE IF EXISTS ctx_db_b CASCADE")
    print("Cleaned up ctx_db_a and ctx_db_b")


def demonstrate_cross_catalog_reference(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Cross-Catalog Reference")
    print("=" * 60)

    # With the default in-memory catalog, there's only one catalog (spark_catalog).
    # Cross-catalog references work when multiple catalogs are configured.
    catalog = spark.catalog.currentCatalog()
    spark.sql("CREATE DATABASE IF NOT EXISTS cross_ref_db")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS cross_ref_db.items (id INT, name STRING)
    """)
    spark.sql("INSERT INTO cross_ref_db.items VALUES (1, 'Item_X')")

    print(f"\nReferencing table with explicit catalog '{catalog}':")
    spark.sql(f"SELECT * FROM {catalog}.cross_ref_db.items").show()

    print("Available catalogs:")
    spark.sql("SHOW CATALOGS").show(truncate=False)

    spark.sql("DROP TABLE IF EXISTS cross_ref_db.items")
    spark.sql("DROP DATABASE IF EXISTS cross_ref_db CASCADE")
    print("Cleaned up cross_ref_db")


def demonstrate_listing_operations(spark: SparkSession) -> None:
    print("\n" + "=" * 60)
    print("DEMO: Listing Operations")
    print("=" * 60)

    catalog = spark.catalog.currentCatalog()

    spark.sql("CREATE DATABASE IF NOT EXISTS list_demo_db")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS list_demo_db.table_alpha (id INT)
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS list_demo_db.table_beta (id INT)
    """)

    print("\nSHOW CATALOGS:")
    spark.sql("SHOW CATALOGS").show(truncate=False)

    print(f"SHOW DATABASES IN {catalog}:")
    spark.sql(f"SHOW DATABASES IN {catalog}").show(truncate=False)

    print("SHOW TABLES IN list_demo_db:")
    spark.sql("SHOW TABLES IN list_demo_db").show(truncate=False)

    spark.sql("DROP TABLE IF EXISTS list_demo_db.table_alpha")
    spark.sql("DROP TABLE IF EXISTS list_demo_db.table_beta")
    spark.sql("DROP DATABASE IF EXISTS list_demo_db CASCADE")
    print("Cleaned up list_demo_db")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark version:", spark.version)
    print("Current catalog:", spark.catalog.currentCatalog())
    print("Current database:", spark.catalog.currentDatabase())

    demonstrate_three_level_namespace(spark)
    demonstrate_context_switching(spark)
    demonstrate_cross_catalog_reference(spark)
    demonstrate_listing_operations(spark)

    print("\n--- All namespace resolution demos complete ---")
    spark.stop()


if __name__ == "__main__":
    main()

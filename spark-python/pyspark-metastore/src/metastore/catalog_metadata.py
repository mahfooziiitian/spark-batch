from typing import Optional

from pyspark.sql import SparkSession


def print_catalog_details(spark: SparkSession, catalog_name: str) -> None:
    try:
        catalogs = spark.catalog.listCatalogs()
        catalog_names = [catalog.name for catalog in catalogs]

        if catalog_name not in catalog_names:
            print(
                f"Catalog '{catalog_name}' does not exist. Available catalogs: {catalog_names}"
            )
            return

        spark.sql(f"USE CATALOG {catalog_name}")

        databases = spark.catalog.listDatabases()
        print(f"Databases in catalog '{catalog_name}':")
        for db in databases:
            print(f" - {db.name}")

            tables = spark.catalog.listTables(db.name)
            print(f"   Tables in database '{db.name}':")
            for table in tables:
                print(f"     - {table.name} (Type: {table.tableType})")

    except Exception as e:
        print(f"An error occurred: {e}")


def print_catalog_metadata(spark: SparkSession) -> dict[str, str]:
    catalog_details: dict[str, str] = {}
    catalog_details["defaultCatalog"] = spark.conf.get("spark.sql.defaultCatalog")
    catalog_details["currentCatalog"] = spark.catalog.currentCatalog()
    catalog_details["catalogImplementation"] = spark.conf.get(
        "spark.sql.catalogImplementation"
    )
    catalog_details["spark_catalog"] = spark.conf.get(
        "spark.sql.catalog.spark_catalog", "N/A"
    )
    return catalog_details


def get_catalog_summary(spark: SparkSession) -> dict:
    summary: dict = {}

    catalogs = spark.catalog.listCatalogs()
    summary["catalog_names"] = [c.name for c in catalogs]
    summary["current_catalog"] = spark.catalog.currentCatalog()
    summary["default_catalog"] = spark.conf.get(
        "spark.sql.defaultCatalog", "spark_catalog"
    )
    summary["catalog_implementation"] = spark.conf.get(
        "spark.sql.catalogImplementation", "in-memory"
    )

    databases_per_catalog: dict[str, list[str]] = {}
    tables_per_database: dict[str, int] = {}

    original_catalog = spark.catalog.currentCatalog()
    for catalog in summary["catalog_names"]:
        spark.sql(f"USE CATALOG {catalog}")
        dbs = spark.catalog.listDatabases()
        db_names = [db.name for db in dbs]
        databases_per_catalog[catalog] = db_names

        for db_name in db_names:
            key = f"{catalog}.{db_name}"
            tables = spark.catalog.listTables(db_name)
            tables_per_database[key] = len(tables)

    # Restore original catalog context
    spark.sql(f"USE CATALOG {original_catalog}")

    summary["databases_per_catalog"] = databases_per_catalog
    summary["tables_per_database"] = tables_per_database
    return summary


def list_all_tables(
    spark: SparkSession, catalog: Optional[str] = None
) -> list[dict[str, str]]:
    all_tables: list[dict[str, str]] = []
    original_catalog = spark.catalog.currentCatalog()

    catalogs_to_scan = (
        [catalog] if catalog else [c.name for c in spark.catalog.listCatalogs()]
    )

    for cat in catalogs_to_scan:
        spark.sql(f"USE CATALOG {cat}")
        for db in spark.catalog.listDatabases():
            for table in spark.catalog.listTables(db.name):
                all_tables.append(
                    {
                        "catalog": cat,
                        "database": db.name,
                        "table": table.name,
                        "type": table.tableType,
                        "isTemporary": str(table.isTemporary),
                    }
                )

    spark.sql(f"USE CATALOG {original_catalog}")
    return all_tables

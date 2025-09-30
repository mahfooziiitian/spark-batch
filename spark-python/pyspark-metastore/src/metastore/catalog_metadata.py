from pyspark.sql import SparkSession


def print_catalog_details(spark: SparkSession, catalog_name: str):
    """
    Print the details of the specified catalog.

    Args:
        catalog_name (str): The name of the catalog to print details for.
    """
    try:
        # List all catalogs
        catalogs = spark.catalog.listCatalogs()
        catalog_names = [catalog.name for catalog in catalogs]

        if catalog_name not in catalog_names:
            print(
                f"Catalog '{catalog_name}' does not exist. Available catalogs: {catalog_names}"
            )
            return

        # Set the current catalog
        spark.sql(f"USE CATALOG {catalog_name}")

        # List all databases in the specified catalog
        databases = spark.catalog.listDatabases()
        print(f"Databases in catalog '{catalog_name}':")
        for db in databases:
            print(f" - {db.name}")

            # List all tables in each database
            tables = spark.catalog.listTables(db.name)
            print(f"   Tables in database '{db.name}':")
            for table in tables:
                print(f"     - {table.name} (Type: {table.tableType})")

    except Exception as e:
        print(f"An error occurred: {e}")


def print_catalog_metadata(spark: SparkSession):
    """
    Print the metadata of the specified catalog.

    Args:
        spark (SparkSession): The Spark session.
        catalog_name (str): The name of the catalog to print metadata for.
    """
    catalog_details = {}
    catalog_details["defaultCatalog"] = spark.conf.get("spark.sql.defaultCatalog")
    catalog_details["currentCatalog"] = spark.catalog.currentCatalog()
    catalog_details["catalogImplementation"] = spark.conf.get(
        "spark.sql.catalogImplementation"
    )
    catalog_details["spark_catalog"] = spark.conf.get("spark.sql.catalog.spark_catalog")
    return catalog_details

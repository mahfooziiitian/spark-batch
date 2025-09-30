import os

from pyspark.sql import SparkSession

from metastore.catalog_metadata import print_catalog_metadata

# Set JAVA_HOME environment variable
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")

# Initialize SparkSession with Hive support and custom configurations
spark = (
    SparkSession.builder.appName("EnhancedCatalogDemo")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)


def show_catalogs(spark):
    print("Available catalogs:")
    spark.sql("SHOW CATALOGS").show(truncate=False)


def show_databases(spark, catalog=""):
    if catalog:
        print(f"Databases in catalog '{catalog}':")
        spark.sql(f"SHOW DATABASES IN {catalog}").show(truncate=False)
    else:
        print("Databases in default catalog:")
        spark.sql("SHOW DATABASES").show(truncate=False)


def show_tables(spark, database=""):
    if database:
        print(f"Tables in database '{database}':")
        spark.sql(f"SHOW TABLES IN {database}").show(truncate=False)
    else:
        print("Tables in current database:")
        spark.sql("SHOW TABLES").show(truncate=False)


def main():
    show_catalogs(spark)
    default_catalog = spark.conf.get("spark.sql.defaultCatalog")
    show_databases(spark, default_catalog)
    show_tables(spark)
    print("Catalog metadata:")
    print(print_catalog_metadata(spark))
    print(f"Default catalog: {default_catalog}")


if __name__ == "__main__":
    main()
    spark.stop()

import os

from pyspark.sql import SparkSession

from metastore.catalog_metadata import print_catalog_metadata

# Set JAVA_HOME environment variable
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")
warehouse_location = os.environ.get("SPARK_WAREHOUSE", "spark-warehouse")

# Initialize SparkSession with Hive support and custom configurations
spark = (
    SparkSession.builder.appName("EnhancedCatalogDemo")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", warehouse_location)
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

def drop_table_if_exists(spark, table_name):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

def create_sample_table(spark):
    # Create a sample DataFrame
    data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
    columns = ["id", "name"]
    df = spark.createDataFrame(data, columns)

    # Write the DataFrame to a table in the default catalog and database
    df.write.saveAsTable("my_table")  # Saves to default catalog and current database

def main():
    show_catalogs(spark)
    default_catalog = spark.conf.get("spark.sql.defaultCatalog")
    show_databases(spark, default_catalog)
    show_tables(spark)
    print("Catalog metadata:")
    print(print_catalog_metadata(spark))
    print(f"Default catalog: {default_catalog}")

    create_sample_table(spark)

    spark.sql("select * from spark_catalog.default.my_table").show()

    drop_table_if_exists(spark, "my_table")



if __name__ == "__main__":
    main()
    spark.stop()

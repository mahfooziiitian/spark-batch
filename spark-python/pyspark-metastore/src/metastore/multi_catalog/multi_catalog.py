from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder.appName("MultiCatalog")
        # Hive Catalog
        .config("spark.sql.catalog.hive", "org.apache.spark.sql.hive.HiveCatalog")
        .config("spark.sql.catalog.hive.uri", "thrift://hive-metastore:9083")
        # Iceberg Catalog
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://my-bucket/iceberg")
        # JDBC Catalog
        .config(
            "spark.sql.catalog.postgres",
            "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
        )
        .config("spark.sql.catalog.postgres.url", "jdbc:postgresql://db:5432/mydb")
        .getOrCreate()
    )


def query_catalog(spark, catalog, query):
    spark.sql(f"USE CATALOG {catalog}")
    return spark.sql(query)


if __name__ == "__main__":
    spark = create_spark_session()

    # Query Hive Catalog
    hive_df = query_catalog(spark, "hive", "SELECT * FROM sales_db.transactions")
    hive_df.show()

    # Query Iceberg Catalog
    iceberg_df = query_catalog(spark, "iceberg", "SELECT * FROM analytics.events")
    iceberg_df.show()

    # Example: Query JDBC Catalog (Postgres)
    postgres_df = query_catalog(spark, "postgres", "SELECT * FROM public.users")
    postgres_df.show()

    spark.stop()

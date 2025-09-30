from pyspark.sql import SparkSession


def enable_unity_catalog(spark, workspace_url):
    spark.conf.set("spark.databricks.unityCatalog.enabled", "true")
    spark.conf.set("spark.databricks.unityCatalog.workspaceUrl", workspace_url)


def set_catalog_and_schema(spark, catalog_name, schema_name):
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_name}")


def query_table(spark, table_name):
    df = spark.sql(f"SELECT * FROM {table_name}")
    df.show()
    df.printSchema()
    return df


def main():
    spark = SparkSession.builder.getOrCreate()
    workspace_url = "https://your-workspace.cloud.databricks.com"
    catalog_name = "main"
    schema_name = "analytics"
    table_name = "events"

    enable_unity_catalog(spark, workspace_url)
    set_catalog_and_schema(spark, catalog_name, schema_name)
    query_table(spark, table_name)


if __name__ == "__main__":
    main()

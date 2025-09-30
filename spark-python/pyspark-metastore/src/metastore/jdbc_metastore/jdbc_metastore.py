import os

from pyspark.sql import SparkSession

# Load credentials from environment variables for better security
jdbc_user = os.getenv("JDBC_USER", "username")
jdbc_password = os.getenv("JDBC_PASSWORD", "password")
jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/metastore")

# Initialize SparkSession with JDBC Metastore configuration
spark = (
    SparkSession.builder.appName("JDBCMetastore")
    .config(
        "spark.sql.catalog.jdbc",
        "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
    )
    .config("spark.sql.catalog.jdbc.url", jdbc_url)
    .config("spark.sql.catalog.jdbc.driver", "org.postgresql.Driver")
    .config("spark.sql.catalog.jdbc.user", jdbc_user)
    .config("spark.sql.catalog.jdbc.password", jdbc_password)
    .getOrCreate()
)


def list_tables(catalog="jdbc", schema="default"):
    """List available tables in the specified JDBC catalog and schema."""
    query = f"SHOW TABLES IN {catalog}.{schema}"
    tables = spark.sql(query)
    tables.show(truncate=False)


def describe_table(table_name, catalog="jdbc", schema="default"):
    """Describe the schema of a table."""
    query = f"DESCRIBE TABLE {catalog}.{schema}.{table_name}"
    desc = spark.sql(query)
    desc.show(truncate=False)


if __name__ == "__main__":
    list_tables()
    describe_table("my_table")

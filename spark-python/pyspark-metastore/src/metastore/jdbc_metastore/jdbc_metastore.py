import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

JDBC_URL = os.environ.get("JDBC_URL", "jdbc:postgresql://localhost:5432/metastore")
JDBC_DRIVER = os.environ.get("JDBC_DRIVER", "org.postgresql.Driver")
JDBC_USER = os.environ.get("JDBC_USER", "username")
JDBC_PASSWORD = os.environ.get("JDBC_PASSWORD", "password")
JDBC_SCHEMA = os.environ.get("JDBC_SCHEMA", "public")
JDBC_FETCH_SIZE = os.environ.get("JDBC_FETCH_SIZE", "1000")
JDBC_BATCH_SIZE = os.environ.get("JDBC_BATCH_SIZE", "5000")
JDBC_NUM_PARTITIONS = os.environ.get("JDBC_NUM_PARTITIONS", "10")


def create_spark_session():
    return (
        SparkSession.builder.appName("JDBCMetastore")
        .config(
            "spark.sql.catalog.jdbc",
            "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
        )
        .config("spark.sql.catalog.jdbc.url", JDBC_URL)
        .config("spark.sql.catalog.jdbc.driver", JDBC_DRIVER)
        .config("spark.sql.catalog.jdbc.user", JDBC_USER)
        .config("spark.sql.catalog.jdbc.password", JDBC_PASSWORD)
        .getOrCreate()
    )


def list_tables(spark, catalog="jdbc", schema="default"):
    """List available tables in the specified JDBC catalog and schema."""
    query = f"SHOW TABLES IN {catalog}.{schema}"
    tables = spark.sql(query)
    tables.show(truncate=False)


def describe_table(spark, table_name, catalog="jdbc", schema="default"):
    """Describe the schema of a table."""
    query = f"DESCRIBE TABLE {catalog}.{schema}.{table_name}"
    desc = spark.sql(query)
    desc.show(truncate=False)


def demonstrate_jdbc_catalog_browse(spark):
    print("=== JDBC Catalog Browse ===")

    print("\n-- Namespaces in JDBC catalog --")
    spark.sql("SHOW NAMESPACES IN jdbc").show(truncate=False)

    print(f"\n-- Tables in jdbc.{JDBC_SCHEMA} --")
    spark.sql(f"SHOW TABLES IN jdbc.{JDBC_SCHEMA}").show(truncate=False)

    print("\n-- Describe first available table --")
    tables_df = spark.sql(f"SHOW TABLES IN jdbc.{JDBC_SCHEMA}")
    first_table = tables_df.first()
    if first_table:
        table_name = first_table["tableName"]
        spark.sql(f"DESCRIBE TABLE jdbc.{JDBC_SCHEMA}.{table_name}").show(
            truncate=False
        )
    else:
        print("No tables found to describe.")


def demonstrate_jdbc_read(spark):
    print("=== JDBC Read with Pushdown ===")

    connection_properties = {
        "user": JDBC_USER,
        "password": JDBC_PASSWORD,
        "driver": JDBC_DRIVER,
        "fetchsize": JDBC_FETCH_SIZE,
    }

    print("\n-- Read with filter pushdown --")
    df_filtered = (
        spark.read.jdbc(
            JDBC_URL, f"{JDBC_SCHEMA}.orders", properties=connection_properties
        )
        .filter(F.col("amount") > 100)
        .select("order_id", "customer_id", "amount")
    )
    print("Physical plan showing filter pushdown:")
    df_filtered.explain(True)
    df_filtered.show(10)

    print("\n-- Read with aggregation pushdown --")
    df_agg = (
        spark.read.jdbc(
            JDBC_URL, f"{JDBC_SCHEMA}.orders", properties=connection_properties
        )
        .groupBy("customer_id")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("order_id").alias("order_count"),
        )
    )
    print("Physical plan showing aggregation pushdown:")
    df_agg.explain(True)
    df_agg.show(10)


def demonstrate_jdbc_write(spark):
    print("=== JDBC Write ===")

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True),
        ]
    )
    data = [
        (1, "Alice", "Engineering", 95000),
        (2, "Bob", "Marketing", 78000),
        (3, "Carol", "Engineering", 102000),
        (4, "Dave", "Sales", 67000),
    ]
    df = spark.createDataFrame(data, schema)

    connection_properties = {
        "user": JDBC_USER,
        "password": JDBC_PASSWORD,
        "driver": JDBC_DRIVER,
        "batchsize": JDBC_BATCH_SIZE,
    }

    print("\n-- Write with overwrite mode --")
    df.write.jdbc(
        JDBC_URL,
        f"{JDBC_SCHEMA}.employees_staging",
        mode="overwrite",
        properties=connection_properties,
    )
    print(f"Wrote {df.count()} rows in overwrite mode.")

    print("\n-- Write with append mode --")
    new_rows = spark.createDataFrame([(5, "Eve", "Engineering", 88000)], schema)
    new_rows.write.jdbc(
        JDBC_URL,
        f"{JDBC_SCHEMA}.employees_staging",
        mode="append",
        properties=connection_properties,
    )
    print("Appended 1 row.")

    print("\n-- Verify written data --")
    spark.read.jdbc(
        JDBC_URL, f"{JDBC_SCHEMA}.employees_staging", properties=connection_properties
    ).show()


def demonstrate_jdbc_options(spark):
    print("=== JDBC Advanced Options ===")

    connection_properties = {
        "user": JDBC_USER,
        "password": JDBC_PASSWORD,
        "driver": JDBC_DRIVER,
        "fetchsize": JDBC_FETCH_SIZE,
    }

    print(f"\n-- Partitioned read (numPartitions={JDBC_NUM_PARTITIONS}) --")
    df_partitioned = spark.read.jdbc(
        url=JDBC_URL,
        table=f"{JDBC_SCHEMA}.orders",
        column="order_id",
        lowerBound=1,
        upperBound=100000,
        numPartitions=int(JDBC_NUM_PARTITIONS),
        properties=connection_properties,
    )
    print(f"Number of partitions: {df_partitioned.rdd.getNumPartitions()}")
    df_partitioned.explain(True)

    print("\n-- Custom SQL subquery as source --")
    subquery = f"""(
        SELECT customer_id, SUM(amount) AS total_spent
        FROM {JDBC_SCHEMA}.orders
        GROUP BY customer_id
        HAVING SUM(amount) > 500
    ) AS high_value_customers"""

    df_subquery = spark.read.jdbc(JDBC_URL, subquery, properties=connection_properties)
    df_subquery.show(10)
    df_subquery.explain(True)

    print("\n-- Active JDBC options --")
    print(f"  fetchsize:     {JDBC_FETCH_SIZE}")
    print(f"  batchsize:     {JDBC_BATCH_SIZE}")
    print(f"  numPartitions: {JDBC_NUM_PARTITIONS}")
    print(f"  driver:        {JDBC_DRIVER}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        demonstrate_jdbc_catalog_browse(spark)
        demonstrate_jdbc_read(spark)
        demonstrate_jdbc_write(spark)
        demonstrate_jdbc_options(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

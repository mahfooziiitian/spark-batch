import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(app_name="DeltaCatalog"):
    warehouse = os.environ.get("DELTA_WAREHOUSE", "s3://my-bucket/delta")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "")
    s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.databricks.delta.catalog.enabled", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
    )

    if s3_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    if s3_access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", s3_access_key)
    if s3_secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession created successfully with Delta Lake support.")
    return spark


def demonstrate_delta_table_lifecycle(spark):
    print("\n=== Delta Table Lifecycle ===")

    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.customers (
            id INT,
            name STRING,
            email STRING,
            signup_date DATE
        ) USING DELTA
    """)

    spark.sql("""
        INSERT INTO demo.customers VALUES
        (1, 'Alice', 'alice@example.com', DATE '2024-01-10'),
        (2, 'Bob', 'bob@example.com', DATE '2024-02-15'),
        (3, 'Charlie', 'charlie@example.com', DATE '2024-03-20')
    """)
    print("Initial data:")
    spark.sql("SELECT * FROM demo.customers ORDER BY id").show()

    spark.sql("""
        UPDATE demo.customers SET email = 'alice.new@example.com' WHERE name = 'Alice'
    """)
    print("After UPDATE (Alice email):")
    spark.sql("SELECT * FROM demo.customers ORDER BY id").show()

    spark.sql("DELETE FROM demo.customers WHERE name = 'Charlie'")
    print("After DELETE (Charlie):")
    spark.sql("SELECT * FROM demo.customers ORDER BY id").show()

    print("MERGE INTO (upsert):")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW updates AS
        SELECT * FROM VALUES
            (2, 'Bob', 'bob.updated@example.com', DATE '2024-02-15'),
            (4, 'Diana', 'diana@example.com', DATE '2024-04-01')
        AS updates(id, name, email, signup_date)
    """)
    spark.sql("""
        MERGE INTO demo.customers AS target
        USING updates AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql("SELECT * FROM demo.customers ORDER BY id").show()


def demonstrate_time_travel(spark):
    print("\n=== Delta Time Travel ===")

    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.orders (
            id INT,
            product STRING,
            amount DOUBLE
        ) USING DELTA
    """)

    spark.sql("INSERT INTO demo.orders VALUES (1, 'Widget', 10.0)")
    spark.sql("INSERT INTO demo.orders VALUES (2, 'Gadget', 25.0)")
    spark.sql("INSERT INTO demo.orders VALUES (3, 'Doohickey', 15.0)")

    print("Table history:")
    spark.sql("DESCRIBE HISTORY demo.orders").show(truncate=False)

    print("Data at version 0:")
    spark.sql("SELECT * FROM demo.orders VERSION AS OF 0 ORDER BY id").show()

    print("Data at version 1:")
    spark.sql("SELECT * FROM demo.orders VERSION AS OF 1 ORDER BY id").show()

    print("Current data:")
    spark.sql("SELECT * FROM demo.orders ORDER BY id").show()

    history = spark.sql("DESCRIBE HISTORY demo.orders")
    ts_row = history.filter(F.col("version") == 0).select("timestamp").first()
    if ts_row:
        ts_val = ts_row[0].strftime("%Y-%m-%d %H:%M:%S")
        print(f"Data at timestamp '{ts_val}' (version 0):")
        spark.sql(
            f"SELECT * FROM demo.orders TIMESTAMP AS OF '{ts_val}' ORDER BY id"
        ).show()


def demonstrate_schema_evolution(spark):
    print("\n=== Delta Schema Evolution ===")

    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.inventory (
            id INT,
            item STRING,
            quantity INT
        ) USING DELTA
    """)

    spark.sql("INSERT INTO demo.inventory VALUES (1, 'Bolt', 500)")

    spark.sql(
        "ALTER TABLE demo.inventory ADD COLUMNS (warehouse STRING, weight DOUBLE)"
    )
    print("After ADD COLUMNS:")
    spark.sql("DESCRIBE TABLE demo.inventory").show(truncate=False)

    new_data = spark.createDataFrame(
        [(2, "Nut", 1000, "WH-A", 0.02, "kg")],
        ["id", "item", "quantity", "warehouse", "weight", "unit"],
    )
    new_data.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable("demo.inventory")

    print("Data after schema merge:")
    spark.sql("SELECT * FROM demo.inventory ORDER BY id").show()
    spark.sql("DESCRIBE TABLE demo.inventory").show(truncate=False)


def demonstrate_optimization(spark):
    print("\n=== Delta Optimization ===")

    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.sales (
            id INT,
            region STRING,
            product STRING,
            amount DOUBLE,
            sale_date DATE
        ) USING DELTA
    """)

    for i in range(5):
        spark.sql(f"""
            INSERT INTO demo.sales VALUES
            ({i * 2}, 'US', 'Widget', {10.0 + i}, DATE '2024-01-{10 + i:02d}'),
            ({i * 2 + 1}, 'EU', 'Gadget', {20.0 + i}, DATE '2024-02-{10 + i:02d}')
        """)

    print("OPTIMIZE with Z-ORDER:")
    spark.sql("OPTIMIZE demo.sales ZORDER BY (region, product)")

    print("VACUUM (retain 0 hours for demo):")
    spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
    spark.sql("VACUUM demo.sales RETAIN 0 HOURS")

    print("Table details:")
    spark.sql("DESCRIBE DETAIL demo.sales").show(truncate=False)


def demonstrate_cdf(spark):
    print("\n=== Delta Change Data Feed ===")

    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.accounts (
            id INT,
            name STRING,
            balance DOUBLE
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    spark.sql("""
        INSERT INTO demo.accounts VALUES
        (1, 'Alice', 1000.0),
        (2, 'Bob', 2000.0)
    """)
    spark.sql("UPDATE demo.accounts SET balance = 1500.0 WHERE name = 'Alice'")
    spark.sql("DELETE FROM demo.accounts WHERE name = 'Bob'")

    print("Change Data Feed (from version 0):")
    changes = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table("demo.accounts")
    )
    changes.orderBy("id", "_commit_version").show(truncate=False)


def main():
    spark = create_spark_session()

    try:
        print("\n=== Catalog Information ===")
        spark.sql("SHOW CATALOGS").show()
        spark.sql("SHOW DATABASES").show()
        spark.sql("SHOW TABLES").show()

        demonstrate_delta_table_lifecycle(spark)
        demonstrate_time_travel(spark)
        demonstrate_schema_evolution(spark)
        demonstrate_optimization(spark)
        demonstrate_cdf(spark)

    finally:
        spark.sql("DROP TABLE IF EXISTS demo.customers")
        spark.sql("DROP TABLE IF EXISTS demo.orders")
        spark.sql("DROP TABLE IF EXISTS demo.inventory")
        spark.sql("DROP TABLE IF EXISTS demo.sales")
        spark.sql("DROP TABLE IF EXISTS demo.accounts")
        spark.sql("DROP DATABASE IF EXISTS demo CASCADE")
        spark.stop()


if __name__ == "__main__":
    main()

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(
    jdbc_url: str,
    jdbc_driver: str,
    jdbc_user: str,
    jdbc_password: str,
) -> SparkSession:
    return (
        SparkSession.builder.appName("external-metastore")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("javax.jdo.option.ConnectionURL", jdbc_url)
        .config("javax.jdo.option.ConnectionDriverName", jdbc_driver)
        .config("javax.jdo.option.ConnectionUserName", jdbc_user)
        .config("javax.jdo.option.ConnectionPassword", jdbc_password)
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


def create_mysql_session() -> SparkSession:
    return create_spark_session(
        jdbc_url=os.environ.get(
            "METASTORE_JDBC_URL", "jdbc:mysql://localhost:3306/metastore_db"
        ),
        jdbc_driver=os.environ.get("METASTORE_JDBC_DRIVER", "com.mysql.cj.jdbc.Driver"),
        jdbc_user=os.environ.get("METASTORE_JDBC_USER", "hive"),
        jdbc_password=os.environ.get("METASTORE_JDBC_PASSWORD", "hive"),
    )


def create_postgresql_session() -> SparkSession:
    return create_spark_session(
        jdbc_url=os.environ.get(
            "METASTORE_JDBC_URL", "jdbc:postgresql://localhost:5432/metastore_db"
        ),
        jdbc_driver=os.environ.get("METASTORE_JDBC_DRIVER", "org.postgresql.Driver"),
        jdbc_user=os.environ.get("METASTORE_JDBC_USER", "hive"),
        jdbc_password=os.environ.get("METASTORE_JDBC_PASSWORD", "hive"),
    )


def verify_metastore_connection(spark: SparkSession) -> None:
    print("\n=== Metastore Connection Info ===")
    conf = spark.sparkContext.getConf()
    catalog_impl = conf.get("spark.sql.catalogImplementation", "unknown")
    jdbc_url = conf.get("javax.jdo.option.ConnectionURL", "unknown")
    jdbc_driver = conf.get("javax.jdo.option.ConnectionDriverName", "unknown")

    print(f"  Catalog implementation : {catalog_impl}")
    print(f"  JDBC URL               : {jdbc_url}")
    print(f"  JDBC Driver            : {jdbc_driver}")
    print(f"  Spark version          : {spark.version}")


def demonstrate_external_metastore(spark: SparkSession) -> None:
    print("\n=== External Metastore Demo ===")
    spark.sql("SHOW CATALOGS").show(truncate=False)
    spark.sql("SHOW DATABASES").show(truncate=False)

    spark.sql("CREATE DATABASE IF NOT EXISTS ext_demo_db")
    spark.sql("USE ext_demo_db")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT,
            name STRING,
            category STRING,
            price DOUBLE
        )
        STORED AS PARQUET
    """)

    spark.sql("""
        INSERT INTO products VALUES
        (1, 'Laptop',  'Electronics', 999.99),
        (2, 'Desk',    'Furniture',   249.50),
        (3, 'Monitor', 'Electronics', 349.99),
        (4, 'Chair',   'Furniture',   189.00)
    """)

    spark.sql("SHOW TABLES").show(truncate=False)
    spark.sql("SELECT * FROM products").show(truncate=False)

    summary = (
        spark.table("products")
        .groupBy("category")
        .agg(
            F.count("*").alias("num_products"),
            F.round(F.avg("price"), 2).alias("avg_price"),
        )
    )
    summary.show(truncate=False)


def demonstrate_persistent_metadata(spark: SparkSession) -> None:
    # Metadata stored in the external RDBMS (MySQL/PostgreSQL) persists across
    # Spark sessions. Stopping and restarting Spark with the same JDBC config
    # will show the same databases, tables, and schemas created previously.
    # This is the key advantage over the default Derby-based metastore, which
    # creates a local `metastore_db` directory and is not shareable.
    print("\n=== Persistent Metadata ===")
    spark.sql("USE ext_demo_db")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS audit_log (
            action STRING,
            ts TIMESTAMP
        )
        STORED AS PARQUET
    """)
    spark.sql("""
        INSERT INTO audit_log VALUES
        ('table_created', CURRENT_TIMESTAMP()),
        ('data_inserted', CURRENT_TIMESTAMP())
    """)

    print("Tables persisted in external metastore (visible to any session):")
    spark.sql("SHOW TABLES IN ext_demo_db").show(truncate=False)


def cleanup(spark: SparkSession) -> None:
    print("\n=== Cleanup ===")
    spark.sql("DROP TABLE IF EXISTS ext_demo_db.audit_log")
    spark.sql("DROP TABLE IF EXISTS ext_demo_db.products")
    spark.sql("DROP DATABASE IF EXISTS ext_demo_db CASCADE")
    print("Cleanup complete.")


def main() -> None:
    spark = create_mysql_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        verify_metastore_connection(spark)
        demonstrate_external_metastore(spark)
        demonstrate_persistent_metadata(spark)
    finally:
        cleanup(spark)
        spark.stop()


if __name__ == "__main__":
    main()

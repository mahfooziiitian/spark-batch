"""SQL access to community data sources.

Demonstrates registering community Python Data Sources and accessing them
through Spark SQL — any Python Data Source works seamlessly with SQL once
loaded as a temp view.

Install:
    uv add pyspark-data-sources

Key concepts:
    - Python Data Sources integrate with Spark SQL via temp views
    - Can JOIN data from different custom sources in a single SQL query
    - Aggregations, filters, and window functions all work as expected
"""

from __future__ import annotations

from pyspark_datasources import FakeDataSource

from custom_ds import create_spark_session

if __name__ == "__main__":
    spark = create_spark_session("sql-community")

    spark.dataSource.register(FakeDataSource)

    # -------------------------------------------------------------------------
    # Load fake data and register as SQL views
    # -------------------------------------------------------------------------
    df_employees = (
        spark.read.format("fake")
        .schema("name string, job string, company string, city string")
        .option("numRows", 20)
        .load()
    )
    df_employees.createOrReplaceTempView("employees")

    df_addresses = (
        spark.read.format("fake")
        .schema("name string, street_address string, city string, zipcode string")
        .option("numRows", 20)
        .load()
    )
    df_addresses.createOrReplaceTempView("addresses")

    # -------------------------------------------------------------------------
    # Run SQL queries against the synthetic data
    # -------------------------------------------------------------------------
    print("=== All employees ===")
    spark.sql("SELECT * FROM employees LIMIT 5").show(truncate=False)

    print("=== Employees by city ===")
    spark.sql("""
        SELECT city, COUNT(*) as emp_count
        FROM employees
        GROUP BY city
        ORDER BY emp_count DESC
        LIMIT 10
    """).show(truncate=False)

    print("=== Join employees and addresses ===")
    spark.sql("""
        SELECT e.name, e.job, a.street_address, a.zipcode
        FROM employees e
        JOIN addresses a ON e.name = a.name
        LIMIT 5
    """).show(truncate=False)

    spark.stop()

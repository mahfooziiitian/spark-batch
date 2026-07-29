"""Example: SQL queries and temp views in PySpark.

Demonstrates registering DataFrames as temporary views and querying
them with Spark SQL, including joins, CTEs, and aggregations.

Run:
    uv run python examples/spark_sql_queries.py
"""

import os

from pyspark.sql import SparkSession


def main() -> None:
    """Demonstrate Spark SQL with temp views, joins, and CTEs."""
    spark = (
        SparkSession.builder.appName("example-spark-sql")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Create and register employee data
    employees = spark.createDataFrame(
        [
            (1, "Alice", "E001", 95000),
            (2, "Bob", "E001", 105000),
            (3, "Charlie", "E002", 88000),
            (4, "Diana", "E002", 72000),
            (5, "Eve", "E003", 78000),
        ],
        schema=["id", "name", "dept_id", "salary"],
    )
    employees.createOrReplaceTempView("employees")

    # Create and register department data
    departments = spark.createDataFrame(
        [
            ("E001", "Engineering", "San Francisco"),
            ("E002", "Marketing", "New York"),
            ("E003", "Sales", "Chicago"),
        ],
        schema=["dept_id", "dept_name", "location"],
    )
    departments.createOrReplaceTempView("departments")

    # Basic query
    print("=== All Employees ===")
    spark.sql("SELECT * FROM employees ORDER BY salary DESC").show()

    # JOIN query
    print("=== Employees with Department Info ===")
    spark.sql("""
        SELECT e.name, e.salary, d.dept_name, d.location
        FROM employees e
        JOIN departments d ON e.dept_id = d.dept_id
        ORDER BY e.salary DESC
    """).show()

    # Aggregation with HAVING
    print("=== Departments with Avg Salary > 80k ===")
    spark.sql("""
        SELECT d.dept_name,
               COUNT(*) as headcount,
               ROUND(AVG(e.salary), 0) as avg_salary,
               MAX(e.salary) as max_salary
        FROM employees e
        JOIN departments d ON e.dept_id = d.dept_id
        GROUP BY d.dept_name
        HAVING AVG(e.salary) > 80000
    """).show()

    # CTE (Common Table Expression)
    print("=== Top Earner per Department (CTE) ===")
    spark.sql("""
        WITH ranked AS (
            SELECT e.name, e.salary, d.dept_name,
                   ROW_NUMBER() OVER (PARTITION BY e.dept_id ORDER BY e.salary DESC) as rn
            FROM employees e
            JOIN departments d ON e.dept_id = d.dept_id
        )
        SELECT name, salary, dept_name
        FROM ranked
        WHERE rn = 1
    """).show()

    spark.stop()


if __name__ == "__main__":
    main()

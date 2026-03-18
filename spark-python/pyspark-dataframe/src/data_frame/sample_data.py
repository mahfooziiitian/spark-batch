"""
Shared, typed sample datasets used across DataFrame example scripts.

Each dataset is a tuple of (data, schema) so callers can do:
    df = spark.createDataFrame(*employees())
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# HR domain — employees, departments, salary
# ---------------------------------------------------------------------------


def employee_schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("employee_name", StringType(), nullable=True),
            StructField("department_id", IntegerType(), nullable=True),
        ]
    )


def department_schema() -> StructType:
    return StructType(
        [
            StructField("department_id", IntegerType(), nullable=False),
            StructField("department_name", StringType(), nullable=True),
        ]
    )


def salary_schema() -> StructType:
    return StructType(
        [
            StructField("employee_id", IntegerType(), nullable=False),
            StructField("current_salary", DoubleType(), nullable=True),
        ]
    )


def employees() -> tuple:
    data = [
        (1, "Homer Simpson", 4),
        (2, "Ned Flanders", 1),
        (3, "Barney Gumble", 5),
        (4, "Clancy Wiggum", 3),
        (5, "Moe Syzslak", None),
        (6, "Lisa Simpson", 2),
    ]
    return data, employee_schema()


def departments() -> tuple:
    data = [
        (1, "Sales"),
        (2, "Engineering"),
        (3, "Human Resources"),
        (4, "Customer Service"),
        (5, "Research And Development"),
    ]
    return data, department_schema()


def salaries() -> tuple:
    data = [
        (1, 60000.0),
        (2, 75000.0),
        (3, 50000.0),
        (4, 82000.0),
        (6, 90000.0),
    ]
    return data, salary_schema()


# ---------------------------------------------------------------------------
# Sales domain — orders, products
# ---------------------------------------------------------------------------


def sales_schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("order_id", IntegerType(), nullable=True),
            StructField("product_id", IntegerType(), nullable=True),
            StructField("order_quantity", IntegerType(), nullable=True),
        ]
    )


def sales() -> tuple:
    data = [
        (0, 0, 0, 5),
        (1, 0, 1, 3),
        (2, 0, 2, 1),
        (3, 1, 0, 2),
        (4, 2, 0, 8),
        (5, 2, 2, 8),
    ]
    return data, sales_schema()


def product_revenue_schema() -> StructType:
    return StructType(
        [
            StructField("product", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("revenue", DoubleType(), nullable=True),
        ]
    )


def product_revenue() -> tuple:
    data = [
        ("Widget A", "Electronics", 1200.0),
        ("Widget B", "Electronics", 800.0),
        ("Widget C", "Electronics", 500.0),
        ("Gadget A", "Electronics", 300.0),
        ("Shirt A", "Apparel", 150.0),
        ("Shirt B", "Apparel", 200.0),
        ("Pants A", "Apparel", 180.0),
        ("Book A", "Books", 25.0),
        ("Book B", "Books", 35.0),
        ("Book C", "Books", 15.0),
    ]
    return data, product_revenue_schema()


# ---------------------------------------------------------------------------
# Regional revenue — used in window function and aggregation examples
# ---------------------------------------------------------------------------


def regional_revenue_schema() -> StructType:
    return StructType(
        [
            StructField("region", StringType(), nullable=True),
            StructField("month", StringType(), nullable=True),
            StructField("revenue", DoubleType(), nullable=True),
        ]
    )


def regional_revenue() -> tuple:
    data = [
        ("North", "2024-01", 100.0),
        ("North", "2024-02", 200.0),
        ("North", "2024-03", 150.0),
        ("North", "2024-04", 300.0),
        ("South", "2024-01", 80.0),
        ("South", "2024-02", 120.0),
        ("South", "2024-03", 90.0),
        ("South", "2024-04", 200.0),
        ("East", "2024-01", 60.0),
        ("East", "2024-02", 70.0),
    ]
    return data, regional_revenue_schema()


# ---------------------------------------------------------------------------
# Customer orders — used in ETL and aggregation examples
# ---------------------------------------------------------------------------


def customer_order_schema() -> StructType:
    return StructType(
        [
            StructField("order_id", IntegerType(), nullable=False),
            StructField("customer_id", IntegerType(), nullable=True),
            StructField("product", StringType(), nullable=True),
            StructField("quantity", IntegerType(), nullable=True),
            StructField("unit_price", DoubleType(), nullable=True),
            StructField("status", StringType(), nullable=True),
        ]
    )


def customer_orders() -> tuple:
    data = [
        (1001, 1, "Widget", 3, 9.99, "active"),
        (1002, 2, "Gadget", 1, 49.99, "active"),
        (1003, 1, "Widget", 5, 9.99, "active"),
        (1004, 3, "Book", 10, 14.99, "inactive"),
        (1005, 2, "Gadget", 2, 49.99, "active"),
        (1006, 4, "Widget", 7, 9.99, "active"),
        (1007, None, "Book", 3, 14.99, "active"),
    ]
    return data, customer_order_schema()


# ---------------------------------------------------------------------------
# OLAP sales — multi-dimensional dataset for rollup / cube / grouping sets
# Dimensions: region × category × year × quarter
# ---------------------------------------------------------------------------


def olap_sales_schema() -> StructType:
    return StructType(
        [
            StructField("region", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("year", StringType(), nullable=True),
            StructField("quarter", StringType(), nullable=True),
            StructField("revenue", DoubleType(), nullable=True),
        ]
    )


def olap_sales() -> tuple:
    data = [
        ("North", "Electronics", "2023", "Q1", 15000.0),
        ("North", "Electronics", "2023", "Q2", 18000.0),
        ("North", "Electronics", "2024", "Q1", 20000.0),
        ("North", "Electronics", "2024", "Q2", 22000.0),
        ("North", "Apparel", "2023", "Q1", 8000.0),
        ("North", "Apparel", "2023", "Q2", 9500.0),
        ("North", "Apparel", "2024", "Q1", 10000.0),
        ("North", "Apparel", "2024", "Q2", 11000.0),
        ("South", "Electronics", "2023", "Q1", 12000.0),
        ("South", "Electronics", "2023", "Q2", 14000.0),
        ("South", "Electronics", "2024", "Q1", 16000.0),
        ("South", "Electronics", "2024", "Q2", 17000.0),
        ("South", "Apparel", "2023", "Q1", 6000.0),
        ("South", "Apparel", "2023", "Q2", 7000.0),
        ("South", "Apparel", "2024", "Q1", 8500.0),
        ("South", "Apparel", "2024", "Q2", 9000.0),
        ("East", "Electronics", "2023", "Q1", 9000.0),
        ("East", "Electronics", "2023", "Q2", 11000.0),
        ("East", "Electronics", "2024", "Q1", 13000.0),
        ("East", "Electronics", "2024", "Q2", 14500.0),
        ("East", "Books", "2023", "Q1", 2000.0),
        ("East", "Books", "2023", "Q2", 2500.0),
        ("East", "Books", "2024", "Q1", 3000.0),
        ("East", "Books", "2024", "Q2", 3500.0),
    ]
    return data, olap_sales_schema()

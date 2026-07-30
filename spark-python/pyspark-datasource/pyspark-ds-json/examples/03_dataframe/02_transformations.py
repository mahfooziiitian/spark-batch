"""DataFrame transformations on JSON data.

Demonstrates common DataFrame operations when working with JSON-derived data:
select, filter, withColumn, aggregations, and column expressions.

Key concepts:
    - Selecting and renaming columns
    - Filtering rows with conditions
    - Adding computed columns
    - Aggregation and groupBy
    - Sorting and limiting
    - Distinct and dropDuplicates

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_success,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.transformations")


if __name__ == "__main__":
    spark = get_spark("df-transformations")

    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("department", StringType()),
            StructField("salary", DoubleType()),
            StructField("age", IntegerType()),
            StructField("city", StringType()),
        ]
    )

    data_file = DATA_HOME + "/file_data/json/df_demo/employees.json"
    write_json_lines(
        data_file,
        [
            '{"name": "Alice", "department": "Engineering", "salary": 95000, "age": 30, "city": "NYC"}',
            '{"name": "Bob", "department": "Engineering", "salary": 85000, "age": 25, "city": "LA"}',
            '{"name": "Charlie", "department": "Marketing", "salary": 75000, "age": 35, "city": "Chicago"}',
            '{"name": "Diana", "department": "Engineering", "salary": 105000, "age": 28, "city": "NYC"}',
            '{"name": "Eve", "department": "Marketing", "salary": 80000, "age": 32, "city": "Boston"}',
            '{"name": "Frank", "department": "Sales", "salary": 70000, "age": 40, "city": "Denver"}',
            '{"name": "Grace", "department": "Sales", "salary": 72000, "age": 29, "city": "Austin"}',
        ],
    )

    df = spark.read.schema(schema).json(data_file)

    # =========================================================================
    # 1. Select and rename columns
    # =========================================================================
    print_header("1. Select & Rename")

    df_select = df.select(
        F.col("name"),
        F.col("department").alias("dept"),
        F.col("salary"),
    )
    print_dataframe(df_select, title="Selected & Renamed Columns")

    # =========================================================================
    # 2. Filter rows
    # =========================================================================
    print_header("2. Filter Rows")

    df_eng = df.filter(F.col("department") == "Engineering")
    print_dataframe(df_eng, title="Engineering Only")

    df_high_salary = df.filter((F.col("salary") > 80000) & (F.col("age") < 35))
    print_dataframe(df_high_salary, title="Salary > 80k AND Age < 35")

    # =========================================================================
    # 3. Add computed columns
    # =========================================================================
    print_header("3. Computed Columns")

    df_computed = (
        df.withColumn(
            "annual_bonus",
            F.col("salary") * 0.10,
        )
        .withColumn(
            "total_comp",
            F.col("salary") + F.col("salary") * 0.10,
        )
        .withColumn(
            "name_upper",
            F.upper(F.col("name")),
        )
    )
    print_dataframe(
        df_computed.select("name", "salary", "annual_bonus", "total_comp", "name_upper"),
        title="With Computed Columns",
    )

    # =========================================================================
    # 4. Aggregations
    # =========================================================================
    print_header("4. Aggregations")

    df_agg = df.groupBy("department").agg(
        F.count("*").alias("headcount"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("salary").alias("max_salary"),
        F.min("salary").alias("min_salary"),
        F.round(F.sum("salary"), 2).alias("total_salary"),
    )
    print_dataframe(df_agg, title="Aggregation by Department")

    # =========================================================================
    # 5. Sorting
    # =========================================================================
    print_header("5. Sorting")

    df_sorted = df.orderBy(F.col("salary").desc())
    print_dataframe(df_sorted, title="Sorted by Salary (desc)")

    df_multi_sort = df.orderBy("department", F.col("salary").desc())
    print_dataframe(df_multi_sort, title="Sorted by Department, then Salary desc")

    # =========================================================================
    # 6. Distinct and drop duplicates
    # =========================================================================
    print_header("6. Distinct & Drop Duplicates")

    df_depts = df.select("department").distinct()
    print_dataframe(df_depts, title="Distinct Departments")

    df_cities = df.select("city", "department").dropDuplicates(["department"])
    print_dataframe(df_cities, title="One City per Department")

    # =========================================================================
    # 7. Limit and sampling
    # =========================================================================
    print_header("7. Limit & Sample")

    df_top3 = df.orderBy(F.col("salary").desc()).limit(3)
    print_dataframe(df_top3, title="Top 3 Earners")
    print_success("Transformations complete")

    spark.stop()

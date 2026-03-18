"""
DataFrame filter operations — all patterns for selecting a subset of rows.

`filter()` and `where()` are exact aliases — both accept either a Column
expression or a SQL string expression.

Patterns covered:
  1.  Equality / comparison          — ==, !=, >, <, >=, <=
  2.  Compound conditions            — & (AND), | (OR), ~ (NOT)
  3.  SQL string expression          — df.filter("sql expression")
  4.  F.expr() expression            — F.expr("revenue > 10000")
  5.  between()                      — inclusive range filter
  6.  isin() / ~isin()               — list membership filter
  7.  String pattern filters         — like, rlike, startswith, endswith, contains
  8.  Null / not-null filters        — isNull(), isNotNull()
  9.  where() alias                  — identical to filter()
  10. Chained filters                — multiple .filter() calls
  11. Filter on computed column      — filter on a withColumn result
  12. Filter with array_contains     — element membership in array column
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from data_frame.sample_data import customer_orders, employees, olap_sales
from data_frame.spark_utils import get_spark


# ---------------------------------------------------------------------------
# 1. Equality and comparison filters
# ---------------------------------------------------------------------------

def demo_comparison(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== 1. Equality / comparison filters ===")

    # exact match
    active = df.filter(F.col("status") == "active")
    print(f"  status == 'active'   : {active.count()} rows")

    # not equal
    not_active = df.filter(F.col("status") != "active")
    print(f"  status != 'active'   : {not_active.count()} rows")

    # greater than
    high_qty = df.filter(F.col("quantity") > 4)
    print(f"  quantity > 4         : {high_qty.count()} rows")

    # less than or equal
    cheap = df.filter(F.col("unit_price") <= 10.0)
    print(f"  unit_price <= 10.0   : {cheap.count()} rows")

    cheap.select("order_id", "product", "quantity", "unit_price").show(truncate=False)


# ---------------------------------------------------------------------------
# 2. Compound conditions — & (AND), | (OR), ~ (NOT)
# ---------------------------------------------------------------------------

def demo_compound(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== 2. Compound conditions ===")

    # AND — wrap each condition in parentheses
    and_result = df.filter(
        (F.col("status") == "active") & (F.col("quantity") >= 3)
    )
    print(f"  active AND qty >= 3       : {and_result.count()} rows")

    # OR
    or_result = df.filter(
        (F.col("product") == "Widget") | (F.col("product") == "Book")
    )
    print(f"  product is Widget OR Book : {or_result.count()} rows")

    # NOT (tilde)
    not_result = df.filter(~(F.col("status") == "active"))
    print(f"  NOT active                : {not_result.count()} rows")

    # Three-way AND
    three_way = df.filter(
        (F.col("status") == "active")
        & (F.col("unit_price") > 9.0)
        & (F.col("quantity") > 2)
    )
    print(f"  active AND price>9 AND qty>2: {three_way.count()} rows")
    three_way.show(truncate=False)


# ---------------------------------------------------------------------------
# 3. SQL string expression
# ---------------------------------------------------------------------------

def demo_sql_string(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== 3. SQL string expression ===")

    result = df.filter("status = 'active' AND quantity > 2")
    print(f"  SQL string filter         : {result.count()} rows")
    result.select("order_id", "product", "quantity", "status").show(truncate=False)

    # SQL BETWEEN in string form
    between_sql = df.filter("unit_price BETWEEN 10.0 AND 50.0")
    print(f"  SQL BETWEEN 10 AND 50     : {between_sql.count()} rows")

    # SQL IN in string form
    in_sql = df.filter("product IN ('Widget', 'Book')")
    print(f"  SQL IN (Widget, Book)     : {in_sql.count()} rows")


# ---------------------------------------------------------------------------
# 4. F.expr() expression
# ---------------------------------------------------------------------------

def demo_expr(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== 4. F.expr() expression ===")

    result = df.filter(F.expr("quantity * unit_price > 50"))
    print(f"  quantity * unit_price > 50: {result.count()} rows")
    result.select(
        "order_id", "product", "quantity", "unit_price",
        F.expr("quantity * unit_price").alias("line_total"),
    ).show(truncate=False)

    # expr with CASE WHEN (used as boolean)
    premium = df.filter(F.expr("CASE WHEN unit_price > 20 THEN true ELSE false END"))
    print(f"  unit_price > 20 via expr  : {premium.count()} rows")


# ---------------------------------------------------------------------------
# 5. between() — inclusive range
# ---------------------------------------------------------------------------

def demo_between(spark: SparkSession) -> None:
    df = spark.createDataFrame(*olap_sales())

    print("=== 5. between() range filter ===")

    mid_revenue = df.filter(F.col("revenue").between(8000.0, 15000.0))
    print(f"  revenue between 8k–15k   : {mid_revenue.count()} rows")
    mid_revenue.select("region", "category", "year", "quarter", "revenue").show(truncate=False)

    # Negated between
    outside = df.filter(~F.col("revenue").between(8000.0, 15000.0))
    print(f"  revenue outside 8k–15k   : {outside.count()} rows")


# ---------------------------------------------------------------------------
# 6. isin() / ~isin() — list membership
# ---------------------------------------------------------------------------

def demo_isin(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== 6. isin() / ~isin() ===")

    selected_products = df.filter(F.col("product").isin("Widget", "Gadget"))
    print(f"  product isin [Widget,Gadget]: {selected_products.count()} rows")
    selected_products.select("order_id", "product", "status").show(truncate=False)

    # From a Python list
    excluded = ["inactive", "cancelled"]
    active_like = df.filter(~F.col("status").isin(excluded))
    print(f"  status NOT in excluded list: {active_like.count()} rows")


# ---------------------------------------------------------------------------
# 7. String pattern filters
# ---------------------------------------------------------------------------

def demo_string_patterns(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== 7. String pattern filters ===")

    # like — SQL LIKE syntax, % = wildcard
    like_result = df.filter(F.col("product").like("Wid%"))
    print(f"  product LIKE 'Wid%%'      : {like_result.count()} rows")

    # rlike — regex
    rlike_result = df.filter(F.col("product").rlike(r"^(Widget|Gadget)$"))
    print(f"  rlike ^(Widget|Gadget)$   : {rlike_result.count()} rows")

    # contains
    contains_result = df.filter(F.col("product").contains("get"))
    print(f"  product contains 'get'    : {contains_result.count()} rows")

    # startswith / endswith
    starts = df.filter(F.col("product").startswith("G"))
    ends   = df.filter(F.col("product").endswith("t"))
    print(f"  product starts with 'G'   : {starts.count()} rows")
    print(f"  product ends with 't'     : {ends.count()} rows")

    # F.col with upper — case-insensitive match
    case_insensitive = df.filter(F.upper(F.col("product")) == "WIDGET")
    print(f"  upper(product) == WIDGET  : {case_insensitive.count()} rows")


# ---------------------------------------------------------------------------
# 8. Null / not-null filters
# ---------------------------------------------------------------------------

def demo_null_filters(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())
    emp_df = spark.createDataFrame(*employees())

    print("=== 8. Null / not-null filters ===")

    # rows where customer_id is NULL
    null_cust = df.filter(F.col("customer_id").isNull())
    print(f"  customer_id IS NULL      : {null_cust.count()} rows")
    null_cust.show(truncate=False)

    # rows where customer_id is NOT NULL
    not_null_cust = df.filter(F.col("customer_id").isNotNull())
    print(f"  customer_id IS NOT NULL  : {not_null_cust.count()} rows")

    # NULL-safe equality (treats null == null as True)
    # department_id IS NULL employees
    null_dept = emp_df.filter(F.col("department_id").isNull())
    print(f"  department_id IS NULL    : {null_dept.count()} rows")
    null_dept.show(truncate=False)


# ---------------------------------------------------------------------------
# 9. where() alias
# ---------------------------------------------------------------------------

def demo_where_alias(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== 9. where() — identical alias for filter() ===")

    result = df.where(F.col("status") == "active")
    print(f"  where(status == active)  : {result.count()} rows")

    # Chain filter() and where() together
    chained = df.where(F.col("status") == "active").filter(F.col("quantity") > 3)
    print(f"  where(active).filter(qty>3): {chained.count()} rows")
    chained.select("order_id", "product", "quantity", "status").show(truncate=False)


# ---------------------------------------------------------------------------
# 10. Chained filters — each .filter() call ANDs the conditions
# ---------------------------------------------------------------------------

def demo_chained(spark: SparkSession) -> None:
    df = spark.createDataFrame(*olap_sales())

    print("=== 10. Chained filters ===")

    result = (df
              .filter(F.col("region") == "North")
              .filter(F.col("category") == "Electronics")
              .filter(F.col("year") == "2024")
              .filter(F.col("revenue") > 19000.0))

    print(f"  North / Electronics / 2024 / revenue>19k: {result.count()} rows")
    result.show(truncate=False)


# ---------------------------------------------------------------------------
# 11. Filter on a computed / derived column
# ---------------------------------------------------------------------------

def demo_computed_column(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    print("=== 11. Filter on a derived column ===")

    # Add line_total, then filter on it
    with_total = df.withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
    high_value  = with_total.filter(F.col("line_total") > 50.0)
    print(f"  line_total > 50          : {high_value.count()} rows")
    high_value.select("order_id", "product", "quantity", "unit_price", "line_total").show(truncate=False)

    # Using F.expr inline — no intermediate withColumn needed
    inline = df.filter(F.expr("quantity * unit_price > 50"))
    print(f"  same result via expr     : {inline.count()} rows")


# ---------------------------------------------------------------------------
# 12. Filter with array_contains — array column membership
# ---------------------------------------------------------------------------

def demo_array_filter(spark: SparkSession) -> None:
    data = [
        (1, "Alice",   ["python", "spark", "scala"]),
        (2, "Bob",     ["java", "spark"]),
        (3, "Carol",   ["python", "sql"]),
        (4, "Dave",    ["scala", "sql"]),
        (5, "Eve",     ["python", "spark", "sql"]),
    ]
    schema = StructType([
        StructField("id",     StringType(), nullable=False),
        StructField("name",   StringType(), nullable=True),
        StructField("skills", ArrayType(StringType()), nullable=True),
    ])
    df = spark.createDataFrame(data, schema)

    print("=== 12. array_contains filter ===")

    spark_users = df.filter(F.array_contains(F.col("skills"), "spark"))
    print(f"  skills contains 'spark'  : {spark_users.count()} rows")
    spark_users.show(truncate=False)

    # Filter: must know both python AND spark
    python_and_spark = df.filter(
        F.array_contains(F.col("skills"), "python")
        & F.array_contains(F.col("skills"), "spark")
    )
    print(f"  python AND spark         : {python_and_spark.count()} rows")
    python_and_spark.show(truncate=False)

    # Filter: size of skills array >= 3
    multi_skill = df.filter(F.size(F.col("skills")) >= 3)
    print(f"  skills array size >= 3   : {multi_skill.count()} rows")
    multi_skill.show(truncate=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(spark: SparkSession) -> None:
    demo_comparison(spark)
    demo_compound(spark)
    demo_sql_string(spark)
    demo_expr(spark)
    demo_between(spark)
    demo_isin(spark)
    demo_string_patterns(spark)
    demo_null_filters(spark)
    demo_where_alias(spark)
    demo_chained(spark)
    demo_computed_column(spark)
    demo_array_filter(spark)


if __name__ == "__main__":
    spark = get_spark("filter-operations")
    main(spark)
    spark.stop()

"""Exploding nested arrays with parent context — preserving parent fields during explode.

Demonstrates the correct and incorrect patterns for exploding arrays while
maintaining parent-level context, including multi-level explosions, posexplode
for index tracking, and lateral view equivalents.

Key concepts:
    - Always include parent columns in select BEFORE explode
    - explode_outer preserves rows with null/empty arrays
    - posexplode gives array index for ordering/joining
    - Multi-level: carry parent fields through each level
    - Common mistake: selecting only the exploded column loses parent context

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.explode_parent_context")


if __name__ == "__main__":
    spark = get_spark("explode-parent-context")

    # =========================================================================
    # 1. The mistake — losing parent context
    # =========================================================================
    print_header("1. The Mistake — Losing Parent Context")

    data_file = DATA_HOME + "/explode_context.json"
    write_json_lines(
        data_file,
        [
            '{"customer_id": 1, "name": "Alice", "orders": [{"order_id": "O1", "amount": 100}, {"order_id": "O2", "amount": 200}]}',
            '{"customer_id": 2, "name": "Bob", "orders": [{"order_id": "O3", "amount": 150}]}',
            '{"customer_id": 3, "name": "Charlie", "orders": []}',
        ],
    )
    print_path("Input", data_file)

    schema = StructType(
        [
            StructField("customer_id", LongType(), True),
            StructField("name", StringType(), True),
            StructField(
                "orders",
                ArrayType(
                    StructType(
                        [
                            StructField("order_id", StringType(), True),
                            StructField("amount", DoubleType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

    df = spark.read.schema(schema).json(data_file)
    print_dataframe(df, title="Original data")

    # WRONG: only select the exploded column
    df_wrong = df.select(F.explode_outer("orders").alias("order"))
    print_dataframe(df_wrong, title="WRONG — lost customer_id and name!")
    print_warning("Selecting only the exploded column loses all parent fields")

    # =========================================================================
    # 2. Correct pattern — include parent columns
    # =========================================================================
    print_header("2. Correct — Include Parent Columns")

    df_correct = df.select(
        F.col("customer_id"),
        F.col("name"),
        F.explode_outer("orders").alias("order"),
    ).select(
        F.col("customer_id"),
        F.col("name"),
        F.col("order.order_id").alias("order_id"),
        F.col("order.amount").alias("amount"),
    )
    print_dataframe(df_correct, title="CORRECT — parent context preserved")
    print_success("Include parent columns in the same select as explode")

    # =========================================================================
    # 3. Single select with inline
    # =========================================================================
    print_header("3. Alternative — inline() for Struct Arrays")

    df_inline = df.select(
        "customer_id",
        "name",
        F.inline_outer(F.col("orders")),
    )
    print_dataframe(df_inline, title="inline_outer — struct fields become columns directly")
    print_success("inline_outer() expands array<struct> into columns without intermediate alias")

    # =========================================================================
    # 4. posexplode — preserve array index
    # =========================================================================
    print_header("4. posexplode — Array Index for Ordering")

    df_pos = df.select(
        "customer_id",
        "name",
        F.posexplode_outer("orders").alias("order_idx", "order"),
    ).select(
        "customer_id",
        "name",
        F.col("order_idx"),
        F.col("order.order_id").alias("order_id"),
        F.col("order.amount").alias("amount"),
    )
    print_dataframe(df_pos, title="With array position index")
    print_success("posexplode gives (index, element) — useful for ordering and joining")

    # =========================================================================
    # 5. Multi-level explosion with context
    # =========================================================================
    print_header("5. Multi-Level Explosion — Carry Context Through")

    multi_file = DATA_HOME + "/explode_context_multi.json"
    write_json_lines(
        multi_file,
        [
            '{"store": "NYC", "departments": [{"dept": "Electronics", "products": [{"sku": "TV1", "price": 999}, {"sku": "TV2", "price": 1499}]}, {"dept": "Books", "products": [{"sku": "B1", "price": 20}]}]}',
            '{"store": "LA", "departments": [{"dept": "Sports", "products": [{"sku": "S1", "price": 50}, {"sku": "S2", "price": 75}, {"sku": "S3", "price": 120}]}]}',
        ],
    )
    print_path("Input (3 levels deep)", multi_file)

    df_multi = spark.read.json(multi_file)
    print_schema(df_multi, title="Nested schema")

    # Level 1: explode departments, keep store
    df_depts = df_multi.select(
        F.col("store"),
        F.explode_outer("departments").alias("dept"),
    )

    # Level 2: explode products, keep store + dept name
    df_products = df_depts.select(
        F.col("store"),
        F.col("dept.dept").alias("department"),
        F.explode_outer("dept.products").alias("product"),
    ).select(
        F.col("store"),
        F.col("department"),
        F.col("product.sku").alias("sku"),
        F.col("product.price").alias("price"),
    )

    print_dataframe(df_products, title="Fully flattened — parent context at every level")
    logger.info("2 stores → %s product rows", df_products.count())
    print_success(
        "Carry parent fields (store, department) through each explode level"
    )

    # =========================================================================
    # 6. Aggregation after explode — back to parent level
    # =========================================================================
    print_header("6. Aggregate Back to Parent Level")

    df_agg = df_correct.groupBy("customer_id", "name").agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    print_dataframe(df_agg, title="Aggregated back to customer level")
    print_success("After explode + processing, aggregate back to parent level with groupBy")

    # =========================================================================
    # 7. Lateral join pattern (Spark 3.4+)
    # =========================================================================
    print_header("7. SQL LATERAL VIEW Pattern")

    df.createOrReplaceTempView("customers")
    df_lateral = spark.sql("""
        SELECT
            customer_id,
            name,
            order_entry.order_id,
            order_entry.amount
        FROM customers
        LATERAL VIEW OUTER explode(orders) AS order_entry
    """)
    print_dataframe(df_lateral, title="SQL LATERAL VIEW (equivalent pattern)")
    print_success("LATERAL VIEW OUTER in SQL is equivalent to select + explode_outer in DataFrame API")

    # =========================================================================
    # 8. Common patterns summary
    # =========================================================================
    print_header("8. Pattern Summary")

    patterns = [
        ("Keep parent cols", "df.select('parent_col', explode('array'))", "Always"),
        ("explode_outer", "Preserves null/empty array rows", "Default choice"),
        ("posexplode", "Gives (index, element)", "Need ordering"),
        ("inline_outer", "Expands struct fields directly", "Array<Struct>"),
        ("Multi-level", "Carry parent through each level", "Nested arrays"),
        ("Aggregate back", "groupBy(parent).agg(...)", "After processing"),
    ]
    df_patterns = spark.createDataFrame(patterns, ["Pattern", "Usage", "When"])
    print_dataframe(df_patterns, title="Explode Patterns Quick Reference")
    print_success(
        "Rule: always include parent columns alongside explode. "
        "If you can't see the parent field in the result, you lost context."
    )

    spark.stop()

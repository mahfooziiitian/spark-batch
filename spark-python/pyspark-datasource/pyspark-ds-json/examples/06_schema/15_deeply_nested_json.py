"""Deeply nested JSON — flattening arrays and structs safely.

Demonstrates strategies for reading and flattening deeply nested JSON with
multiple levels of arrays and structs, including row explosion control and
count validation after each explode.

Key concepts:
    - Nested arrays require explode/explode_outer to flatten
    - Multiple nested arrays cause combinatorial row explosion
    - Flatten one level at a time and validate row counts
    - Use explode_outer (not explode) to preserve records with empty arrays
    - DDL string schema for complex nested structures
    - getField() and dot notation for struct access

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
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
logger = get_logger("example.deeply_nested_json")


if __name__ == "__main__":
    spark = get_spark("deeply-nested-json")

    # =========================================================================
    # 1. Basic nested array flattening
    # =========================================================================
    print_header("1. Basic Nested Array Flattening")

    nested_file = DATA_HOME + "/nested_orders.json"
    write_json_lines(
        nested_file,
        [
            '{"customer_id": 101, "orders": [{"order_id": "O1", "items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}]}, {"order_id": "O2", "items": [{"sku": "C", "qty": 5}]}]}',
            '{"customer_id": 102, "orders": [{"order_id": "O3", "items": [{"sku": "D", "qty": 3}]}]}',
        ],
    )
    print_path("Input", nested_file)

    schema = StructType(
        [
            StructField("customer_id", LongType(), True),
            StructField(
                "orders",
                ArrayType(
                    StructType(
                        [
                            StructField("order_id", StringType(), True),
                            StructField(
                                "items",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField("sku", StringType(), True),
                                            StructField("qty", IntegerType(), True),
                                        ]
                                    )
                                ),
                                True,
                            ),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

    df = spark.read.schema(schema).json(nested_file)
    print_schema(df, title="Nested Schema")
    print_dataframe(df, title="Raw nested data")
    logger.info("Input row count: %s", df.count())

    # Flatten level 1: orders
    orders_df = df.select(
        F.col("customer_id"),
        F.explode_outer("orders").alias("order"),
    )
    print_dataframe(orders_df, title="After explode_outer(orders)")
    logger.info("After orders explode: %s rows", orders_df.count())

    # Flatten level 2: items
    items_df = orders_df.select(
        F.col("customer_id"),
        F.col("order.order_id").alias("order_id"),
        F.explode_outer("order.items").alias("item"),
    ).select(
        F.col("customer_id"),
        F.col("order_id"),
        F.col("item.sku").alias("sku"),
        F.col("item.qty").alias("qty"),
    )
    print_dataframe(items_df, title="Fully flattened (customer → order → item)")
    logger.info("Final row count: %s", items_df.count())
    print_success("Flatten one level at a time: 2 input rows → 4 output rows")

    # =========================================================================
    # 2. Row explosion with multiple nested arrays
    # =========================================================================
    print_header("2. Row Explosion Problem")

    explosion_file = DATA_HOME + "/nested_explosion.json"
    write_json_lines(
        explosion_file,
        [
            '{"id": 1, "orders": [{"oid": "O1", "items": [{"sku": "A", "qty": 1}, {"sku": "B", "qty": 2}], "discounts": [{"code": "D1", "pct": 10}, {"code": "D2", "pct": 5}], "taxes": [{"type": "state", "rate": 6.5}, {"type": "city", "rate": 2.0}]}]}',
        ],
    )
    print_path("Input (1 record)", explosion_file)

    df_exp = spark.read.json(explosion_file)

    # Naive approach: explode all arrays at once → cartesian product
    df_naive = (
        df_exp.select(
            "id",
            F.explode_outer("orders").alias("order"),
        )
        .select(
            "id",
            F.col("order.oid").alias("order_id"),
            F.explode_outer("order.items").alias("item"),
            F.col("order.discounts").alias("discounts"),
            F.col("order.taxes").alias("taxes"),
        )
        .select(
            "id",
            "order_id",
            F.col("item.sku").alias("sku"),
            F.explode_outer("discounts").alias("discount"),
            F.col("taxes"),
        )
        .select(
            "id",
            "order_id",
            "sku",
            F.col("discount.code").alias("discount_code"),
            F.explode_outer("taxes").alias("tax"),
        )
        .select(
            "id",
            "order_id",
            "sku",
            "discount_code",
            F.col("tax.type").alias("tax_type"),
            F.col("tax.rate").alias("tax_rate"),
        )
    )
    naive_count = df_naive.count()
    print_dataframe(df_naive, title="Naive multi-explode (cartesian product!)")
    print_warning(
        f"1 input record → {naive_count} output rows! "
        "Each explode multiplies by array length (2 items × 2 discounts × 2 taxes = 8)"
    )

    # =========================================================================
    # 3. Safe approach: flatten and join separately
    # =========================================================================
    print_header("3. Safe Approach — Flatten Separately, Join Back")

    df_orders = df_exp.select(
        "id",
        F.explode_outer("orders").alias("order"),
    ).select(
        "id",
        F.col("order.oid").alias("order_id"),
        F.col("order.items").alias("items"),
        F.col("order.discounts").alias("discounts"),
        F.col("order.taxes").alias("taxes"),
    )

    # Flatten items independently
    df_items = df_orders.select(
        "id",
        "order_id",
        F.explode_outer("items").alias("item"),
    ).select(
        "id",
        "order_id",
        F.col("item.sku").alias("sku"),
        F.col("item.qty").alias("qty"),
    )
    print_dataframe(df_items, title="Items (flattened independently)")
    logger.info("Items rows: %s", df_items.count())

    # Flatten discounts independently
    df_discounts = df_orders.select(
        "id",
        "order_id",
        F.explode_outer("discounts").alias("discount"),
    ).select(
        "id",
        "order_id",
        F.col("discount.code").alias("discount_code"),
        F.col("discount.pct").alias("discount_pct"),
    )
    print_dataframe(df_discounts, title="Discounts (flattened independently)")
    logger.info("Discounts rows: %s", df_discounts.count())

    # Flatten taxes independently
    df_taxes = df_orders.select(
        "id",
        "order_id",
        F.explode_outer("taxes").alias("tax"),
    ).select(
        "id",
        "order_id",
        F.col("tax.type").alias("tax_type"),
        F.col("tax.rate").alias("tax_rate"),
    )
    print_dataframe(df_taxes, title="Taxes (flattened independently)")
    logger.info("Taxes rows: %s", df_taxes.count())

    print_success(
        "Flatten each nested array independently to avoid cartesian products — "
        "join results by order_id when needed"
    )

    # =========================================================================
    # 4. Row count validation pattern
    # =========================================================================
    print_header("4. Row Count Validation After Each Explode")

    validation_file = DATA_HOME + "/nested_validation.json"
    write_json_lines(
        validation_file,
        [
            '{"id": 1, "tags": ["a", "b", "c"], "scores": [90, 85]}',
            '{"id": 2, "tags": ["x"], "scores": [100, 95, 88]}',
            '{"id": 3, "tags": [], "scores": [70]}',
        ],
    )

    df_val = spark.read.json(validation_file)
    input_count = df_val.count()
    logger.info("Input rows: %s", input_count)

    # Explode tags
    df_tags = df_val.select("id", F.explode_outer("tags").alias("tag"), "scores")
    tags_count = df_tags.count()
    logger.info("After explode(tags): %s rows (%.1fx expansion)", tags_count, tags_count / input_count)

    # Explode scores from tags result → cartesian!
    df_both = df_tags.select("id", "tag", F.explode_outer("scores").alias("score"))
    both_count = df_both.count()
    logger.info(
        "After explode(scores): %s rows (%.1fx total expansion)",
        both_count,
        both_count / input_count,
    )
    print_dataframe(df_both, title="Double explode result")
    print_warning(
        f"Validate after every explode: {input_count} → {tags_count} → {both_count} rows. "
        "Unexpected growth signals a cartesian product."
    )

    # =========================================================================
    # 5. Preserving nulls with explode_outer vs explode
    # =========================================================================
    print_header("5. explode_outer vs explode")

    null_array_file = DATA_HOME + "/nested_null_arrays.json"
    write_json_lines(
        null_array_file,
        [
            '{"id": 1, "items": [{"name": "A"}, {"name": "B"}]}',
            '{"id": 2, "items": null}',
            '{"id": 3, "items": []}',
        ],
    )

    df_null = spark.read.json(null_array_file)

    df_explode = df_null.select("id", F.explode("items").alias("item"))
    df_explode_outer = df_null.select("id", F.explode_outer("items").alias("item"))

    print_dataframe(df_explode, title="explode() — drops null/empty arrays")
    print_dataframe(df_explode_outer, title="explode_outer() — preserves as null row")
    logger.info("explode: %s rows, explode_outer: %s rows", df_explode.count(), df_explode_outer.count())
    print_success(
        "Use explode_outer() to preserve records with null/empty arrays — "
        "explode() silently drops them"
    )

    # =========================================================================
    # 6. DDL string for complex nested schemas
    # =========================================================================
    print_header("6. DDL String Schema for Nested Structures")

    ddl_schema = """
        customer_id BIGINT,
        orders ARRAY<STRUCT<
            order_id: STRING,
            items: ARRAY<STRUCT<
                sku: STRING,
                qty: INT
            >>
        >>
    """

    df_ddl = spark.read.schema(ddl_schema).json(nested_file)
    print_schema(df_ddl, title="Schema from DDL string")
    print_dataframe(df_ddl, title="Read with DDL schema")
    print_success("DDL strings are concise for nested schemas — good for configs and docs")

    spark.stop()

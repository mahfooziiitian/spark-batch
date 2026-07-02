import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.mark.unit
def test_simple_update(spark: SparkSession) -> None:
    customers = spark.createDataFrame(
        [
            ("c1", "Alice", 1200.0, "Silver"),
            ("c2", "Bob", 500.0, "Bronze"),
            ("c3", "Charlie", 3000.0, "Silver"),
        ],
        ["customer_id", "name", "annual_spend", "loyalty_tier"],
    )
    result = customers.withColumn(
        "loyalty_tier",
        F.when(F.col("annual_spend") > 1000, "Gold").otherwise(F.col("loyalty_tier")),
    )
    expected = spark.createDataFrame(
        [
            ("c1", "Alice", 1200.0, "Gold"),
            ("c2", "Bob", 500.0, "Bronze"),
            ("c3", "Charlie", 3000.0, "Gold"),
        ],
        ["customer_id", "name", "annual_spend", "loyalty_tier"],
    )
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_update_multiple_columns(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("status", StringType(), True),
            StructField("cancelled_at", StringType(), True),
        ]
    )
    orders = spark.createDataFrame(
        [
            ("o1", "active", None),
            ("o2", "active", None),
            ("o3", "pending", "2024-02-01"),
        ],
        schema,
    )
    to_cancel = "o1"
    result = orders.withColumn(
        "status",
        F.when(F.col("order_id") == to_cancel, "cancelled").otherwise(F.col("status")),
    ).withColumn(
        "cancelled_at",
        F.when(F.col("order_id") == to_cancel, "2024-03-01").otherwise(F.col("cancelled_at")),
    )

    row_o1 = result.filter(F.col("order_id") == "o1").first()
    row_o2 = result.filter(F.col("order_id") == "o2").first()
    assert row_o1["status"] == "cancelled"
    assert row_o1["cancelled_at"] == "2024-03-01"
    assert row_o2["status"] == "active"
    assert row_o2["cancelled_at"] is None


@pytest.mark.unit
def test_conditional_update_with_case(spark: SparkSession) -> None:
    products = spark.createDataFrame(
        [
            ("p1", "electronics", 100.0),
            ("p2", "clothing", 50.0),
            ("p3", "food", 20.0),
        ],
        ["product_id", "category", "price"],
    )
    result = products.withColumn(
        "adjusted_price",
        F.when(F.col("category") == "electronics", F.col("price") * 0.9)
        .when(F.col("category") == "clothing", F.col("price") * 1.1)
        .otherwise(F.col("price")),
    )
    rows = {r["product_id"]: r["adjusted_price"] for r in result.collect()}
    assert abs(rows["p1"] - 90.0) < 0.001  # electronics: 10% discount
    assert abs(rows["p2"] - 55.0) < 0.001  # clothing: 10% surcharge
    assert abs(rows["p3"] - 20.0) < 0.001  # food: unchanged


@pytest.mark.unit
def test_update_with_subquery_filter(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("reorder_flag", IntegerType(), True),
        ]
    )
    products = spark.createDataFrame(
        [("p1", "widget", 0), ("p2", "gadget", 0), ("p3", "gizmo", 0)],
        schema,
    )
    # Subquery: high-velocity product IDs that need the reorder flag set
    high_velocity = spark.createDataFrame([("p1",), ("p3",)], ["product_id"])

    result = (
        products.join(high_velocity.withColumn("_flag", F.lit(True)), on="product_id", how="left")
        .withColumn(
            "reorder_flag",
            F.when(F.col("_flag").isNotNull(), 1).otherwise(F.col("reorder_flag")),
        )
        .drop("_flag")
    )
    rows = {r["product_id"]: r["reorder_flag"] for r in result.collect()}
    assert rows["p1"] == 1
    assert rows["p2"] == 0
    assert rows["p3"] == 1

"""
UDFs that return StructType — creating or repackaging Row objects inside UDFs.

Patterns covered:
  1. UDF returning a StructType column (via namedtuple)
  2. UDF accepting and returning struct fields individually
  3. Struct column created with F.struct() — no UDF needed (preferred)
  4. UDF parsing a string into a struct
  5. UDF enriching a struct field
  6. Calling a StructType-returning UDF and selecting its sub-fields
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_frame.sample_data import customer_orders, product_revenue
from data_frame.spark_utils import get_spark

# ---------------------------------------------------------------------------
# 1. UDF returning StructType — use a namedtuple as the return value
# ---------------------------------------------------------------------------

_PriceTier = StructType(
    [
        StructField("tier", StringType(), nullable=True),
        StructField("multiplier", DoubleType(), nullable=True),
        StructField("label", StringType(), nullable=True),
    ]
)


def _price_tier_fn(price: float):
    """Classify a price into a tier struct."""
    if price is None:
        return None
    if price >= 1000.0:
        return ("Platinum", 1.0, f"Platinum — ${price:.2f}")
    if price >= 500.0:
        return ("Gold", 0.95, f"Gold — ${price:.2f}")
    if price >= 100.0:
        return ("Silver", 0.90, f"Silver — ${price:.2f}")
    return ("Bronze", 0.85, f"Bronze — ${price:.2f}")


def demo_udf_returns_struct(spark: SparkSession) -> None:
    price_tier_udf = F.udf(_price_tier_fn, _PriceTier)

    df = spark.createDataFrame(*product_revenue())

    result = df.withColumn("tier_info", price_tier_udf(F.col("revenue"))).select(
        "product",
        "category",
        "revenue",
        "tier_info",
        F.col("tier_info.tier").alias("tier"),
        F.col("tier_info.multiplier").alias("multiplier"),
        F.col("tier_info.label").alias("label"),
    )
    print("=== UDF returning StructType ===")
    result.printSchema()
    result.show(truncate=False)


# ---------------------------------------------------------------------------
# 2. UDF parsing a raw string into a struct
# ---------------------------------------------------------------------------

_AddressStruct = StructType(
    [
        StructField("street", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
    ]
)


def _parse_address(raw: str):
    """Parse 'street|city|country' string into a struct."""
    if not raw:
        return None
    parts = raw.split("|")
    if len(parts) != 3:
        return None
    return tuple(p.strip() for p in parts)


def demo_udf_parse_to_struct(spark: SparkSession) -> None:
    parse_address_udf = F.udf(_parse_address, _AddressStruct)

    data = [
        (1, "Alice", "Baker St 221B | London   | UK"),
        (2, "Bob", "Unter d. Linden 1 | Berlin | DE"),
        (3, "Carol", None),  # NULL → None result
        (4, "Dave", "INVALID"),  # wrong format → None
    ]
    df = spark.createDataFrame(data, ["id", "name", "raw_address"])

    result = df.withColumn("address", parse_address_udf(F.col("raw_address")))
    print("\n=== UDF parsing string → struct ===")
    result.printSchema()
    result.show(truncate=False)

    # Select sub-fields from the parsed struct
    result.select(
        "id",
        "name",
        F.col("address.city").alias("city"),
        F.col("address.country").alias("country"),
    ).show(truncate=False)


# ---------------------------------------------------------------------------
# 3. F.struct() — preferred over UDF when inputs are existing columns
# ---------------------------------------------------------------------------


def demo_f_struct_no_udf(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # Build a struct column from existing columns — no UDF, fully optimised
    result = df.withColumn(
        "line_summary",
        F.struct(
            F.col("product").alias("product"),
            (F.col("quantity") * F.col("unit_price")).alias("line_total"),
            F.when(F.col("unit_price") >= 30, "premium")
            .otherwise("standard")
            .alias("price_band"),
        ),
    )
    print("\n=== F.struct() — no UDF needed ===")
    result.select(
        "order_id",
        "line_summary",
        F.col("line_summary.product").alias("product"),
        F.round("line_summary.line_total", 2).alias("total"),
        F.col("line_summary.price_band").alias("band"),
    ).show(truncate=False)


# ---------------------------------------------------------------------------
# 4. UDF enriching an existing struct field
# ---------------------------------------------------------------------------


def demo_udf_enrich_struct(spark: SparkSession) -> None:
    inner_schema = StructType(
        [
            StructField("quantity", IntegerType(), nullable=True),
            StructField("unit_price", DoubleType(), nullable=True),
        ]
    )
    _EnrichedOrder = StructType(
        [
            StructField("quantity", IntegerType(), nullable=True),
            StructField("unit_price", DoubleType(), nullable=True),
            StructField("line_total", DoubleType(), nullable=True),
            StructField("discount_pct", IntegerType(), nullable=True),
        ]
    )

    def enrich_order(qty, price):
        if qty is None or price is None:
            return None
        total = round(qty * price, 2)
        discount = 20 if total > 50 else 10 if total > 20 else 0
        return (qty, price, total, discount)

    enrich_udf = F.udf(enrich_order, _EnrichedOrder)

    df = spark.createDataFrame(*customer_orders())
    result = df.withColumn(
        "order_detail", enrich_udf(F.col("quantity"), F.col("unit_price"))
    )
    print("\n=== UDF enriching struct ===")
    result.select("order_id", "product", "status", "order_detail").show(truncate=False)


def main(spark: SparkSession) -> None:
    demo_udf_returns_struct(spark)
    demo_udf_parse_to_struct(spark)
    demo_f_struct_no_udf(spark)
    demo_udf_enrich_struct(spark)


if __name__ == "__main__":
    spark = get_spark("row-udf")
    main(spark)
    spark.stop()

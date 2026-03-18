"""
Duplicate detection: identify fully duplicate rows, key-level duplicates,
and near-duplicates based on a subset of columns.
"""

from pyspark.sql import functions as F

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    # Dataset with intentional duplicates for demonstration
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("product", StringType(), True),
            StructField("amount", DoubleType(), True),
        ]
    )
    data = [
        (1, 101, "Widget", 29.99),
        (2, 102, "Gadget", 49.99),
        (1, 101, "Widget", 29.99),  # exact duplicate of row 0
        (3, 101, "Widget", 29.99),  # same customer+product+amount, different order_id
        (4, 103, "Book", 14.99),
        (5, 103, "Book", 14.99),  # same customer+product+amount
    ]
    df = spark.createDataFrame(data, schema)

    total = df.count()
    print(f"Total rows: {total}")

    # --- Fully duplicate rows ---
    fully_duped = df.count() - df.distinct().count()
    print(f"\n=== Fully duplicate rows: {fully_duped} ===")
    df.groupBy(df.columns).count().filter(F.col("count") > 1).show(truncate=False)

    # --- Key-level duplicates on order_id ---
    key_col = "order_id"
    key_dupes = df.groupBy(key_col).count().filter(F.col("count") > 1)
    print(f"=== Duplicate {key_col}s: {key_dupes.count()} ===")
    key_dupes.show(truncate=False)

    # --- Near-duplicates: same customer + product + amount ===
    subset = ["customer_id", "product", "amount"]
    near_dupe_keys = df.groupBy(subset).count().filter(F.col("count") > 1)
    print(f"=== Near-duplicates on {subset}: {near_dupe_keys.count()} groups ===")
    near_dupe_keys.show(truncate=False)

    # --- Deduplicated result ---
    deduped = df.dropDuplicates()
    print(f"=== After deduplication: {deduped.count()} rows ===")
    deduped.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("explore-duplicates")
    main(spark)
    spark.stop()

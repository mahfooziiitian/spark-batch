"""
toLocalIterator() and toJSON() stream rows to the driver one at a time,
avoiding the memory spike of collect().

  toLocalIterator()   → Iterator[Row]   — yields one Row at a time partition-by-partition
  toJSON()            → RDD[str]        — converts each Row to a JSON string on executors
  toJSON().collect()  → List[str]       — bring JSON strings to driver

toLocalIterator is the preferred pattern when you need to process all rows
on the driver but cannot hold the full dataset in memory simultaneously.
"""

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import customer_orders, employees, product_revenue
from data_frame.spark_utils import get_spark


def demo_to_local_iterator(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # toLocalIterator() — memory-efficient alternative to collect()
    # Rows are fetched partition-by-partition from executors
    # ------------------------------------------------------------------
    print("=== toLocalIterator() — process rows one at a time ===")
    for row in df.toLocalIterator():  # (1)
        print(f"  {row['id']:>2}  {row['employee_name']}")


def demo_to_local_iterator_large(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # Processing pattern: transform each row without materialising all rows
    # ------------------------------------------------------------------
    processed = 0
    revenue_total = 0.0

    for row in df.toLocalIterator():
        if row["status"] == "active":
            revenue_total += row["quantity"] * row["unit_price"]
            processed += 1

    print(f"\ntoLocalIterator — processed {processed} active orders")
    print(f"toLocalIterator — total revenue: {revenue_total:.2f}")


def demo_to_local_iterator_prefetch(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # prefetchPartitions=True — prefetch next partition while processing
    # current one; trades memory for reduced idle time (Spark 3.x)
    # ------------------------------------------------------------------
    print("\n=== toLocalIterator(prefetchPartitions=True) ===")
    rows = list(df.toLocalIterator(prefetchPartitions=True))  # (2)
    print(f"  fetched {len(rows)} rows with prefetch")


def demo_to_json(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # toJSON() — returns an RDD of JSON strings (one per row)
    # Conversion happens on executors — only strings travel to the driver
    # ------------------------------------------------------------------
    json_rdd = df.toJSON()
    print("\n=== toJSON() — first 3 JSON strings ===")
    for json_str in json_rdd.take(3):
        parsed = json.loads(json_str)
        print(f"  {parsed}")


def demo_to_json_collect(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # Collect all JSON strings and deserialise on the driver
    # ------------------------------------------------------------------
    records = [json.loads(s) for s in df.toJSON().collect()]
    electronics = [r for r in records if r["category"] == "Electronics"]

    print(f"\ntoJSON — electronics products: {len(electronics)}")
    for r in electronics:
        print(f"  {r['product']:12s}  revenue={r['revenue']}")


def demo_to_json_write(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # toJSON() → write newline-delimited JSON to a file
    # ------------------------------------------------------------------
    import os
    import shutil

    output_path = os.environ.get("OUTPUT_PATH", "/tmp/orders_json")
    shutil.rmtree(output_path, ignore_errors=True)  # (3)
    df.toJSON().saveAsTextFile(output_path)
    print(f"\ntoJSON — saved NDJSON to {output_path}")

    # Read back to verify
    read_back = spark.read.json(output_path)
    print(f"  round-trip row count: {read_back.count()}")


def main(spark: SparkSession) -> None:
    demo_to_local_iterator(spark)
    demo_to_local_iterator_large(spark)
    demo_to_local_iterator_prefetch(spark)
    demo_to_json(spark)
    demo_to_json_collect(spark)
    demo_to_json_write(spark)


# (1) toLocalIterator fetches one partition at a time — peak memory on the
#     driver equals the size of the largest single partition.
# (2) prefetchPartitions overlaps network transfer and driver processing
#     at the cost of holding two partitions in driver memory simultaneously.
# (3) saveAsTextFile writes one part-* file per partition; use
#     df.coalesce(1).toJSON() to force a single output file.

if __name__ == "__main__":
    spark = get_spark("action-iterate")
    main(spark)
    spark.stop()

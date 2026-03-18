"""
RDD-level reduce operations — bring distributed data to a single driver value
without a full collect().

  rdd.reduce(f)               — associative + commutative binary function → single value
  rdd.fold(zeroValue, f)      — reduce with an identity element (safe on empty RDDs)
  rdd.aggregate(zero, seqOp, combOp) — compute a result of a different type than the RDD

These operate on the RDD layer; access via df.rdd or df.select(...).rdd.
For most use cases prefer df.agg() — it stays in the Catalyst optimizer.
Use rdd.reduce/fold/aggregate when the aggregation logic cannot be expressed
with built-in Spark functions.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import Row

from data_frame.sample_data import product_revenue
from data_frame.spark_utils import get_spark


def demo_reduce(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # rdd.reduce(f) — f must be associative AND commutative
    # Spark may apply f in any order across partitions
    # ------------------------------------------------------------------

    # Sum revenue using reduce (equivalent to df.agg(F.sum("revenue")))
    total_revenue = df.rdd.map(lambda r: r["revenue"]).reduce(lambda a, b: a + b)
    print(f"reduce — total revenue : {total_revenue:.2f}")

    # Max revenue
    max_revenue = df.rdd.map(lambda r: r["revenue"]).reduce(lambda a, b: max(a, b))
    print(f"reduce — max revenue   : {max_revenue:.2f}")

    # Concatenate all product names (demonstrates string reduce)
    all_products = df.rdd.map(lambda r: r["product"]).reduce(lambda a, b: f"{a}, {b}")
    print(f"reduce — products      : {all_products}")


def demo_fold(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # rdd.fold(zeroValue, f) — like reduce but safe on empty RDDs
    # zeroValue is combined with each partition result AND the final result
    # ------------------------------------------------------------------
    total_revenue = df.rdd.map(lambda r: r["revenue"]).fold(0.0, lambda a, b: a + b)
    print(f"\nfold  — total revenue : {total_revenue:.2f}")

    # Empty DataFrame — fold returns zeroValue; reduce would raise ValueError
    empty_rdd = spark.createDataFrame([], df.schema).rdd.map(lambda r: r["revenue"])
    safe_total = empty_rdd.fold(0.0, lambda a, b: a + b)
    print(f"fold  — empty RDD     : {safe_total}")


def demo_aggregate(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # rdd.aggregate(zeroValue, seqOp, combOp)
    # Accumulate into a type different from the RDD element type.
    #
    #   seqOp  — merges one RDD element into the accumulator (runs on executor)
    #   combOp — merges two accumulators (runs on driver to combine partitions)
    # ------------------------------------------------------------------

    # Compute (sum, count) in one pass to derive the mean without two scans
    zero = (0.0, 0)  # (sum, count)

    def seq_op(acc: tuple, row: Row) -> tuple:
        return acc[0] + row["revenue"], acc[1] + 1

    def comb_op(a: tuple, b: tuple) -> tuple:
        return a[0] + b[0], a[1] + b[1]

    total, count = df.rdd.aggregate(zero, seq_op, comb_op)
    mean = total / count if count else 0.0
    print(f"\naggregate — sum={total:.2f}  count={count}  mean={mean:.2f}")

    # Compare with built-in
    builtin_mean = df.select(F.avg("revenue")).first()[0]
    print(f"F.avg    — mean={builtin_mean:.2f}")


def demo_reduce_by_key(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # reduceByKey — sum revenue per category without groupBy
    # ------------------------------------------------------------------
    category_totals = (
        df.rdd.map(lambda r: (r["category"], r["revenue"]))  # (key, value) pairs
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: -kv[1])
        .collect()
    )
    print("\nreduceByKey — revenue per category:")
    for category, total in category_totals:
        print(f"  {category:15s}: {total:.2f}")


def demo_tree_reduce(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # treeReduce(f, depth) — multi-level reduce tree
    # Reduces the number of data sent to driver for large partitions
    # ------------------------------------------------------------------
    total = df.rdd.map(lambda r: r["revenue"]).treeReduce(lambda a, b: a + b, depth=2)
    print(f"\ntreeReduce — total revenue: {total:.2f}")


def main(spark: SparkSession) -> None:
    demo_reduce(spark)
    demo_fold(spark)
    demo_aggregate(spark)
    demo_reduce_by_key(spark)
    demo_tree_reduce(spark)


# (1) Always prefer df.agg() with built-in F functions over rdd.reduce() —
#     the optimizer can push aggregations to executors more efficiently.
#     Use rdd.reduce/aggregate only when the logic cannot be expressed in SQL.

if __name__ == "__main__":
    spark = get_spark("action-reduce")
    main(spark)
    spark.stop()

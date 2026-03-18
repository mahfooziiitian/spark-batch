"""
explain() prints the query execution plan — it does NOT trigger computation.
Use it to understand how Spark will execute a query: join strategies, filter
push-down, broadcast decisions, and stage boundaries.

  explain()                    — simple physical plan summary
  explain(extended=True)       — parsed → analysed → optimised → physical plans
  explain(mode="simple")       — physical plan (default)
  explain(mode="extended")     — all four plans
  explain(mode="codegen")      — generated Java code (advanced)
  explain(mode="cost")         — plan with cost-based optimizer statistics
  explain(mode="formatted")    — human-readable two-section layout (Spark 3.x)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import (
    departments,
    employees,
    product_revenue,
    regional_revenue,
)
from data_frame.spark_utils import get_spark


def demo_explain_simple(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # explain() — default physical plan
    # ------------------------------------------------------------------
    print("=== explain() — simple physical plan ===")
    df.filter(F.col("id") > 2).select("id", "employee_name").explain()


def demo_explain_extended(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # explain(extended=True) — all logical plan phases + physical plan
    # Parsed → Analysed → Optimised → Physical
    # ------------------------------------------------------------------
    print("=== explain(extended=True) ===")
    df.filter(F.col("id") > 2).explain(extended=True)


def demo_explain_modes(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())
    query = (
        df.filter(F.col("revenue") > 100)
        .groupBy("category")
        .agg(F.round(F.sum("revenue"), 2).alias("total"))
    )

    # ------------------------------------------------------------------
    # All supported mode strings
    # ------------------------------------------------------------------
    for mode in ("simple", "extended", "cost", "formatted"):
        print(f"\n=== explain(mode='{mode}') ===")
        query.explain(mode=mode)


def demo_explain_join_strategy(spark: SparkSession) -> None:
    emp = spark.createDataFrame(*employees())
    dept = spark.createDataFrame(*departments())

    # ------------------------------------------------------------------
    # explain() reveals which join strategy Spark chose:
    #   BroadcastHashJoin    — small table fits in memory
    #   SortMergeJoin        — both sides large, sorted + merged
    #   ShuffledHashJoin     — one side hashed, the other streamed
    # ------------------------------------------------------------------
    print("=== explain — inner join (expect BroadcastHashJoin for small data) ===")
    emp.join(dept, on=["department_id"], how="inner").explain()

    print("\n=== explain — forced broadcast hint ===")
    emp.join(F.broadcast(dept), on=["department_id"], how="inner").explain()


def demo_explain_filter_pushdown(spark: SparkSession) -> None:
    df = spark.createDataFrame(*regional_revenue())

    # ------------------------------------------------------------------
    # Filter push-down: Spark moves filters as early as possible.
    # explain() shows whether the filter appears before or after other ops.
    # ------------------------------------------------------------------
    print("=== explain — filter before groupBy (pushed down) ===")
    (
        df.filter(F.col("revenue") > 100)
        .groupBy("region")
        .agg(F.sum("revenue").alias("total"))
        .explain()
    )


def demo_explain_window(spark: SparkSession) -> None:
    from pyspark.sql.window import Window

    df = spark.createDataFrame(*regional_revenue())
    w = Window.partitionBy("region").orderBy("month")

    # ------------------------------------------------------------------
    # Window function plans show the Window node and its partition spec
    # ------------------------------------------------------------------
    print("=== explain — window function ===")
    df.withColumn("running", F.sum("revenue").over(w)).explain()


def demo_explain_cache_effect(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue()).cache()

    # ------------------------------------------------------------------
    # After cache(), InMemoryRelation appears in the plan instead of a scan
    # ------------------------------------------------------------------
    print("=== explain — after cache() ===")
    df.filter(F.col("revenue") > 500).explain()
    df.unpersist()


def main(spark: SparkSession) -> None:
    demo_explain_simple(spark)
    demo_explain_extended(spark)
    demo_explain_modes(spark)
    demo_explain_join_strategy(spark)
    demo_explain_filter_pushdown(spark)
    demo_explain_window(spark)
    demo_explain_cache_effect(spark)


if __name__ == "__main__":
    spark = get_spark("action-explain")
    main(spark)
    spark.stop()

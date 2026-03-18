"""
ROLLUP computes hierarchical subtotals along an ordered list of dimensions.

Given dimensions (region, category, year), it produces:
  - One row per (region, category, year)  — finest grain
  - One row per (region, category)         — year collapsed to NULL
  - One row per (region)                   — category + year collapsed to NULL
  - One grand-total row                    — all dimensions NULL

The number of output rows = N + all intermediate levels + 1 grand total.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import olap_sales
from data_frame.spark_utils import get_spark


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(*olap_sales())
    df.createOrReplaceTempView("sales")

    # ------------------------------------------------------------------
    # 1. Two-level rollup: (region, category)
    # ------------------------------------------------------------------
    print("=== ROLLUP(region, category) ===")
    (
        df.rollup("region", "category")
        .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
        )
        .show(truncate=False)
    )

    # ------------------------------------------------------------------
    # 2. Three-level rollup: (region, category, year)
    # ------------------------------------------------------------------
    print("=== ROLLUP(region, category, year) ===")
    (
        df.rollup("region", "category", "year")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.count("*").alias("row_count"),
        )
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
            F.col("year").asc_nulls_last(),
        )
        .show(40, truncate=False)
    )

    # ------------------------------------------------------------------
    # 3. Replace NULLs with readable labels
    # ------------------------------------------------------------------
    print("=== ROLLUP with labelled subtotals ===")
    (
        df.rollup("region", "category")
        .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
        .withColumn("region", F.coalesce(F.col("region"), F.lit("GRAND TOTAL")))
        .withColumn("category", F.coalesce(F.col("category"), F.lit("ALL")))
        .orderBy("region", "category")
        .show(truncate=False)
    )

    # ------------------------------------------------------------------
    # 4. SQL equivalent
    # ------------------------------------------------------------------
    print("=== SQL ROLLUP ===")
    spark.sql("""
        SELECT
            region,
            category,
            ROUND(SUM(revenue), 2) AS total_revenue
        FROM sales
        GROUP BY ROLLUP(region, category)
        ORDER BY region ASC NULLS LAST, category ASC NULLS LAST
        """).show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("olap-rollup")
    main(spark)
    spark.stop()

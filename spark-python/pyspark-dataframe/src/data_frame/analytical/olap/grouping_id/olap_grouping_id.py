"""
GROUPING() and GROUPING_ID() distinguish detail rows from subtotal/total rows
produced by ROLLUP, CUBE, or GROUPING SETS.

  grouping(col)          → 1 if col is NULL because of aggregation, 0 otherwise
  grouping_id(c1, c2)    → bitmask: bit[i]=1 when column i is aggregated

Bitmask for grouping_id(region, category):
  0b00 = 0  →  detail row       (region, category both present)
  0b01 = 1  →  category rolled  (only region present)
  0b10 = 2  →  region rolled    (only category present)  — CUBE only
  0b11 = 3  →  grand total      (both NULL)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import olap_sales
from data_frame.spark_utils import get_spark


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(*olap_sales())
    df.createOrReplaceTempView("sales")

    # ------------------------------------------------------------------
    # 1. grouping() on a two-level rollup
    # ------------------------------------------------------------------
    print("=== grouping() on ROLLUP(region, category) ===")
    (
        df.rollup("region", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.grouping("region").alias("region_is_subtotal"),
            F.grouping("category").alias("category_is_subtotal"),
        )
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
        )
        .show(truncate=False)
    )

    # ------------------------------------------------------------------
    # 2. grouping_id() to label aggregation levels
    # ------------------------------------------------------------------
    print("=== grouping_id() levels on ROLLUP(region, category) ===")
    (
        df.rollup("region", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.grouping_id("region", "category").alias("level"),
        )
        .withColumn(
            "level_label",
            F.when(F.col("level") == 0, "detail")
            .when(F.col("level") == 1, "region subtotal")
            .when(F.col("level") == 3, "grand total"),
        )
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
        )
        .show(truncate=False)
    )

    # ------------------------------------------------------------------
    # 3. Filter to only grand-total and region-subtotal rows
    # ------------------------------------------------------------------
    print("=== Filter: subtotal rows only (level >= 1) ===")
    (
        df.rollup("region", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.grouping_id("region", "category").alias("level"),
        )
        .filter(F.col("level") >= 1)
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
        )
        .show(truncate=False)
    )

    # ------------------------------------------------------------------
    # 4. All four levels from CUBE with grouping_id bitmask
    # ------------------------------------------------------------------
    print("=== grouping_id() on CUBE(region, category) — all 4 levels ===")
    (
        df.cube("region", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.grouping_id("region", "category").alias("gid"),
        )
        .withColumn(
            "level_label",
            F.when(F.col("gid") == 0, "detail (region+category)")
            .when(F.col("gid") == 1, "by region only")
            .when(F.col("gid") == 2, "by category only")
            .when(F.col("gid") == 3, "grand total"),
        )
        .orderBy("gid", F.col("region").asc_nulls_last())
        .show(truncate=False)
    )

    # ------------------------------------------------------------------
    # 5. Three-dimension grouping_id bitmask: (region, category, year)
    #    gid values: 0..7 (3 bits)
    # ------------------------------------------------------------------
    print("=== grouping_id() on ROLLUP(region, category, year) — 4 levels ===")
    (
        df.rollup("region", "category", "year")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.grouping_id("region", "category", "year").alias("gid"),
        )
        .withColumn(
            "level_label",
            F.when(F.col("gid") == 0, "detail")
            .when(F.col("gid") == 1, "region+category subtotal")
            .when(F.col("gid") == 3, "region subtotal")
            .when(F.col("gid") == 7, "grand total"),
        )
        .orderBy("gid", F.col("region").asc_nulls_last())
        .show(50, truncate=False)
    )

    # ------------------------------------------------------------------
    # 6. SQL equivalent using GROUPING() and GROUPING_ID()
    # ------------------------------------------------------------------
    print("=== SQL GROUPING() and GROUPING_ID() ===")
    spark.sql("""
        SELECT
            region,
            category,
            ROUND(SUM(revenue), 2)         AS total_revenue,
            GROUPING(region)               AS region_is_subtotal,
            GROUPING(category)             AS category_is_subtotal,
            GROUPING_ID(region, category)  AS gid
        FROM sales
        GROUP BY ROLLUP(region, category)
        ORDER BY region ASC NULLS LAST, category ASC NULLS LAST
        """).show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("olap-grouping-id")
    main(spark)
    spark.stop()

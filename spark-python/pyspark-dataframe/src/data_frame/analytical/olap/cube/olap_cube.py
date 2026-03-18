"""
CUBE computes aggregates for every combination of the given dimensions.

Unlike ROLLUP (which is hierarchical), CUBE generates subtotals for ALL
possible combinations of the specified dimensions. With (region, category):

  - (region,  category)   — finest grain
  - (region,  NULL)        — subtotal per region, all categories
  - (NULL,    category)    — subtotal per category, all regions
  - (NULL,    NULL)        — grand total

With n dimensions, CUBE produces 2^n grouping combinations.
ROLLUP with n dimensions produces n+1 combinations.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import olap_sales
from data_frame.spark_utils import get_spark


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(*olap_sales())
    df.createOrReplaceTempView("sales")

    # ------------------------------------------------------------------
    # 1. Basic cube: (region, category) — 2^2 = 4 combinations
    # ------------------------------------------------------------------
    print("=== CUBE(region, category) ===")
    (
        df.cube("region", "category")
        .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
        )
        .show(truncate=False)
    )

    # ------------------------------------------------------------------
    # 2. Cube with multiple aggregates
    # ------------------------------------------------------------------
    print("=== CUBE with count + avg ===")
    (
        df.cube("region", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.avg("revenue"), 2).alias("avg_revenue"),
            F.count("*").alias("records"),
        )
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
        )
        .show(truncate=False)
    )

    # ------------------------------------------------------------------
    # 3. Three-dimension cube: (region, category, year) — 2^3 = 8 combos
    # ------------------------------------------------------------------
    print("=== CUBE(region, category, year) ===")
    (
        df.cube("region", "category", "year")
        .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
            F.col("year").asc_nulls_last(),
        )
        .show(60, truncate=False)
    )

    # ------------------------------------------------------------------
    # 4. CUBE vs ROLLUP — row count comparison
    # ------------------------------------------------------------------
    rollup_count = df.rollup("region", "category", "year").agg(F.sum("revenue")).count()
    cube_count = df.cube("region", "category", "year").agg(F.sum("revenue")).count()
    print(f"ROLLUP(region, category, year) rows : {rollup_count}")
    print(f"CUBE  (region, category, year) rows : {cube_count}")
    print(f"  ROLLUP = n+1 levels    → {rollup_count}")
    print(f"  CUBE   = 2^n combos    → {cube_count}")

    # ------------------------------------------------------------------
    # 5. SQL equivalent
    # ------------------------------------------------------------------
    print("=== SQL CUBE ===")
    spark.sql("""
        SELECT
            region,
            category,
            ROUND(SUM(revenue), 2) AS total_revenue
        FROM sales
        GROUP BY CUBE(region, category)
        ORDER BY region ASC NULLS LAST, category ASC NULLS LAST
        """).show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("olap-cube")
    main(spark)
    spark.stop()

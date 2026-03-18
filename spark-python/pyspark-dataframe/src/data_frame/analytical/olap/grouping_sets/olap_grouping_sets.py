"""
GROUPING SETS gives you precise control over which grouping combinations
to compute — unlike ROLLUP (hierarchical) or CUBE (all combinations).

  GROUP BY GROUPING SETS (
      (region, category),   -- subtotals per region+category
      (region),             -- subtotals per region only
      (category),           -- subtotals per category only
      ()                    -- grand total
  )

GROUPING SETS is only available via spark.sql() in PySpark 3.5.
The DataFrame API does not expose a groupingSets() method.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import olap_sales
from data_frame.spark_utils import get_spark


def main(spark: SparkSession) -> None:
    df = spark.createDataFrame(*olap_sales())
    df.createOrReplaceTempView("sales")

    # ------------------------------------------------------------------
    # 1. Two grouping sets: per-region and grand total
    # ------------------------------------------------------------------
    print("=== GROUPING SETS: (region), () ===")
    spark.sql("""
        SELECT
            region,
            ROUND(SUM(revenue), 2) AS total_revenue
        FROM sales
        GROUP BY GROUPING SETS (
            (region),
            ()
        )
        ORDER BY region ASC NULLS LAST
        """).show(truncate=False)

    # ------------------------------------------------------------------
    # 2. Cross-dimensional: (region,category), (region), (category), ()
    #    This is equivalent to CUBE(region, category)
    # ------------------------------------------------------------------
    print("=== GROUPING SETS: (region,category), (region), (category), () ===")
    spark.sql("""
        SELECT
            region,
            category,
            ROUND(SUM(revenue), 2) AS total_revenue
        FROM sales
        GROUP BY GROUPING SETS (
            (region, category),
            (region),
            (category),
            ()
        )
        ORDER BY region ASC NULLS LAST, category ASC NULLS LAST
        """).show(truncate=False)

    # ------------------------------------------------------------------
    # 3. Custom set: skip the grand total, only region+category and region
    #    This cannot be expressed with ROLLUP or CUBE alone
    # ------------------------------------------------------------------
    print("=== GROUPING SETS: (region,category), (region) — no grand total ===")
    spark.sql("""
        SELECT
            region,
            category,
            year,
            ROUND(SUM(revenue), 2) AS total_revenue
        FROM sales
        GROUP BY GROUPING SETS (
            (region, category, year),
            (region, category),
            (region)
        )
        ORDER BY region ASC NULLS LAST, category ASC NULLS LAST, year ASC NULLS LAST
        """).show(50, truncate=False)

    # ------------------------------------------------------------------
    # 4. Multiple metrics over custom grouping sets
    # ------------------------------------------------------------------
    print("=== GROUPING SETS: multiple aggregates ===")
    spark.sql("""
        SELECT
            region,
            category,
            ROUND(SUM(revenue), 2)  AS total_revenue,
            ROUND(AVG(revenue), 2)  AS avg_revenue,
            COUNT(*)                AS records
        FROM sales
        GROUP BY GROUPING SETS (
            (region, category),
            (region),
            ()
        )
        ORDER BY region ASC NULLS LAST, category ASC NULLS LAST
        """).show(truncate=False)

    # ------------------------------------------------------------------
    # 5. Emulate GROUPING SETS with DataFrame API using unionByName
    #    (useful when you cannot use SQL but need custom groupings)
    # ------------------------------------------------------------------
    print("=== Emulated GROUPING SETS via unionByName ===")

    by_region_category = df.groupBy("region", "category").agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue")
    )
    by_region = (
        df.groupBy("region")
        .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
        .withColumn("category", F.lit(None).cast("string"))
    )
    grand_total = (
        df.agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
        .withColumn("region", F.lit(None).cast("string"))
        .withColumn("category", F.lit(None).cast("string"))
    )

    (
        by_region_category.unionByName(by_region)
        .unionByName(grand_total)
        .orderBy(
            F.col("region").asc_nulls_last(),
            F.col("category").asc_nulls_last(),
        )
        .show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("olap-grouping-sets")
    main(spark)
    spark.stop()

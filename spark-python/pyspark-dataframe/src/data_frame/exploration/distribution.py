"""
Distribution analysis: approximate histogram buckets for numeric columns
using Spark's built-in approxQuantile and histogram RDD action.
"""

from pyspark.sql import functions as F

from data_frame.sample_data import product_revenue
from data_frame.spark_utils import get_spark

BUCKETS = 5
QUANTILES = [0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 1.0]
REL_ERROR = 0.01


def main(spark) -> None:
    df = spark.createDataFrame(*product_revenue())

    print("=== Approximate quantiles for 'revenue' ===")
    quantile_labels = ["min", "p10", "p25", "p50", "p75", "p90", "max"]
    quantile_vals = df.stat.approxQuantile("revenue", QUANTILES, REL_ERROR)
    for label, val in zip(quantile_labels, quantile_vals):
        print(f"  {label:5s}: {val:.2f}")

    print(f"\n=== Histogram ({BUCKETS} buckets) for 'revenue' ===")
    buckets, counts = (
        df.select("revenue").rdd.flatMap(lambda row: [row[0]]).histogram(BUCKETS)
    )
    max_count = max(counts)
    for i, count in enumerate(counts):
        lo = f"{buckets[i]:.0f}"
        hi = f"{buckets[i + 1]:.0f}"
        bar = "█" * int(count / max_count * 30)
        print(f"  [{lo:>6s} – {hi:>6s}]  {count:3d}  {bar}")

    print("\n=== Revenue by category — box-plot summary ===")
    (
        df.groupBy("category")
        .agg(
            F.min("revenue").alias("min"),
            F.expr("percentile_approx(revenue, 0.25)").alias("q1"),
            F.expr("percentile_approx(revenue, 0.50)").alias("median"),
            F.expr("percentile_approx(revenue, 0.75)").alias("q3"),
            F.max("revenue").alias("max"),
            F.round(F.avg("revenue"), 2).alias("mean"),
        )
        .orderBy("category")
        .show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("explore-distribution")
    main(spark)
    spark.stop()

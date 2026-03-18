"""
Value frequency analysis: distinct counts, top-N most frequent values,
and cardinality check per column — useful for spotting skew and outliers.
"""

from pyspark.sql import functions as F

from data_frame.sample_data import customer_orders
from data_frame.spark_utils import get_spark

TOP_N = 5


def main(spark) -> None:
    df = spark.createDataFrame(*customer_orders())
    total = df.count()

    print(f"=== Distinct value counts (total rows: {total}) ===")
    for col in df.columns:
        n_distinct = df.select(col).distinct().count()
        cardinality = "high" if n_distinct > total * 0.5 else "low"
        print(f"  {col:20s} {n_distinct:4d} distinct  [{cardinality}]")

    print(f"\n=== Top-{TOP_N} most frequent values per categorical column ===")
    cat_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]

    for col in cat_cols:
        print(f"\n  [{col}]")
        (
            df.groupBy(col)
            .agg(
                F.count("*").alias("count"),
                F.round(F.count("*") / total * 100, 1).alias("pct"),
            )
            .orderBy(F.desc("count"))
            .limit(TOP_N)
            .show(truncate=False)
        )

    print("=== Value range for numeric columns ===")
    num_cols = [
        f.name
        for f in df.schema.fields
        if "IntegerType" in str(f.dataType) or "DoubleType" in str(f.dataType)
    ]

    for col in num_cols:
        row = df.agg(F.min(col), F.max(col)).first()
        print(f"  {col:20s} min={row[0]}  max={row[1]}")


if __name__ == "__main__":
    spark = get_spark("explore-value-frequency")
    main(spark)
    spark.stop()

"""
Correlation analysis: Pearson correlation matrix for all numeric column pairs.
High correlation (|r| > 0.8) can indicate redundant features or data leakage.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

from data_frame.sample_data import regional_revenue
from data_frame.spark_utils import get_spark

_NUMERIC = (IntegerType, LongType, DoubleType)


def main(spark) -> None:
    df = spark.createDataFrame(*regional_revenue())

    # Encode month as an integer for correlation purposes
    df = df.withColumn(
        "month_num", F.regexp_extract("month", r"-(\d+)$", 1).cast("int")
    )

    num_cols = [
        f.name for f in df.schema.fields if isinstance(f.dataType, tuple(_NUMERIC))
    ]
    print(f"Numeric columns: {num_cols}\n")

    print("=== Pearson Correlation Matrix ===")
    header = f"{'':20s}" + "".join(f"{c:>12s}" for c in num_cols)
    print(header)

    for c1 in num_cols:
        row_str = f"{c1:20s}"
        for c2 in num_cols:
            corr = df.stat.corr(c1, c2)
            row_str += f"{corr:12.4f}"
        print(row_str)

    print("\n=== Pairs with |r| > 0.5 ===")
    for i, c1 in enumerate(num_cols):
        for c2 in num_cols[i + 1 :]:
            corr = df.stat.corr(c1, c2)
            if abs(corr) > 0.5:
                print(f"  {c1} ↔ {c2}: r = {corr:.4f}")


if __name__ == "__main__":
    spark = get_spark("explore-correlation")
    main(spark)
    spark.stop()

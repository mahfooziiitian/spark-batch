"""
RANGE frames use logical offsets from the ordering expression of the current row.

For a row with revenue R and RANGE BETWEEN unboundedPreceding AND unboundedFollowing,
the frame covers the entire partition — useful for computing a partition-wide maximum.
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import product_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*product_revenue())

    # Full RANGE frame — equivalent to unbounded window over the whole partition
    window_spec = (
        Window.partitionBy("category")
        .orderBy(F.col("revenue").desc())
        .rangeBetween(-sys.maxsize, sys.maxsize)
    )

    revenue_difference = F.max("revenue").over(window_spec) - F.col("revenue")

    (
        df.select(
            "product",
            "category",
            "revenue",
            revenue_difference.alias("revenue_difference"),
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-range-frame")
    main(spark)
    spark.stop()

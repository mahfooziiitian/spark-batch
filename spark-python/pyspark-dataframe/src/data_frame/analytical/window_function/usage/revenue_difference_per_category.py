from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import product_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*product_revenue())

    window_spec = Window.partitionBy("category").orderBy(F.col("revenue").desc())

    revenue_diff = F.max("revenue").over(window_spec) - F.col("revenue")

    (
        df.select(
            "product", "category", "revenue", revenue_diff.alias("revenue_difference")
        ).show(truncate=False)
    )

    lead_diff = F.lead("revenue", 1).over(window_spec) - F.col("revenue")
    lag_diff = F.lag("revenue", 1).over(window_spec) - F.col("revenue")

    (
        df.select(
            "product",
            "category",
            "revenue",
            lead_diff.alias("lead_revenue"),
            lag_diff.alias("lag_revenue"),
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("revenue-difference-per-category")
    main(spark)
    spark.stop()

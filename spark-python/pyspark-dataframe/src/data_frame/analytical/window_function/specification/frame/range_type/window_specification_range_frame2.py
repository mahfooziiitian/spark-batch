from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import product_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*product_revenue())

    # Frame: current row up to 200 higher in revenue (ascending order)
    w_forward = (
        Window.partitionBy("category")
        .orderBy(F.col("revenue"))
        .rangeBetween(Window.currentRow, 200)
    )

    (
        df.select(
            "product",
            "category",
            "revenue",
            (F.max("revenue").over(w_forward) - F.col("revenue")).alias(
                "sales_difference"
            ),
        ).show(truncate=False)
    )

    # Frame: 200 lower than current row down to current row (descending order)
    w_backward = (
        Window.partitionBy("category")
        .orderBy(F.col("revenue").desc())
        .rangeBetween(-200, Window.currentRow)
    )

    (
        df.select(
            "product",
            "category",
            "revenue",
            (F.max("revenue").over(w_backward) - F.col("revenue")).alias(
                "sales_difference"
            ),
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-range-frame-2")
    main(spark)
    spark.stop()

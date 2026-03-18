"""
The running total is the cumulative sum of all rows up to and including the current one.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import sales
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*sales())
    df.show(truncate=False)

    # Global running total ordered by id
    w_global = Window.orderBy("id")
    (
        df.select("*", F.sum("order_quantity").over(w_global).alias("running_total"))
        .orderBy("id")
        .show(truncate=False)
    )

    # Running total per order_id
    w_per_order = Window.partitionBy("order_id").orderBy("id")
    (
        df.select(
            "*",
            F.sum("order_quantity").over(w_per_order).alias("running_total_per_order"),
        )
        .orderBy("id")
        .show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("running-total")
    main(spark)
    spark.stop()

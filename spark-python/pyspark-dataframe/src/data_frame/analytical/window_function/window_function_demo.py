"""
Window functions compute a result for each input row using a set of related rows
defined by a window specification, without collapsing the rows into a single result.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import product_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*product_revenue())

    window_spec = Window.partitionBy("category").orderBy(F.col("revenue").desc())

    w_running = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    (
        df.withColumn("rank", F.rank().over(window_spec))
        .withColumn("running_total", F.sum("revenue").over(w_running))
        .show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-function-intro")
    main(spark)
    spark.stop()

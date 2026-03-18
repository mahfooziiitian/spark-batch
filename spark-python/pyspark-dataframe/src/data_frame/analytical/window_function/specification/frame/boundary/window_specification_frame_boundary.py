"""
Frame boundary types:
  UNBOUNDED PRECEDING — first row of the partition
  UNBOUNDED FOLLOWING — last row of the partition
  CURRENT ROW        — the row being evaluated
  n PRECEDING / n FOLLOWING — n rows before/after (ROW) or value offset (RANGE)
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import regional_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*regional_revenue())

    # Running total: start of partition → current row
    w_running = (
        Window.partitionBy("region")
        .orderBy("month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Full-partition sum: start → end of partition
    w_full = Window.partitionBy("region").rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    (
        df.withColumn("running_total", F.sum("revenue").over(w_running))
        .withColumn("partition_total", F.sum("revenue").over(w_full))
        .show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-frame-boundary")
    main(spark)
    spark.stop()

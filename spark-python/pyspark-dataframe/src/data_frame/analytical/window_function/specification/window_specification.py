"""
A window specification defines which rows are included in the frame for each input row.

Components:
  Partitioning — controls which rows share the same partition
  Ordering     — determines each row's position within its partition
  Frame        — selects the subset of rows relative to the current row
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import regional_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*regional_revenue())

    window_spec = (
        Window.partitionBy("region")
        .orderBy("month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    (
        df.withColumn("running_total", F.sum("revenue").over(window_spec)).show(
            truncate=False
        )
    )


if __name__ == "__main__":
    spark = get_spark("window-specification")
    main(spark)
    spark.stop()

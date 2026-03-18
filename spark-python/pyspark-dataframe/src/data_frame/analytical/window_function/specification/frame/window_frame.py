"""
A window frame defines which rows are included in the window for each input row.

ROW frame  — physical offsets: CURRENT ROW ± n rows
RANGE frame — logical offsets: rows whose ordering column value is within ± n of the current row
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import regional_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*regional_revenue())

    # ROW frame: 1 row before and 1 row after (3-row sliding window)
    w_rows = Window.partitionBy("region").orderBy("month").rowsBetween(-1, 1)

    # RANGE frame: all rows with revenue within ±50 of the current row's revenue
    w_range = Window.partitionBy("region").orderBy("revenue").rangeBetween(-50, 50)

    (
        df.withColumn("row_avg", F.round(F.avg("revenue").over(w_rows), 2))
        .withColumn("range_avg", F.round(F.avg("revenue").over(w_range), 2))
        .show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-frame")
    main(spark)
    spark.stop()

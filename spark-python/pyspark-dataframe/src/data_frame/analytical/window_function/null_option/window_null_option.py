from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        (1, 10.0),
        (2, None),
        (3, None),
        (4, 40.0),
        (5, None),
        (6, 60.0),
    ]
    df = spark.createDataFrame(data, ["id", "v"])

    w_ordered = Window.orderBy("id")
    w_preceding = w_ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # ignorenulls=True skips null values when scanning the frame
    (
        df.select(
            F.col("id"),
            F.col("v"),
            F.first("v", ignorenulls=True).over(w_ordered).alias("first_ignore_nulls"),
            F.last("v", ignorenulls=True).over(w_preceding).alias("last_ignore_nulls"),
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-null-option")
    main(spark)
    spark.stop()

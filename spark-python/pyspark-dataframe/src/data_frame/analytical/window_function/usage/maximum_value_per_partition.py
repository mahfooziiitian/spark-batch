from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        ("John", "2017-07-02", 13.35),
        ("John", "2017-07-06", 27.33),
        ("John", "2017-07-04", 21.72),
        ("Mary", "2017-07-07", 69.74),
        ("Mary", "2017-07-01", 59.44),
        ("Mary", "2017-07-05", 80.14),
    ]
    df = spark.createDataFrame(data, ["name", "tx_date", "amount"])

    # RANGE frame spanning the full partition
    w_range = (
        Window.partitionBy("name")
        .orderBy(F.desc("amount"))
        .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    (
        df.withColumn(
            "amount_diff", F.round(F.max("amount").over(w_range) - F.col("amount"), 3)
        ).show(truncate=False)
    )

    # ROW frame spanning the full partition (same result for non-duplicate keys)
    w_rows = (
        Window.partitionBy("name")
        .orderBy(F.desc("amount"))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    (
        df.withColumn(
            "amount_diff", F.round(F.max("amount").over(w_rows) - F.col("amount"), 3)
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("maximum-value-per-partition")
    main(spark)
    spark.stop()

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

    # 3-row sliding window: 1 row before, current row, 1 row after
    w_moving = Window.partitionBy("name").orderBy("tx_date").rowsBetween(-1, 1)

    (
        df.withColumn("moving_avg", F.round(F.avg("amount").over(w_moving), 2)).show(
            truncate=False
        )
    )


if __name__ == "__main__":
    spark = get_spark("moving-average")
    main(spark)
    spark.stop()

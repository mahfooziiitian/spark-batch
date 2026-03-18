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

    w_cumulative = (
        Window.partitionBy("name")
        .orderBy("tx_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    (
        df.withColumn(
            "cumulative_sum", F.round(F.sum("amount").over(w_cumulative), 2)
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("cumulative-aggregates")
    main(spark)
    spark.stop()

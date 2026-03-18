from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        ("Alice", "Engineering", 30),
        ("Bob", "Engineering", 25),
        ("Charlie", "Engineering", 35),
        ("David", "Sales", 40),
        ("Eve", "Sales", 28),
        ("Frank", "HR", 45),
    ]
    df = spark.createDataFrame(data, ["name", "dept", "age"])

    # RANGE from start of partition to current row (ordered by age ascending)
    window_spec = (
        Window.partitionBy("dept")
        .orderBy(F.col("age").asc())
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )

    (
        df.select(
            "name",
            "dept",
            "age",
            F.cume_dist().over(window_spec).alias("cumulative_dist"),
        ).show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("window-unbounded-preceding")
    main(spark)
    spark.stop()

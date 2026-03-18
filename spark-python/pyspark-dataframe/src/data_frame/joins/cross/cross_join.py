"""
CROSS JOIN (Cartesian product) — combines every row from the left with every row from the right.
Row count = left_count × right_count. Use with care on large datasets.
"""

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    colours = spark.createDataFrame([("Red",), ("Green",), ("Blue",)], ["colour"])
    sizes = spark.createDataFrame([("S",), ("M",), ("L",)], ["size"])

    result = colours.crossJoin(sizes)
    result.show(truncate=False)
    print(f"Total rows: {result.count()}")  # 3 × 3 = 9


if __name__ == "__main__":
    spark = get_spark("cross-join")
    main(spark)
    spark.stop()

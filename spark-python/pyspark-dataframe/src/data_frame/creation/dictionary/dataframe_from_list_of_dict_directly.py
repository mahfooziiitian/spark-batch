"""
Create a DataFrame directly from a list of Python dicts.
Keys become column names; missing keys produce null values.
"""

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        {"id": 1, "name": "John", "age": 25},
        {"id": 2, "name": "Jane", "age": 30},
        {"id": 3, "name": "Bob", "age": 22},
        {"id": 4, "name": "Carol"},  # missing 'age' → null
    ]

    df = spark.createDataFrame(data)
    df.printSchema()
    df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("creation-from-dicts")
    main(spark)
    spark.stop()

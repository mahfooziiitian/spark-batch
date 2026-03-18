"""
Create a DataFrame by unpacking dicts into pyspark.sql.Row objects.
"""

from pyspark.sql import Row

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        {"id": 1, "name": "John", "age": 25},
        {"id": 2, "name": "Jane", "age": 30},
        {"id": 3, "name": "Bob", "age": 22},
    ]

    rdd = spark.sparkContext.parallelize([Row(**item) for item in data])
    df = spark.createDataFrame(rdd)
    df.printSchema()
    df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("creation-unpacking-row")
    main(spark)
    spark.stop()

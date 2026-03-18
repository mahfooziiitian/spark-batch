"""
Create a single-column DataFrame from a plain Python list of scalars.
"""

from pyspark.sql.types import IntegerType

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    numbers = [1, 2, 3, 4]
    df = spark.createDataFrame(numbers, IntegerType())
    df.printSchema()
    df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("creation-list-of-scalars")
    main(spark)
    spark.stop()

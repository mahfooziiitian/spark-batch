import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]


def main():

    # Initialize Spark session
    spark = SparkSession.builder.appName("ArrayOfTuplesFilter").getOrCreate()

    data = [
        (1, [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]),
        (2, [{"name": "Charlie", "age": 35}, {"name": "David", "age": 40}]),
    ]

    df = spark.createDataFrame(data, ["id", "people"])

    # Filter array inside a struct
    df_filtered = df.withColumn(
        "filtered_people", F.expr("filter(people, x -> x.age > 30)")
    )
    df_filtered.show(truncate=False)


if __name__ == "__main__":
    main()

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]


def main():

    spark = SparkSession.builder.appName("").getOrCreate()

    data = [
        (1, {"name": "Alice", "age": 25}),
        (2, {"name": "Charlie", "age": 35}),
    ]

    # Define Schema
    df = spark.createDataFrame(data, ["id", "info"])

    # Filtering based on a field inside the Struct
    df_filtered = df.filter(F.col("info.age") > 28)
    df_filtered.show(truncate=False)


if __name__ == "__main__":
    main()

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]


def main():

    spark = SparkSession.builder.appName("").getOrCreate()
    # Sample Data
    data = [
        (1, ["apple", "banana"]),
        (2, ["banana", "cherry"]),
        (3, ["apple", "cherry"]),
    ]

    df = spark.createDataFrame(data, ["id", "fruits"])

    # Filter rows where the array contains "apple"
    df_filtered = df.filter(array_contains(col("fruits"), "apple"))
    df_filtered.show()


if __name__ == "__main__":
    main()

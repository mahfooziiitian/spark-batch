""" """

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_except

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("array_distinct").getOrCreate()

    # Sample DataFrame with an array column containing null and empty elements
    data = [
        ("John", [25, None, 30, 35, None, 40], [40, 56, ""]),
        ("Alice", [30, 35, 40, None, ""], [45, 67, None]),
        ("Bob", [], [23]),
    ]

    columns = ["name", "ages", "other_ages"]

    df = spark.createDataFrame(data, columns)
    df.printSchema()

    # Use expr and filter to remove null and empty elements from the "ages" array column
    df_compacted = df.withColumn("array_except", array_except("ages", "other_ages"))

    # Show the DataFrame with the compacted array column
    df_compacted.show(truncate=False)

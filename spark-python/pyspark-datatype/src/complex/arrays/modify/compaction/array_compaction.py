"""
Removes null values from the array.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_compact

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("array_compaction").getOrCreate()

    # Sample DataFrame with an array column containing null and empty elements
    data = [
        ("John", [25, None, 30, 35, None, 40]),
        ("Alice", [30, 35, 40, None, ""]),
        ("Bob", []),
    ]

    columns = ["name", "ages"]

    df = spark.createDataFrame(data, columns)

    # Use expr and filter to remove null and empty elements from the "ages" array column
    df_compacted = df.withColumn("ages_cleaned", array_compact("ages"))

    # Show the DataFrame with the compacted array column
    df_compacted.show(truncate=False)

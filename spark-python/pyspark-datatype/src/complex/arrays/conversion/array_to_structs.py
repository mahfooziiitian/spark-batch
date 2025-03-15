import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import struct

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("array_to_structs").getOrCreate()

    # Sample DataFrame with arrays
    data = [
        ("John", [25, 30], ["New York", "San Francisco"]),
        ("Alice", [30, 35], ["San Francisco", "Los Angeles"]),
    ]

    columns = ["name", "ages", "cities"]

    df = spark.createDataFrame(data, columns)

    # Use the struct function to combine arrays into a struct column
    df_with_struct = df.withColumn("info", struct("ages", "cities"))

    # Drop the original array columns if needed
    df_with_struct = df_with_struct.drop("ages", "cities")

    # Show the DataFrame with the struct column
    df_with_struct.show(truncate=False)

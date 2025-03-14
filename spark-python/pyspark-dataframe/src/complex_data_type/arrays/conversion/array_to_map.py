import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import create_map

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("array_to_map").getOrCreate()

    # Sample DataFrame with an array column
    data = [("John", [25, 30, 35, 50]),
            ("Alice", [30, 35, 40, 60])]

    columns = ["name", "ages"]

    df = spark.createDataFrame(data, columns)

    # Use create_map to convert the array column to a tuple column
    df_with_tuple = df.withColumn("ages_tuple", create_map(["ages"]).alias("ages_tuple"))

    # Drop the original array column if needed
    df_with_tuple = df_with_tuple.drop("ages")

    # Show the DataFrame with the tuple column
    df_with_tuple.show(truncate=False)

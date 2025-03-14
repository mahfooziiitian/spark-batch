import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, lit

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("creating_array_column").getOrCreate()

    # Sample DataFrame
    data = [("John", 25, "New York"),
            ("Alice", 30, "San Francisco")]

    columns = ["name", "age", "city"]

    df = spark.createDataFrame(data, columns)

    # Use the array function to create an array column
    df_with_array = df.withColumn("info_array", array(col("name"), col("age"), col("city")))
    df_with_array = df_with_array.withColumn("literal_array", array(lit("John"), lit(25), lit("New York")))

    # Show the DataFrame with the array column
    df_with_array.show(truncate=False)

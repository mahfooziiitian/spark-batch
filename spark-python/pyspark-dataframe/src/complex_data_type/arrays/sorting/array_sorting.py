import os
import sys

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import arrays_overlap, arrays_zip, get, array_position, shuffle, sort_array

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("sort_array").getOrCreate()

    # Sample DataFrame with an array column
    data = [("John", [25, 40, 35, None]),
            ("Alice", [30, 20, 55, 50])]

    columns = ["name", "ages"]

    df = spark.createDataFrame(data, columns)

    df.printSchema()

    # Append a literal value to the "ages" array column
    df_appended = (df.withColumn("sort_array", sort_array('ages')))

    # Show the DataFrame with the appended array column
    df_appended.show(truncate=False)

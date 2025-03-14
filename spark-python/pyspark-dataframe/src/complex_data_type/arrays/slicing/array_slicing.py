import os
import sys

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import arrays_overlap, arrays_zip, get, array_position, shuffle

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("array_slicing").getOrCreate()

    # Sample DataFrame with an array column
    data = [("John", [25, 30, 35]),
            ("Alice", [30, 35, 40, 50])]

    columns = ["name", "ages"]

    df = spark.createDataFrame(data, columns)

    df.printSchema()

    # Append a literal value to the "ages" array column
    df_appended = (df.withColumn("array_slicing", functions.slice('ages', 1, 2)))

    # Show the DataFrame with the appended array column
    df_appended.show(truncate=False)

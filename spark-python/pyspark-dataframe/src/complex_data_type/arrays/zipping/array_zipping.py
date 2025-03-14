import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_overlap, arrays_zip

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("zipping_array").getOrCreate()

    # Sample DataFrame with an array column
    data = [("John", [25, 30, 35], [40, 30, 60]),
            ("Alice", [30, 35, 40], [45, 55])]

    columns = ["name", "ages", "age1"]

    df = spark.createDataFrame(data, columns)

    df.printSchema()

    # Append a literal value to the "ages" array column
    df_appended = (df.withColumn("zipping_array", arrays_zip('ages', "age1")))

    # Show the DataFrame with the appended array column
    df_appended.show(truncate=False)

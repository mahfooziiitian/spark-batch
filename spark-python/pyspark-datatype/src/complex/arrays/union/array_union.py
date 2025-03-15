import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_union

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("array_union").getOrCreate()

    # Sample DataFrame with an array column
    data = [("John", [25, 30, 35], [40, 30, 60]), ("Alice", [30, 35, 40], [45, 55, 65])]

    columns = ["name", "ages", "age1"]

    df = spark.createDataFrame(data, columns)

    df.printSchema()

    # Append a literal value to the "ages" array column
    df_appended = df.withColumn("union_array", array_union("ages", "age1"))

    # Show the DataFrame with the appended array column
    df_appended.show(truncate=False)

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_repeat

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("array_repeat").getOrCreate()

    # Sample DataFrame with an array column
    data = [("John", [25, 30, 35]), ("Alice", [30, 35, 40])]

    columns = ["name", "ages"]

    df = spark.createDataFrame(data, columns)

    df.printSchema()

    # Append a literal value to the "ages" array column
    df_appended = df.withColumn("repeat_array", array_repeat("ages", 2))

    # Show the DataFrame with the appended array column
    df_appended.show(truncate=False)

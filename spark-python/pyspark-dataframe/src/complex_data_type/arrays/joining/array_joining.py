import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_join

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName("array_joining").getOrCreate()

    data = [("John", [1, 2, 3]),
            ("Alice", [2, 5])]

    columns = ["name", "ages"]

    df = spark.createDataFrame(data, columns)
    df.printSchema()

    # Use expr and filter to remove null and empty elements from the "ages" array column
    df_array_intersect = (df.withColumn("array_joining", array_join("ages", ","))
                          .withColumn("array_joining_null_replace", array_join("ages", ",", "")))

    # Show the DataFrame with the compacted array column
    df_array_intersect.show(truncate=False)

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_intersect

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName("array_intersection").getOrCreate()
    spark.sql("SELECT array_intersect(array(1, 2, 3), array(1, 3, 5))").show(truncate=False)

    data = [("John", [1, 2, 3], [1, 3, 5]),
            ("Alice", [2, 5], [4, 6])]

    columns = ["name", "array1", "array2"]

    df = spark.createDataFrame(data, columns)
    df.printSchema()

    # Use expr and filter to remove null and empty elements from the "ages" array column
    df_array_intersect = df.withColumn("array_intersection", array_intersect("array1", "array2"))

    # Show the DataFrame with the compacted array column
    df_array_intersect.show(truncate=False)

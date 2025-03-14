import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # Define two Spark actions that return Futures
    future1 = spark.range(10).countAsync()
    future2 = spark.range(11, 21).reduceAsync(lambda a, b: a + b)

    result1 = future1.get()
    result2 = future2.get()
    print(result1)
    print(result2)


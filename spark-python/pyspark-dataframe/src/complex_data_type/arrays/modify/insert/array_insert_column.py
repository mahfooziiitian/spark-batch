import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName("array_insert_column").getOrCreate()
    spark.sql("SELECT array_insert(array(5, 3, 2, 1), -4, 4)").show(truncate=False)

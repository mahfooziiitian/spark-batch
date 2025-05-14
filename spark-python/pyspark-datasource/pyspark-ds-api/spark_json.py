import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def main():
    spark = SparkSession.builder.appName("Spark_JSON_Processing").getOrCreate()
    df = spark.read.json("simple_offset.json")
    df.show(truncate=False)
    df.printSchema()


if __name__ == "__main__":
    main()

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import Row

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Sample list of dictionaries
    data = [
        {"id": 1, "name": "John", "age": 25},
        {"id": 2, "name": "Jane", "age": 30},
        {"id": 3, "name": "Bob", "age": 22}
    ]

    # Create DataFrame
    df = spark.createDataFrame(spark.sparkContext.parallelize([Row(**item) for item in data]))

    df.printSchema()

    # Show DataFrame
    df.show()

    spark.stop()

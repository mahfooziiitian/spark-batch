import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import Row

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable


# Define a data class
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age


if __name__ == '__main__':
    # Initialize SparkSession
    spark = (SparkSession.builder
             .appName("class_schema")
             .getOrCreate())

    # Create some sample data
    people_data = [
        Person("Alice", 30),
        Person("Bob", 25),
        Person("Charlie", 35)
    ]

    # Convert data to Rows
    people_rows = [Row(name=p.name, age=p.age) for p in people_data]

    # Create DataFrame
    df = spark.createDataFrame(people_rows)

    # Show DataFrame
    df.show(truncate=False)

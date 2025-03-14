import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.master("local[*]").appName("TungstenCodeGen").getOrCreate()

    # Sample data
    data = [("John", 30), ("Alice", 25), ("Bob", 35)]
    columns = ["name", "age"]

    # Create a DataFrame
    df = spark.createDataFrame(data, schema=columns)

    # Define a simple UDF
    @udf(returnType=IntegerType())
    def add_one(age):
        return age + 1


    # Use the UDF in a transformation
    result_df = df.withColumn("age_plus_one", add_one(df["age"]))

    # Show the result
    result_df.show()

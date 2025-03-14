import os
import sys

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':

    # Create a Spark session
    spark = SparkSession.builder.appName("BinaryProcessingTungsten").getOrCreate()

    # Sample data with binary column
    data = [
        (1, b'binary_data_1'),
        (2, b'binary_data_2'),
        (3, b'binary_data_3')
    ]

    # Define schema with a binary column
    schema = ["id", "binary_data"]

    # Create a DataFrame
    df = spark.createDataFrame(data, schema=schema)

    # Show the original DataFrame
    print("Original DataFrame:")
    df.show()

    # Apply a transformation with binary processing
    transformed_df = df.withColumn("binary_data_length", functions.length(col("binary_data")))

    # Show the transformed DataFrame
    print("Transformed DataFrame:")
    transformed_df.show()

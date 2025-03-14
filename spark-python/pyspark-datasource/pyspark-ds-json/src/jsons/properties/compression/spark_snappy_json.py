"""
Use the spark.read.json method to read the compressed JSON files.
Spark infers the compression format from the file extension.
"""
import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("spark_snappy_json") \
        .getOrCreate()

    # Create a sample DataFrame
    data = [
        {"id": 1, "name": "John Doe", "age": 30},
        {"id": 2, "name": "Jane Smith", "age": 25}
    ]
    df = spark.createDataFrame(data)

    data_home = os.environ['DATA_HOME'].replace("\\", "/")
    gzip_json_file = f"{data_home}/FileData/Json/properties/compression/snappy/json_snappy"

    # Write the DataFrame to a Gzip compressed JSON file
    df.write.mode("overwrite").json(gzip_json_file, compression="snappy")

    # reading the gzip compression
    df_gzip = spark.read.json(gzip_json_file)
    df_gzip.show()

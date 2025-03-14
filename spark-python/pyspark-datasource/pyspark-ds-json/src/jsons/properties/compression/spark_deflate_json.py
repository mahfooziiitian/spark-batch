"""
Compression codec to use when saving to file.
This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
JSON built-in functions ignore this option.
"""
import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("spark_deflate_json") \
        .getOrCreate()

    # Create a sample DataFrame
    data = [
        {"id": 1, "name": "John Doe", "age": 30},
        {"id": 2, "name": "Jane Smith", "age": 25}
    ]
    df = spark.createDataFrame(data)

    data_home = os.environ['DATA_HOME'].replace("\\", "/")
    deflate_json_file = f"{data_home}/FileData/Json/properties/compression/deflate/json_deflate"

    # Write the DataFrame to a Gzip compressed JSON file
    df.write.mode("overwrite").json(deflate_json_file, compression="deflate")

    # reading the gzip compression
    df_gzip = spark.read.json(deflate_json_file)
    df_gzip.show()

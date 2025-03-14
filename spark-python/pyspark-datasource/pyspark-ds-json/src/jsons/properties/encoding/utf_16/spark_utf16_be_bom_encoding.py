"""
For reading, allows to forcibly set one of standard basic or extended encoding for the JSON files.
For example UTF-16BE, UTF-32LE.
For writing, Specifies encoding (charset) of saved json files.
JSON built-in functions ignore this option.
"""

import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("utf_16")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/encoding/utf_16/utf_16_le_bom.json"

    # Read the JSON file
    df = (spark.read
          .option("encoding", "UTF-16")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)

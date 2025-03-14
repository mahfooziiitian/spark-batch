"""
Whether to ignore column of all null values or empty array during schema inference.
"""

import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("drop_field_if_all_null")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/fields/drop_field_if_all_null.json"

    # Read the JSON file
    df = (spark.read
          .option("dropFieldIfAllNull", "true")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)





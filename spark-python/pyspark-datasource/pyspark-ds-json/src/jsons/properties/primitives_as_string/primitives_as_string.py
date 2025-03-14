"""
"""
import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("prefer_decimal")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/primitives_as_string/primitives_as_string.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("primitivesAsString", "true")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show()

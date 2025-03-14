"""
Sets the string that indicates a timestamp format.
Custom date formats follow the formats at datetime pattern.
This applies to timestamp type.

yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]

"""
import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("timestamp_format")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/timestamp_format/timestamp_format.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)

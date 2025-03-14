"""
Sets the string that indicates a timestamp without timezone format.

Custom date formats follow the formats at Datetime Patterns.

This applies to timestamp without timezone type, note that zone-offset and time-zone components are not supported
when writing or reading this data type.

yyyy-MM-dd'T'HH:mm:ss[.SSS]

"""
import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("timestamp_format")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/timestamp_format/timestamp_ntz_format.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)

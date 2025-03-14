"""
Enabled if the time parser policy has legacy settings or if no custom date or timestamp pattern was provided.
Allows falling back to the backward compatible (Spark 1.x and 2.0) behavior of parsing dates and
timestamps if values do not match the set patterns.
"""

import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("timestamp_format")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/timestamp_format/date_time_parsing_fallback.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
          .option("dateFormat", "yyyy-MM-dd")
          .option("enableDateTimeParsingFallback", "true")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)

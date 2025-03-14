"""
Sets the string that indicates a time zone ID to be used to format timestamps in the JSON datasources or partition values.

The following formats of timeZone are supported:
Region-based zone ID: It should have the form 'area/city', such as 'America/Los_Angeles'.
Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or '+01:00'.
Also 'UTC' and 'Z' are supported as aliases of '+00:00'.
Other short names like 'CST' are not recommended to use because they can be ambiguous.

spark.sql.session.timeZone

"""
import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("timestamp_format")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/timestamp_format/timezone_format.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("timeZone", "Asia/Kolkata")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)

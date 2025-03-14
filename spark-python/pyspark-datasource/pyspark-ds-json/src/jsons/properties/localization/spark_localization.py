"""
Sets a locale as language tag in IETF BCP 47 format.
For instance, locale is used while parsing dates and timestamps.
"""
import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("us_locale")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/localization/us_locale.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("dateFormat", "dd-MMM-yyyy")
          .option("locale", "en-US")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show()

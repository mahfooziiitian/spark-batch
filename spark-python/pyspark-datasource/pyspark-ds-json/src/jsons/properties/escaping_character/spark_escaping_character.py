"""
Sets a locale as language tag in IETF BCP 47 format.
For instance, locale is used while parsing dates and timestamps.
"""
import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("JSONExample") \
        .getOrCreate()

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/escape_characters/escape_characters.json"

    df = (spark.read
          .option("allowBackslashEscapingAnyCharacter", "true")
          .option("multiLine", True)
          .json(data_file))

    df.show(truncate=False)

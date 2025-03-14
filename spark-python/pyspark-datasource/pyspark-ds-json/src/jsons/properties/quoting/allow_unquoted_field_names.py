import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("JSONExample") \
        .getOrCreate()

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/unquote_field_names/unquote_field_name.json"

    # Read the JSON file with allowUnquotedFieldNames option
    df = (spark.read
          .option("allowUnquotedFieldNames", "true")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.show(truncate=False)

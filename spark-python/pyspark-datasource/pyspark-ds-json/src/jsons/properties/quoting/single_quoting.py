import os

from pyspark.sql import SparkSession

if __name__ == '__main__':

    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("JSONExample")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/quoting/single_quoting.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("allowSingleQuotes", "true")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.show()

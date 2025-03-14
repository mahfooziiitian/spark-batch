import os

from pyspark.sql import SparkSession

if __name__ == '__main__':

    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("leading_zero")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/leading_zero/leading_zero_numerical.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("allowNumericLeadingZeros", "true")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.show()

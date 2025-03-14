"""
Infers all floating-point values as a decimal type.
If the values do not fit in decimal, then it infers them as doubles.
"""
import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("prefer_decimal")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/prefer_decimal/prefer_decimal.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("prefersDecimal", "true")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show()

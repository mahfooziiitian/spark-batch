"""
Defines fraction of input JSON objects used for schema inferring.
"""
import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("sampling_ratio")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/sampling_ratio/sampling_ratio.json"

    # Read the JSON file with allowComments option
    df = (spark.read
          .option("samplingRatio", 0.5)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)

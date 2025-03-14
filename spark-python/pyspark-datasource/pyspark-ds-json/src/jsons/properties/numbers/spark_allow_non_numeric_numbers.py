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
             .appName("allow_non_numeric_numbers")
             .getOrCreate())

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/numbers/allow_non_numeric_numbers.json"

    # Read the JSON file
    df = (spark.read
          .option("allowNonNumericNumbers", "true")
          .option("multiLine", True)
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)

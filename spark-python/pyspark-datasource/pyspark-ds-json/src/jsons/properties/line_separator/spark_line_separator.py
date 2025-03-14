import os
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("line_seperator")
             .getOrCreate())

    # Sample data
    data = [
        {"id": 1, "name": "Alice", "age": 28},
        {"id": 2, "name": "Bob", "age": 35},
        {"id": 3, "name": "Charlie", "age": 30}
    ]

    # Create DataFrame
    df = spark.createDataFrame(data)
    df.show()

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/line_seperator/line_seperator.json"

    (df.write.mode("overwrite")
     .option("lineSep", "\n")
     .json(data_file))

    # Read the JSON file
    df = (spark.read
          .option("lineSep", "\n")
          .json(data_file))

    # Show the DataFrame
    df.printSchema()
    df.show(truncate=False)

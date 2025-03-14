"""
Whether to ignore null fields when generating JSON objects.
"""
import os

from pyspark.sql import Row
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("ignore_null_fields")
             .getOrCreate())

    # Create a DataFrame with null values
    data = [
        Row(id=1, name="John Doe", age=None),
        Row(id=2, name="Jane Smith", age=25),
        Row(id=3, name=None, age=30)
    ]

    df = spark.createDataFrame(data)

    # Show the DataFrame
    df.printSchema()
    df.show()

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/fields/ignore_null_fields.json"

    (df
     .repartition(1)
     .write
     .mode("overwrite")
     .option("ignoreNullFields", "true")
     .json(data_file))

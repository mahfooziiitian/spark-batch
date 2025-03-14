"""
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("spark_streaming_struct_schema")
             .master("local[*]").getOrCreate())

    # Read all the csv files written atomically in a directory
    userSchema = StructType().add("name", "string").add("age", "integer")
    csvDF = (spark
             .readStream
             .option("sep", ";")
             .schema(userSchema)
             .csv("/path/to/directory"))


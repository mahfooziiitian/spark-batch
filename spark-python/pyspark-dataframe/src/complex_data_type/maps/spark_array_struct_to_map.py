from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == '__main__':

    spark = (SparkSession.builder.master("local[*]")
             .appName("spark_array_struct_to_map")
             .getOrCreate())

    # Sample data with an array of structs
    data = [
        (1, [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]),
        (2, [("key4", "value4"), ("key5", "value5")])
    ]

    # Create a DataFrame from the sample data
    df = spark.createDataFrame(data, ["id", "structs"])

    # Convert an array of key-value pairs to maps column
    df = df.withColumn("map_col", f.map_from_entries(f.col("structs")))

    # Show the result
    df.show(truncate=False)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType, StructType, ArrayType, StructField, IntegerType

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('parsing_json_array_to_array_type')
             .getOrCreate()
             )

    # Sample data
    data = [(1, '[{"name": "Bob", "age": 35}, {"name": "Charlie", "age": 30}]')]

    # Define schema
    schema = ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ]))

    # Create DataFrame
    df = spark.createDataFrame(data, ["id", "value"])
    df.show(truncate=False)

    # Parse JSON array to ArrayType
    df_parsed = df.withColumn("value", from_json(df.value, schema))
    df_parsed.show(truncate=False)

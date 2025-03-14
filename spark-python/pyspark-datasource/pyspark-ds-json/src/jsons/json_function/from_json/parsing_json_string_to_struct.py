from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('dataframe_column_json_string')
             .getOrCreate()
             )

    # Sample data
    data = [(1, '{"name": "John", "age": 30}'),
            (2, '{"name": "Mahfooz", "age": 39}'),
            (3, '{"name": "Alam", "age": 33}')
            ]

    # Define schema
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    # Create DataFrame
    df = spark.createDataFrame(data, ["id", "value"])

    # Parse JSON string to StructType
    df_parsed = df.withColumn("value", from_json(df.value, schema))
    df_parsed.show()

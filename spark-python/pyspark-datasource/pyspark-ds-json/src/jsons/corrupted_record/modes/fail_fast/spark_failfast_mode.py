"""
It throws an exception when it meets corrupted records.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.appName("corrupt_record_failfast").getOrCreate()

    # Sample JSON data with a corrupt record
    data = [
        '{"name": "John", "age": 30}',
        '{"name": "Alice", "age": "twenty-five"}',
        '{"name": "Bob", "age": 35}'
    ]

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/corrupt_record/corrupt_record_failfast.json"

    # Save sample data to a file
    with open(data_file, "w") as file:
        for line in data:
            file.write(line + "\n")

    schema = StructType([StructField("name", StringType(), True),
                         StructField("age", IntegerType(), True),
                         StructField("_corrupt_record", StringType(), True)
                         ])
    try:
        # Read JSON file with PERMISSIVE mode
        df = (spark.read.option("mode", "FAILFAST")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .schema(schema)
              .json(data_file))

        df.show(truncate=False)
    except Exception as e:
        print(e)

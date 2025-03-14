import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':
    # Create Spark session
    spark = SparkSession.builder.appName("corrupt_record_drop_malformed").getOrCreate()

    # Sample JSON data
    data = [
        '{"name": "John", "age": 30}',
        '{"name": "Alice", "age": "twenty-five"}',
        '{"name": "Bob", "age": 35}'
    ]

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/corrupt_record/corrupt_record_drop_malformed.json"

    # Save sample data to a file
    with open(data_file, "w") as file:
        for line in data:
            file.write(line + "\n")

    # Read JSON file with DROPMALFORMED mode
    schema = StructType([StructField("name", StringType(), True),
                         StructField("age", IntegerType(), True),
                         StructField("_corrupt_record", StringType(), True)
                         ])
    df = (spark.read
          .option("mode", "DROPMALFORMED")
          .schema(schema)
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .json(data_file))

    df.show(truncate=False)



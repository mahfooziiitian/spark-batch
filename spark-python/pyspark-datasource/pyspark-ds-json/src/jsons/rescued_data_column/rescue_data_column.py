"""
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.appName("rescue_data_column").getOrCreate()

    # Sample JSON data with a corrupt record
    data = [
        '{"name": "John", "age": 30}',
        '{"name": "Alice", "age": "twenty-five"}',
        '{"name": "Bob", "age": 35}'
    ]

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/corrupt_record/rescue_data_column.json"

    # Save sample data to a file
    with open(data_file, "w") as file:
        for line in data:
            file.write(line + "\n")

    schema = StructType([StructField("name", StringType(), True),
                         StructField("age", IntegerType(), True),
                         StructField("_rescued_data", StringType(), True)
                         ])

    # Read JSON file with PERMISSIVE mode
    df = (spark.read.option("mode", "PERMISSIVE")
          .option("rescueDataColumn", "_rescued_data")
          .schema(schema)
          .json(data_file))

    df.show(truncate=False)

    corrupt_records = df.filter(col("_rescued_data").isNotNull())
    corrupt_records.show(truncate=False)

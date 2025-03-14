"""
PERMISSIVE: when it meets a corrupted record, puts the malformed string into a field configured by
columnNameOfCorruptRecord, and sets malformed fields to null.
To keep corrupt records, a user can set a string type field named columnNameOfCorruptRecord in a user-defined schema.
If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly
adds a columnNameOfCorruptRecord field in an output schema.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.appName("corrupt_record_permissive").getOrCreate()

    # Sample JSON data with a corrupt record
    data = [
        '{"name": "John", "age": 30}',
        '{"name": "Alice", "age": "twenty-five"}',
        '{"name": "Bob", "age": 35}'
    ]

    data_file = os.environ['DATA_HOME'] + "/FileData/Json/properties/corrupt_record/corrupt_record_permissive.json"

    # Save sample data to a file
    with open(data_file, "w") as file:
        for line in data:
            file.write(line + "\n")

    schema = StructType([StructField("name", StringType(), True),
                         StructField("age", IntegerType(), True),
                         StructField("_corrupt_record", StringType(), True)
                         ])

    # Read JSON file with PERMISSIVE mode
    df = (spark.read.option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .schema(schema)
          .json(data_file))

    df.show(truncate=False)
    
    corrupt_records = df.filter(col("_corrupt_record").isNotNull())
    corrupt_records.show(truncate=False)

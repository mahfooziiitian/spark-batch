from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.appName("Read XML with Timestamps").getOrCreate()

# Define schema with Date and Timestamp fields
schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("created_at", TimestampType(), True),  # for timestamp
        StructField("birth_date", DateType(), True),  # for date
    ]
)

df = (
    spark.read.format("xml")
    .option("rowTag", "record")
    .schema(schema)
    .load("path/to/your.xml")
)

df.show()
df.printSchema()

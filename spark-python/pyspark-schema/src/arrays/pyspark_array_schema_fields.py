import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType,
)

schema = StructType([
    StructField("rollno",  StringType(), nullable=False),
    StructField("name",    StringType(), nullable=True),
    StructField("metrics", StructType([
        StructField("age",    IntegerType(), nullable=True),
        StructField("height", FloatType(),   nullable=True),
        StructField("weight", IntegerType(), nullable=True),
    ]), nullable=True),
    StructField("address", StringType(), nullable=True),
])

STUDENTS = [
    ("001", "Alice", (23, 5.79, 67), "New York"),
    ("002", "Bob",   (16, 3.79, 34), "London"),
    ("003", "Carol", (7,  2.79, 17), "Berlin"),
    ("004", "Dave",  (9,  3.69, 28), "Tokyo"),
    ("005", "Eve",   (37, 5.59, 54), "Paris"),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("array-schema-fields")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(STUDENTS, schema=schema)
    df.show(truncate=False)
    df.printSchema()

    print("top-level fields :", [f.name for f in df.schema.fields])
    print("nested fields    :", [f.name for f in df.schema["metrics"].dataType.fields])
    print("simpleString     :", df.schema.simpleString())

    spark.stop()

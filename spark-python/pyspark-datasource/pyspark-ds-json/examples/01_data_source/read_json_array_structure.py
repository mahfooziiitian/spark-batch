from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pys_json import get_spark

if __name__ == "__main__":
    spark = get_spark("json_array")

    schema = StructType(
        [
            StructField("book_name", StringType(), True),
            StructField("author", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [("book", "John", 59), ("book", "Björn", 61), ("tv", "Roger", 36)]

    df = spark.createDataFrame(data=data, schema=schema)

    df.show(truncate=False)

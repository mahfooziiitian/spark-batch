import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]


def main():

    # Initialize Spark session
    spark = SparkSession.builder.appName("ArrayOfTuplesFilter").getOrCreate()

    # Define schema
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField(
                "contacts",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("name", T.StringType()),
                            T.StructField("phone", T.StringType()),
                        ]
                    )
                ),
            ),
        ]
    )

    # Sample data
    data = [
        (
            1,
            [
                {"name": "Alice", "phone": "123-456"},
                {"name": "Bob", "phone": "789-012"},
            ],
        ),
        (2, [{"name": "Charlie", "phone": "345-678"}]),
        (3, []),  # Empty array
        (
            4,
            [
                {"name": "Alice", "phone": "999-999"},
                {"name": "Diana", "phone": "888-888"},
            ],
        ),
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)

    # Show DataFrame
    print("Sample DataFrame:")
    df.show(truncate=False)

    # Use exists (Spark 3.0+) to check if any element in the array satisfies a condition.
    df_filtered = df.filter(F.expr("exists(contacts, x -> x.name = 'Alice')"))
    df_filtered.show(truncate=False)


if __name__ == "__main__":
    main()

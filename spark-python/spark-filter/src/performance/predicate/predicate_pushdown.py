import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("FilterExamples").getOrCreate()

    # Define schema
    schema = T.StructType(
        [
            T.StructField("name", T.StringType()),
            T.StructField("age", T.IntegerType()),
            T.StructField("salary", T.IntegerType()),
            T.StructField("department", T.StringType()),
            T.StructField("email", T.StringType()),
            T.StructField(
                "address", T.StructType([T.StructField("city", T.StringType())])
            ),
            T.StructField("tags", T.ArrayType(T.StringType())),
            T.StructField("year", T.IntegerType()),
        ]
    )

    # Sample data
    data = [
        (
            "Alice",
            28,
            60000,
            "Sales",
            "alice@company.com",
            {"city": "Paris"},
            ["Spark", "AI"],
            2023,
        ),
        ("Bob", 22, 45000, "HR", None, {"city": "London"}, ["Hiring"], 2022),
        (
            "Charlie",
            35,
            75000,
            "Sales",
            "charlie@company.com",
            {"city": "Paris"},
            ["Sales", "Spark"],
            2024,
        ),
        (
            "Diana",
            40,
            None,
            "Finance",
            "diana@company.com",
            {"city": "Tokyo"},
            ["Budget"],
            2023,
        ),
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)

    df.filter(F.col("year") == 2023).show(truncate=False)


if __name__ == "__main__":
    main()

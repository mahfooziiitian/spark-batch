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
            T.StructField(
                "event_date", T.StringType()
            ),  # Stored as string for simplicity
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
            "2023-05-15",
        ),
        ("Bob", 22, 45000, "HR", None, {"city": "London"}, ["Hiring"], "2022-11-30"),
        (
            "Charlie",
            35,
            75000,
            "Sales",
            "charlie123@company.com",
            {"city": "Paris"},
            ["Sales", "Spark"],
            "2023-01-10",
        ),
        (
            "Diana",
            40,
            None,
            "Finance",
            "diana@company.com",
            {"city": "Tokyo"},
            ["Budget"],
            "2023-07-22",
        ),
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)
    df_paris = df.filter(F.col("address.city") == "Paris")
    df_paris.show(truncate=False)


if __name__ == "__main__":
    main()

import os
import sys

import py4j
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import col, to_date

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
            "charlie@company.com",
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

    # Convert event_date string to DateType
    df = df.withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))

    try:

        # Wrong: Evaluates "age > (20 & name)" first
        df.filter(col("age") > 20 & col("name") == "Alice").show()
    except py4j.protocol.Py4JError as e:
        print(f"Error: {str(e)}")

    # Correct: Use parentheses
    df.filter((col("age") > 20) & (col("name") == "Alice")).show()


if __name__ == "__main__":
    main()

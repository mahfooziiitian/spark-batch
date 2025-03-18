import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]


def main():

    # Initialize Spark Session
    spark = SparkSession.builder.appName("FilterDF").getOrCreate()

    # Sample Data
    data = [
        (1, "USA", "California"),
        (2, "USA", "Texas"),
        (3, "India", "Maharashtra"),
        (4, "India", "Karnataka"),
        (5, "Canada", "Ontario"),
        (6, "Canada", "Quebec"),
    ]

    # Creating DataFrame
    df = spark.createDataFrame(data, ["id", "country", "state"])

    # List of (Country, State) tuples for filtering
    filter_list = [["USA", "California"], ["India", "Karnataka"]]

    # Convert list of tuples into a set of conditions
    # df_filtered = df.filter((F.col("country"), F.col("state")).isin(filter_list))

    # Convert filter list into SQL condition
    conditions = " OR ".join(
        [f"(country = '{c}' AND state = '{s}')" for c, s in filter_list]
    )

    # Apply filter using Spark SQL expression
    df_filtered = df.filter(F.expr(conditions))

    # Show Result
    df_filtered.show()

    # Apply `exists` function to filter rows where (country, state) exists in the filter list
    df_filtered = df.filter()

    # Show the result
    df_filtered.show()


if __name__ == "__main__":
    main()

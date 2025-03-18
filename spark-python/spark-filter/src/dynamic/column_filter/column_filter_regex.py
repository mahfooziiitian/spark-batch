import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]


def main():

    spark = SparkSession.builder.appName("").getOrCreate()
    # Sample DataFrame
    data = [
        ("204/24", 10),
        ("205/26", 20),
        ("208/27", 30),
        ("208/27", 40),
        ("208/27", 50),
    ]
    df = spark.createDataFrame(data, ["col1", "col2"])

    # List of regex patterns
    regex_patterns = ["^\\d{3}/\\d{2}$", "^\\d{1,3}/\\d{1,3}$"]
    column = "col1"

    # Create a combined regex pattern using OR (|)
    "|".join(regex_patterns)

    filter_expression = f"col('{column}').rlike('{'|'.join(regex_patterns)}')"
    print(filter_expression)

    # Filter the DataFrame using rlike()
    filtered_df = df.filter(eval(filter_expression))

    # Show the filtered DataFrame
    filtered_df.show()


if __name__ == "__main__":
    main()

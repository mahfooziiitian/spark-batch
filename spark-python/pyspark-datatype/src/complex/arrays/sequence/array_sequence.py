import os
import sys

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import expr, lit, sequence

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("sequence_array").getOrCreate()

    # Sample DataFrame with an array column
    data = [("John", [25, 40, 35, None]), ("Alice", [30, 20, 55, 50])]

    columns = ["name", "ages"]

    df = spark.createDataFrame(data, columns)

    df.printSchema()

    # Append a literal value to the "ages" array column
    df_appended = df.withColumn(
        "sequence_array", functions.sequence(lit(1), lit(8), lit(1))
    )

    # Generate a sequence of dates
    start_date = "2023-01-01"
    end_date = "2023-01-05"
    step = "interval 1 month"

    # Create a DataFrame with a column containing the generated sequence of dates
    df_appended = df_appended.withColumn(
        "generated_dates",
        sequence(
            expr("CAST('{}' AS DATE)".format(start_date)),
            expr("CAST('{}' AS DATE)".format(end_date)),
            step,
        ),
    )

    # Show the DataFrame with the appended array column
    df_appended.show(truncate=False)

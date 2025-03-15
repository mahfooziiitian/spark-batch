import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("array_to_tuple").getOrCreate()

    # Sample DataFrame with an array column
    data = [("John", [25, 30, 35]), ("Alice", [30, 35, 40])]

    columns = ["name", "ages"]

    df = spark.createDataFrame(data, columns)

    # Define a function to convert array to tuple
    def array_to_tuple(arr):
        """
        Converts an array into a tuple.

        Args:
            arr (list): The input array.

        Returns:
            tuple: The converted tuple.
        """
        return tuple(arr)

    # Create a UDF (User Defined Function) from the conversion function
    array_to_tuple_udf = udf(array_to_tuple)

    # Apply the UDF to create a new column with tuples
    df_with_tuples = df.withColumn("ages_tuple", array_to_tuple_udf("ages"))

    # Show the DataFrame with the new tuple column
    df_with_tuples.show(truncate=False)

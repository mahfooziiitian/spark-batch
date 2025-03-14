import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("date_overlap").getOrCreate()

    # Create a DataFrame with sample data
    data = [
        ("1-Jan-23", "10-Jan-23"),
        ("13-Jan-23", "18-Jan-23"),
        ("8-Jan-23", "11-Jan-23")
    ]

    columns = ["Start_date", "End_Date"]

    df = spark.createDataFrame(data, columns)

    # Convert string dates to timestamp
    df = df.withColumn("Start_Date", col("Start_Date").cast("timestamp"))
    df = df.withColumn("End_Date", col("End_Date").cast("timestamp"))

    # Create a new DataFrame with all combinations of date ranges
    df2 = (df.withColumnRenamed("Start_Date", "Start_Date_2")
           .withColumnRenamed("End_Date", "End_Date_2"))
    
    cross_df = df.crossJoin(df2)

    cross_df.show(truncate=False)

    # Define the condition for overlap
    condition = (
            (col("Start_Date") <= col("End_Date_2")) &
            (col("End_Date") >= col("Start_Date_2")) &
            (col("End_Date") != col("End_Date_2"))
    )
    #
    # Add a new column indicating overlap
    result_df = cross_df.withColumn("overlap_ind", condition)

    # Select relevant columns
    result_df = result_df.select("Start_Date", "End_Date", "Start_Date_2", "End_Date_2", "overlap_ind")

    # # Show the result
    result_df.show(truncate=False)

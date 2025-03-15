import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]

if __name__ == "__main__":

    # Create a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Sample data
    data = [
        ("Bilbo Baggins", 50),
        ("Gandalf", 32),
        ("Thorin", 19),
        ("Balin", 18),
        ("Kili", 37),
        ("Dwalin", 19),
        ("Oin", 46),
        ("Gloin", 28),
        ("Fili", 22),
    ]

    # Create a DataFrame
    df = spark.createDataFrame(data, ["name", "age"])

    # Selecting all columns and creating a new struct column
    mod_df = df.select(collect_list(struct(*df.columns)).alias("PersonalDetails"))

    mod_df.printSchema()

    # Show the modified DataFrame
    mod_df.show(truncate=False)

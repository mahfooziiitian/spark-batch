import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session with Tungsten enabled
    spark = (SparkSession.builder
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .appName("TungstenMemoryOptimization")
             .getOrCreate()
             )

    # Sample data
    data = [("John", 30), ("Alice", 25), ("Bob", 35)]
    columns = ["name", "age"]

    # Create a DataFrame
    df = spark.createDataFrame(data, schema=columns)

    # Perform a simple transformation
    result_df = df.withColumn("age_plus_one", expr("age + 1"))

    # Convert the DataFrame to a Pandas DataFrame using Arrow
    result_pandas = result_df.toPandas()

    print(result_pandas)

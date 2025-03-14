import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DynamicPivot").getOrCreate()

    data = [
        ("Alice", "Category1", 10),
        ("Bob", "Category2", 20),
        ("Alice", "Category3", 30),
        ("Bob", "Category1", 40),
        ("Alice", "Category2", 50),
        ("Bob", "Category3", 60),
    ]

    columns = ["Name", "Category", "Value"]

    df = spark.createDataFrame(data, columns)
    df.show()

    # Pivot the DataFrame
    result_df = df.groupBy("Name").pivot("Category").agg({"Value": "sum"}).fillna(0)

    result_df.show()

    # Get distinct categories
    categories = df.select("Category").distinct().rdd.flatMap(lambda x: x).collect()

    # Generate dynamic pivot expressions
    pivot_expr = [col(category) for category in categories]

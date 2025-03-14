import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DoubleType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Define the schema
    schema = StructType([
        StructField("index", LongType(), True),
        StructField("text", StringType(), True),
        StructField("topicDistribution", StructType([
            StructField("type", LongType(), True),
            StructField("values", ArrayType(DoubleType()), True)
        ]), True),
        StructField("wiki_index", StringType(), True)
    ])

    # Sample data
    data = [
        (1, "Sample Text 1", (100, [0.3, 0.5, 0.2]), "wiki_1"),
        (2, "Sample Text 2", (101, [0.1, 0.8, 0.1]), "wiki_2"),
        (3, "Sample Text 3", (102, [0.6, 0.2, 0.2]), "wiki_3")
    ]
    df = spark.createDataFrame(data, schema=schema)

    # Accessing the 'type' field within 'topicDistribution'
    df_type = df.select(col("topicDistribution.type").alias("topic_type"))

    # Show the resulting DataFrames
    df_type.show(truncate=False)

    # Accessing the 'values' array within 'topicDistribution'
    df_values = df.withColumn("topicDistribution",col("topicDistribution.values"))
    df_values.printSchema()
    df_values.show(truncate=False)

    # Accessing a specific element within the 'values' array (e.g., first element)
    df_first_value = df.select(col("topicDistribution.values").alias("first_value"))
    df_first_value.show(truncate=False)

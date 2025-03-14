from pyspark.sql import Window, SparkSession
from pyspark.sql.functions import rank, col

if __name__ == '__main__':
    spark = SparkSession.builder.appName("ranking_window_function").master("local[*]").getOrCreate()

    data = [("Alice", 10), ("Bob", 20), ("Alice", 20), ("Bob", 10)]
    df = spark.createDataFrame(data, ["Name", "Score"])

    # Define window specification
    window_spec = Window.partitionBy("Name").orderBy(col("Score").desc())

    # Apply rank function
    ranked_df = df.withColumn("Rank", rank().over(window_spec))

    ranked_df.show()

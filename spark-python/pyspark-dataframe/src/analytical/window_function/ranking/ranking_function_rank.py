"""
To pick the best-selling and the second best-selling products based the ranking.
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, dense_rank

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    # Loading data into dataframe
    data_home = os.environ['DATA_HOME'].replace('\\', '/')

    data_path = f"{data_home}/FileData/Csv/product_revenue.csv"

    df = spark.read.csv(data_path, inferSchema=True, header=True)

    windowSpec = (Window
                  .partitionBy("category")
                  .orderBy(col("revenue").desc())
                  )

    (df.withColumn("rank", dense_rank().over(windowSpec))
     .where("rank <= 2")
     .show())

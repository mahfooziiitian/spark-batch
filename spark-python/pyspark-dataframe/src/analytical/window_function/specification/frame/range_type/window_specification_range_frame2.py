import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    # Loading data into dataframe
    data_home = os.environ['DATA_HOME'].replace('\\', '/')

    data_path = f"{data_home}/FileData/Csv/product_revenue.csv"

    df = spark.read.csv(data_path, inferSchema=True, header=True)

    windowSpec = (Window
                  .partitionBy("category")
                  .orderBy(col("revenue"))
                  .rangeBetween(Window.currentRow, 2000)
                  )

    revenue_difference = (max(df['revenue']).over(windowSpec) - df['revenue'])

    (df.select(
        df['product'],
        df['category'],
        df['revenue'],
        revenue_difference.alias("sales_difference")
    ).show(truncate=False))

    windowSpec = (Window
                  .partitionBy("category")
                  .orderBy(col("revenue").desc())
                  .rangeBetween(-2000, Window.currentRow)
                  )

    revenue_difference = (max(df['revenue']).over(windowSpec) - df['revenue'])

    (df.select(
        df['product'],
        df['category'],
        df['revenue'],
        revenue_difference.alias("sales_difference")
    ).show(truncate=False))

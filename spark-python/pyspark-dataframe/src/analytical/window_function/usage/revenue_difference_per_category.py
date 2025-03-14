import os

from pyspark.sql import functions
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col

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

    revenue_diff = functions.max(col("revenue")).over(windowSpec) - col("revenue")

    (df.select(
        df['product'],
        df['category'],
        df['revenue'],
        revenue_diff.alias("revenue_difference")
    )).show(truncate=False)

    # Difference on Column

    lead_revenue = functions.lead(col("revenue"), 1).over(windowSpec) - col("revenue")
    lag_revenue = functions.lag(col("revenue"), 1).over(windowSpec) - col("revenue")

    (df.select(
        df['product'],
        df['category'],
        df['revenue'],
        lead_revenue.alias("lead_revenue"),
        lag_revenue.alias("lag_revenue")
    )).show(truncate=False)

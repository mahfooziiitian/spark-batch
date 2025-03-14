import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, cume_dist

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    # Loading data into dataframe
    data_home = os.environ['DATA_HOME'].replace('\\', '/')

    data_path = f"{data_home}/FileData/Csv/employees.csv"

    df = spark.read.csv(data_path, inferSchema=True, header=True)

    df.printSchema()

    windowSpec = (Window
                  .partitionBy("dept")
                  .orderBy(col("age").asc())
                  .rangeBetween(Window.unboundedPreceding, Window.currentRow)
                  )

    cumulative_dist = cume_dist().over(windowSpec)

    (df.select(
        df['name'],
        df['dept'],
        df['age'],
        cumulative_dist.alias("cumulative_dist")
    ).show(truncate=False))

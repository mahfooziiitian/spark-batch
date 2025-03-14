import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, cume_dist
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    # Loading data into dataframe
    data_home = os.environ['DATA_HOME'].replace('\\', '/')

    data_path = f"{data_home}/FileData/Csv/employees.csv"

    df = spark.read.csv(data_path, inferSchema=True, header=True)

    df.printSchema()

    windowSpec = (Window
                  .partitionBy("dept")
                  .orderBy(col("salary").desc())
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                  )

    revenue_difference = (max(df['salary']).over(windowSpec) - df['salary'])

    (df.select(
        df['name'],
        df['dept'],
        df['salary'],
        revenue_difference.alias("salary_difference")
    ).show(truncate=False))

    

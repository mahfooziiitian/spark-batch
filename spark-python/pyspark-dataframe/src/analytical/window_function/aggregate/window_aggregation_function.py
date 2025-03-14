import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    # Loading data into dataframe
    data_home = os.environ['DATA_HOME'].replace('\\', '/')

    data_path = f"{data_home}/FileData/Csv/employees.csv"

    df = spark.read.csv(data_path, inferSchema=True, header=True)

    df.printSchema()

    windowSpec = (Window
                  .partitionBy("dept")
                  )

    min_salary = functions.min("salary").over(windowSpec)
    avg_salary = functions.avg("salary").over(windowSpec)
    max_salary = functions.max("salary").over(windowSpec)

    (df.select(
        df['name'],
        df['dept'],
        df['salary'],
        min_salary.alias("min_salary"),
        avg_salary.alias("avg_salary"),
        max_salary.alias("max_salary")
    ).show(truncate=False))

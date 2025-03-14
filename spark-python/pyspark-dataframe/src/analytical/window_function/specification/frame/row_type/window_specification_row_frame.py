"""
ROW frame

ROW frames are based on physical offsets from the position of the current input row, which means that CURRENT ROW,
PRECEDING, or FOLLOWING specifies a physical offset.

If CURRENT ROW is used as a boundary, it represents the current input row.

PRECEDING and FOLLOWING describes the number of rows appear before and after the current input row, respectively.

"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    # Loading data into dataframe
    data_home = os.environ['DATA_HOME'].replace('\\', '/')

    data_path = f"{data_home}/FileData/Csv/employees.csv"

    df = spark.read.csv(data_path, inferSchema=True, header=True)

    windowSpec = (Window
                  .partitionBy("dept")
                  .orderBy(col("salary"))
                  .rowsBetween(Window.currentRow, 2)
                  )

    revenue_difference = (max(df['salary']).over(windowSpec) - df['salary'])

    (df.select(
        df['name'],
        df['dept'],
        df['salary'],
        revenue_difference.alias("salary_difference")
    ).show(truncate=False))

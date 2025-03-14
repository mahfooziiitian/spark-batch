"""

RANGE frames are based on logical offsets from the position of the current input row, and have similar syntax to the ROW
frame.

A logical offset is the difference between the value of the ordering expression of the current input row and the value
of that same expression of the boundary row of the frame.

Because of this definition, when a RANGE frame is used, only a single ordering expression is allowed.

In this example, the ordering expressions is revenue; the start boundary is 2000 PRECEDING; and the end boundary is 1000
FOLLOWING

    RANGE BETWEEN 2000 PRECEDING AND 1000 FOLLOWING

The following five figures illustrate how the frame is updated with the update of the current input row.

Basically, for every current input row, based on the value of revenue, we calculate the revenue range

    [current revenue value - 2000, current revenue value + 1000]

"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').getOrCreate()

    # Loading data into dataframe
    data_home = os.environ['DATA_HOME'].replace('\\', '/')

    data_path = f"{data_home}/FileData/Csv/sales_data_sample.csv"

    df = spark.read.csv(data_path, inferSchema=True, header=True)

    windowSpec = (Window
                  .partitionBy("PRODUCTLINE")
                  .orderBy(col("SALES").desc())
                  .rangeBetween(-sys.maxsize, sys.maxsize)
                  )

    revenue_difference = (max(df['SALES']).over(windowSpec) - df['SALES'])

    (df.select(df['PRODUCTCODE'], df['PRODUCTLINE'], df['SALES'], revenue_difference.alias("SALES_DIFFERENCE"))
        .show(n=1000, truncate=False))

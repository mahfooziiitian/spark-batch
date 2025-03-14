import os
import sys

from pyspark.sql import SparkSession, Window, functions

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    tx_data = [("John", "2017-07-02", 13.35),
               ("John", "2017-07-06", 27.33),
               ("John", "2017-07-04", 21.72),
               ("Mary", "2017-07-07", 69.74),
               ("Mary", "2017-07-01", 59.44),
               ("Mary", "2017-07-05", 80.14)
               ]
    tx_data_df = spark.createDataFrame(tx_data).toDF("name", "tx_date", "amount")

    """
    you want each frame to include three rows: the current row plus one row before it and one row after it
    """

    for_moving_avg_window = (
        Window.partitionBy("name").orderBy("tx_date").rowsBetween(Window.currentRow - 1, Window.currentRow + 1))

    tx_moving_avg_df = tx_data_df.withColumn("moving_avg",
                                             functions.round(functions.avg("amount")
                                                             .over(for_moving_avg_window), 2))
    # display the result
    tx_moving_avg_df.show(truncate=False)

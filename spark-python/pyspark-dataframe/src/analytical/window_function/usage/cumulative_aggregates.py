import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

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

    for_cumulative_sum_window = (Window.partitionBy("name").orderBy("tx_date")
                                 .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    tx_cumulative_sum_df = tx_data_df.withColumn("cumulative_sum",
                                                 f.round(f.sum("amount").over(for_cumulative_sum_window), 2))

    tx_cumulative_sum_df.show(truncate=False)

    tx_cumulative_sum_df.explain(extended=True)


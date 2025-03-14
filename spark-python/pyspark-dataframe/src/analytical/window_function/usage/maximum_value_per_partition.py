import os
import sys

from pyspark.sql import Window, SparkSession, functions
from pyspark.sql.functions import desc, col

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

    window_spec = (Window.partitionBy("name")
                   .orderBy(desc("amount"))
                   .rangeBetween(Window.unboundedPreceding,
                                 Window.unboundedFollowing))

    amount_difference = functions.max(col("amount")).over(window_spec) - col("amount")

    tx_diff_with_highest_df = tx_data_df.withColumn("amount_diff", functions.round(amount_difference, 3))

    tx_diff_with_highest_df.show(truncate=False)

    tx_diff_with_highest_df.explain(extended=True)

    window_spec = (Window.partitionBy("name")
                   .orderBy(desc("amount"))
                   .rowsBetween(Window.unboundedPreceding,
                                Window.unboundedFollowing))

    amount_difference = functions.max(col("amount")).over(window_spec) - col("amount")

    tx_diff_with_highest_df = tx_data_df.withColumn("amount_diff", functions.round(amount_difference, 3))

    tx_diff_with_highest_df.show(truncate=False)

    tx_diff_with_highest_df.explain(extended=True)

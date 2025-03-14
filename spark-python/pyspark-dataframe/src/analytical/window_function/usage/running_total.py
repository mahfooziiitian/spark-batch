"""
The running total is the sum of all previous lines including the current one.
"""
import os
import sys

from pyspark.sql import SparkSession, Window, functions
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName('window').master("local[*]").getOrCreate()

    sales_data = [
        (0, 0, 0, 5),
        (1, 0, 1, 3),
        (2, 0, 2, 1),
        (3, 1, 0, 2),
        (4, 2, 0, 8),
        (5, 2, 2, 8)
    ]

    sales = spark.createDataFrame(sales_data).toDF("id", "order_id", "prod_id", "order_quantity")

    sales.show(truncate=False)

    ordered_by_id = Window.orderBy(sales["id"])

    total_qty = functions.sum(col("order_quantity")).over(ordered_by_id).alias("running_total")

    print(total_qty)

    sales_total_qty = sales.select("*", total_qty).orderBy("id")

    sales_total_qty.show(truncate=False)

    sales_total_qty.explain(extended=True)

    # running total per order
    by_order_id = ordered_by_id.partitionBy(col("order_id"))

    total_qty_per_order = functions.sum(col("order_quantity")).over(by_order_id).alias("running_total_per_order")

    sales_total_qty_per_order = sales.select("*", total_qty_per_order).orderBy(col("id"))

    sales_total_qty_per_order.show(truncate=False)
    sales_total_qty_per_order.explain(extended=True)

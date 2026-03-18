"""
Top N per category using dense_rank over a window partitioned by category.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import product_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*product_revenue())

    window_spec = Window.partitionBy("category").orderBy(F.col("revenue").desc())

    top_3 = df.withColumn("rank", F.dense_rank().over(window_spec)).where(
        F.col("rank") <= 3
    )

    top_3.show()


if __name__ == "__main__":
    spark = get_spark("top-n-per-group")
    main(spark)
    spark.stop()

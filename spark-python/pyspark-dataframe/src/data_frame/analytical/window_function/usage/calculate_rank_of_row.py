from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import product_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*product_revenue())

    window_spec = Window.partitionBy("category").orderBy(F.col("revenue").desc())

    (
        df.withColumn("rank", F.rank().over(window_spec))
        .withColumn("dense_rank", F.dense_rank().over(window_spec))
        .withColumn("row_number", F.row_number().over(window_spec))
        .show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("calculate-rank-of-row")
    main(spark)
    spark.stop()

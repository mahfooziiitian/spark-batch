from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    buckets = spark.range(9).withColumn("bucket", F.expr("id % 3"))
    dataset = buckets.union(buckets)

    window_spec = Window.partitionBy(F.col("bucket")).orderBy(F.col("id"))

    dataset.withColumn("row_number", F.row_number().over(window_spec)).show(
        truncate=False
    )


if __name__ == "__main__":
    spark = get_spark("sequential-numbering-per-partition")
    main(spark)
    spark.stop()

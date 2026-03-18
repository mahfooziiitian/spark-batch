from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.sample_data import regional_revenue
from data_frame.spark_utils import get_spark


def main(spark) -> None:
    df = spark.createDataFrame(*regional_revenue())

    w_ordered = Window.partitionBy("region").orderBy("month")
    # unbounded frame needed for last() to see all rows in the partition
    w_full = w_ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    (
        df.withColumn("first_revenue", F.first("revenue").over(w_ordered))
        .withColumn("last_revenue", F.last("revenue").over(w_full))
        .show(truncate=False)
    )


if __name__ == "__main__":
    spark = get_spark("analytical-first-last")
    main(spark)
    spark.stop()

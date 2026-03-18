from pyspark.sql import functions as F

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        ("1-Jan-23", "10-Jan-23"),
        ("13-Jan-23", "18-Jan-23"),
        ("8-Jan-23", "11-Jan-23"),
    ]
    df = spark.createDataFrame(data, ["Start_Date", "End_Date"])

    df = df.withColumn("Start_Date", F.col("Start_Date").cast("timestamp")).withColumn(
        "End_Date", F.col("End_Date").cast("timestamp")
    )

    df2 = df.withColumnRenamed("Start_Date", "Start_Date_2").withColumnRenamed(
        "End_Date", "End_Date_2"
    )

    cross_df = df.crossJoin(df2)
    cross_df.show(truncate=False)

    condition = (
        (F.col("Start_Date") <= F.col("End_Date_2"))
        & (F.col("End_Date") >= F.col("Start_Date_2"))
        & (F.col("End_Date") != F.col("End_Date_2"))
    )

    result_df = cross_df.withColumn("overlap_ind", condition).select(
        "Start_Date", "End_Date", "Start_Date_2", "End_Date_2", "overlap_ind"
    )

    result_df.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("date-overlap")
    main(spark)
    spark.stop()

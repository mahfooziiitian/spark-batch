"""
How can we generate a unique session id for click stream data by using Spark dataframes with following two conditions?

    Session expires after 30 minutes of inactivity (Means no click stream data within 30 minutes)
    Session remains active for a total duration of 2 hours. After 2 hours, renew the session.

"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, when, first, sum

if __name__ == '__main__':
    spark = SparkSession.builder.appName("ActiveSessions").getOrCreate()

    click_stream_data = os.environ['DATA_HOME'].replace('\\', '/') + "/FileData/Text/click_stream_data.txt"

    df = (spark.read.option("header", True)
          .option("delimiter", ",")
          .csv(click_stream_data)
          .select(col("user_id"), col("click_time").cast("timestamp")))

    print("original data")
    df.show(truncate=False)

    df_with_previous_click_time = df.withColumn("lag_click_time", lag("click_time", 1).over(
        Window.partitionBy("user_id").orderBy("click_time")))

    print("df_with_previous_click_time data")
    df_with_previous_click_time.show(truncate=False)

    df_with_click_time_diff = df_with_previous_click_time.withColumn("time_diff", (
                (col("click_time").cast("long") - col("lag_click_time").cast("long")) / (60 * 30))).na.fill(0)

    print("df_with_click_time_diff data")
    df_with_click_time_diff.show(truncate=False)

    df_with_new_session = df_with_click_time_diff.withColumn("is_new_session",
                                                             when(col("time_diff") > 1, 1).otherwise(0))

    print("df_with_new_session data")
    df_with_new_session.show(truncate=False)

    df_with_temp_session = df_with_new_session.withColumn("temp_session_id",
                                                          sum(col("is_new_session")).over(
                                                              Window.partitionBy("user_id").orderBy("click_time")))

    print("df_with_temp_session data")
    df_with_temp_session.show(truncate=False)

    df_with_temp_session = df_with_temp_session.withColumn("first_click_time",
                                                           first(col("click_time"))
                                                           .over(Window.partitionBy("user_id", "temp_session_id")
                                                                 .orderBy("click_time")))

    print("df_with_temp_session data")
    df_with_temp_session.show(truncate=False)

    df_with_time_diff_session = df_with_temp_session.withColumn("time_diff2",
                                                                ((col("click_time").cast("long") -
                                                                  col("first_click_time").cast("long")) / (60 * 60 * 2)
                                                                 ).cast("int"))
    print("df_with_time_diff_session data")
    df_with_time_diff_session.show(truncate=False)

    print("session id data")
    (df_with_time_diff_session.withColumn("session_id", (col("time_diff2") + col("temp_session_id")))
     .drop("lag_click_time", "time_diff", "is_new_session", "temp_session_id", "first_click_time", "time_diff2")
     .show())

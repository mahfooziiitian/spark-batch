import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("ActiveSessions").getOrCreate()

    click_stream_data = os.environ['DATA_HOME'].replace('\\', '/') + "/FileData/Text/click_stream_data.txt"

    # Read your data into a DataFrame with header starting from the third row
    df = (spark.read
          .option("header", "true")
          .schema("user_id string, Timestamp timestamp")
          .option("inferSchema", "true")
          .csv(click_stream_data))

    df.printSchema()

    df.show(truncate=False)

    # Define session expiration time in minutes
    session_expire_time = 30

    # Add a session_id column to the DataFrame based on user and session expiration time
    window_spec = Window().partitionBy("user_id").orderBy("Timestamp")

    df = df.withColumn("time_diff",
                       F.unix_timestamp("Timestamp") - F.lag(F.unix_timestamp("Timestamp")).over(window_spec))

    df.show(truncate=False)

    df = df.withColumn("session_id",
                       F.when(F.col("time_diff").isNull() | (F.col("time_diff") > session_expire_time * 60),
                              F.monotonically_increasing_id()))

    df.show(truncate=False)

    # Fill in the session_id for each row based on the previous non-null session_id
    df = df.withColumn("session_id", F.last("session_id", ignorenulls=True).over(window_spec))

    df.show(truncate=False)

    # Calculate session start time and end time
    df = df.withColumn("session_start_time", F.min("Timestamp").over(Window().partitionBy("user_id", "session_id")))
    df = df.withColumn("session_end_time", F.max("Timestamp").over(Window().partitionBy("user_id", "session_id")))

    df.show(truncate=False)

    # Calculate total session duration in minutes
    df = df.withColumn("session_duration",
                       (F.unix_timestamp("session_end_time") - F.unix_timestamp("session_start_time")) / 60)

    df.show(truncate=False)

    # Calculate the number of sessions generated each day
    daily_sessions = df.groupBy(F.to_date("Timestamp").alias("date"), "user_id").agg(
        F.countDistinct("session_id").alias("num_sessions"))

    daily_sessions.show(truncate=False)

    # Calculate the total time spent by a user in a day
    daily_time_spent = (df.groupBy(F.to_date("Timestamp").alias("date"), "user_id").agg(F.countDistinct("session_id").alias("num_sessions")).agg(
        F.sum("session_duration").alias("total_time_spent")))

    # Show the results
    daily_sessions.show()
    daily_time_spent.show()

    # Stop the Spark session
    spark.stop()

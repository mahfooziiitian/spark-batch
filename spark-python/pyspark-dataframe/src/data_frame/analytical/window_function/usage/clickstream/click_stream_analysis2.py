"""
Assign session IDs to clickstream events using two rules:
  1. A session expires after 30 minutes of inactivity.
  2. A session renews after 2 hours of total duration.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_frame.spark_utils import get_spark


def main(spark) -> None:
    data = [
        ("user1", "2024-01-01 10:00:00"),
        ("user1", "2024-01-01 10:15:00"),
        ("user1", "2024-01-01 10:50:00"),
        ("user1", "2024-01-01 11:30:00"),
        ("user2", "2024-01-01 09:00:00"),
        ("user2", "2024-01-01 09:20:00"),
        ("user2", "2024-01-01 10:00:00"),
    ]
    df = spark.createDataFrame(data, ["user_id", "click_time"])
    df = df.withColumn("click_time", F.col("click_time").cast("timestamp"))

    session_expire_minutes = 30
    w_user = Window.partitionBy("user_id").orderBy("click_time")

    df = df.withColumn(
        "time_diff",
        F.unix_timestamp("click_time")
        - F.lag(F.unix_timestamp("click_time")).over(w_user),
    )

    # Mark the start of a new session when the gap exceeds 30 minutes
    df = df.withColumn(
        "session_id",
        F.when(
            F.col("time_diff").isNull()
            | (F.col("time_diff") > session_expire_minutes * 60),
            F.monotonically_increasing_id(),
        ),
    )

    # Forward-fill the session ID to all rows within the session
    df = df.withColumn(
        "session_id", F.last("session_id", ignorenulls=True).over(w_user)
    )

    w_session = Window.partitionBy("user_id", "session_id")
    df = (
        df.withColumn("session_start_time", F.min("click_time").over(w_session))
        .withColumn("session_end_time", F.max("click_time").over(w_session))
        .withColumn(
            "session_duration",
            (
                F.unix_timestamp("session_end_time")
                - F.unix_timestamp("session_start_time")
            )
            / 60,
        )
    )

    df.show(truncate=False)

    daily_sessions = df.groupBy(F.to_date("click_time").alias("date"), "user_id").agg(
        F.countDistinct("session_id").alias("num_sessions")
    )

    daily_sessions.show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("clickstream-session-analysis")
    main(spark)
    spark.stop()

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DateType, TimestampType,
)

schema = StructType([
    StructField("id",         LongType(),      nullable=False),
    StructField("event",      StringType(),    nullable=True),
    StructField("event_date", DateType(),      nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
])

# Raw strings — cast to date/timestamp inside Spark
RAW_DATA = [
    (1, "signup",   "2024-01-15", "2024-01-15 08:30:00"),
    (2, "login",    "2024-02-20", "2024-02-20 14:00:00"),
    (3, "purchase", "2024-03-10", "2024-03-10 10:15:30"),
    (4, "logout",   "2024-03-10", "2024-03-10 18:45:00"),
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-dates")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Parse strings to proper date/timestamp types
    string_schema = StructType([
        StructField("id",         LongType(),   nullable=False),
        StructField("event",      StringType(), nullable=True),
        StructField("event_date", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
    ])
    df = (spark.createDataFrame(RAW_DATA, schema=string_schema)
          .withColumn("event_date", F.to_date(F.col("event_date"),   "yyyy-MM-dd"))
          .withColumn("created_at", F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss")))

    df.show(truncate=False)
    df.printSchema()

    # Date arithmetic
    print("=== date arithmetic ===")
    (df.select(
        F.col("id"),
        F.col("event"),
        F.col("event_date"),
        F.year(F.col("event_date")).alias("year"),
        F.month(F.col("event_date")).alias("month"),
        F.dayofweek(F.col("event_date")).alias("dow"),
        F.datediff(F.current_date(), F.col("event_date")).alias("days_ago"),
    ).show())

    # Timestamp arithmetic
    print("=== timestamp arithmetic ===")
    (df.select(
        F.col("id"),
        F.col("created_at"),
        F.hour(F.col("created_at")).alias("hour"),
        F.minute(F.col("created_at")).alias("minute"),
        F.unix_timestamp(F.col("created_at")).alias("epoch_seconds"),
        F.date_trunc("hour", F.col("created_at")).alias("truncated_to_hour"),
    ).show(truncate=False))

    # Grouping by date
    print("=== events per day ===")
    (df.groupBy(F.col("event_date"))
       .agg(F.count("*").alias("event_count"))
       .orderBy("event_date")
       .show())

    print("DateType simpleString      :", DateType().simpleString())
    print("TimestampType simpleString :", TimestampType().simpleString())

    spark.stop()

"""Extract structured data from text lines using regular expressions."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("parse_text_regex")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Apache access log parsing ---
    tmp = os.path.join(tempfile.mkdtemp(), "access.log")
    with open(tmp, "w") as f:
        f.write('192.168.1.10 - - [10/Mar/2025:13:55:36] "GET /index.html HTTP/1.1" 200 2326\n')
        f.write('10.0.0.5 - - [10/Mar/2025:13:56:01] "POST /api/users HTTP/1.1" 201 512\n')
        f.write('192.168.1.10 - - [10/Mar/2025:14:00:12] "GET /style.css HTTP/1.1" 304 0\n')
        f.write('172.16.0.1 - - [10/Mar/2025:14:01:45] "DELETE /api/users/5 HTTP/1.1" 403 128\n')

    print("=== Apache log parsing ===")
    df_log = spark.read.text(tmp)
    df_parsed = df_log.select(
        F.regexp_extract("value", r"^(\S+)", 1).alias("ip"),
        F.regexp_extract("value", r"\[(.+?)\]", 1).alias("timestamp"),
        F.regexp_extract("value", r'"(\w+)\s', 1).alias("method"),
        F.regexp_extract("value", r'"\w+\s(\S+)', 1).alias("path"),
        F.regexp_extract("value", r'"\s(\d+)', 1).cast(IntegerType()).alias("status"),
        F.regexp_extract("value", r'"\s\d+\s(\d+)', 1).cast(IntegerType()).alias("bytes"),
    )
    df_parsed.show(truncate=False)

    # --- application log levels ---
    tmp2 = os.path.join(tempfile.mkdtemp(), "app.log")
    with open(tmp2, "w") as f:
        f.write("2025-03-10 08:00:01.123 INFO  [main] Application started\n")
        f.write("2025-03-10 08:00:02.456 WARN  [pool-1] Connection slow: 2500ms\n")
        f.write("2025-03-10 08:00:03.789 ERROR [pool-1] NullPointerException at Service.java:42\n")
        f.write("2025-03-10 08:00:04.012 DEBUG [main] Cache hit ratio: 0.85\n")

    print("=== Application log parsing ===")
    df_app = spark.read.text(tmp2)
    df_app_parsed = df_app.select(
        F.regexp_extract("value", r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})", 1)
        .cast(TimestampType())
        .alias("ts"),
        F.regexp_extract("value", r"\d{3}\s+(\w+)", 1).alias("level"),
        F.regexp_extract("value", r"\[(.+?)\]", 1).alias("thread"),
        F.regexp_extract("value", r"\]\s+(.+)$", 1).alias("message"),
    )
    df_app_parsed.show(truncate=False)

    # --- filter by log level ---
    print("=== Errors only ===")
    df_app_parsed.filter(F.col("level") == "ERROR").show(truncate=False)

    spark.stop()

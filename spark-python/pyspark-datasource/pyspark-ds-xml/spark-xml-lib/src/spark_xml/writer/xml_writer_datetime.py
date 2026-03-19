"""XML writer with date/time formatting.

Demonstrates writing DataFrames with DateType, TimestampType,
and custom date format options for XML output.
"""

import os
import sys
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def create_events_df(spark: SparkSession):
    """Create a DataFrame with date and timestamp columns."""
    schema = StructType([
        StructField("event_id", IntegerType()),
        StructField("title", StringType()),
        StructField("event_date", DateType()),
        StructField("start_time", TimestampType()),
        StructField("end_time", TimestampType()),
        StructField("ticket_price", DoubleType()),
    ])
    data = [
        (1, "Tech Conference", date(2025, 3, 15), datetime(2025, 3, 15, 9, 0, 0), datetime(2025, 3, 15, 17, 30, 0), 299.99),
        (2, "Music Festival", date(2025, 6, 21), datetime(2025, 6, 21, 12, 0, 0), datetime(2025, 6, 22, 2, 0, 0), 150.00),
        (3, "Workshop: PySpark", date(2025, 4, 10), datetime(2025, 4, 10, 10, 0, 0), datetime(2025, 4, 10, 16, 0, 0), 49.99),
        (4, "Art Exhibition", date(2025, 5, 1), datetime(2025, 5, 1, 10, 0, 0), datetime(2025, 5, 31, 18, 0, 0), 25.00),
        (5, "Hackathon", date(2025, 7, 12), datetime(2025, 7, 12, 8, 0, 0), datetime(2025, 7, 13, 20, 0, 0), 0.00),
    ]
    return spark.createDataFrame(data, schema)


if __name__ == "__main__":
    out_dir = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "writer_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(
        app_name="xml-writer-datetime",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = create_events_df(spark)
    print("=== Source DataFrame ===")
    df.printSchema()
    df.show(truncate=False)

    # ── 1. Default date/timestamp format ────────────────────────────
    print("\n=== 1. Default date/timestamp format (ISO-8601) ===")
    default_path = (out_dir / "events_default_dates").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "events")
        .option("rowTag", "event")
        .save(default_path)
    )
    spark.read.format("xml").option("rowTag", "event").load(default_path).show(truncate=False)

    # ── 2. Custom dateFormat ────────────────────────────────────────
    print("\n=== 2. Custom dateFormat (dd/MM/yyyy) ===")
    custom_date_path = (out_dir / "events_custom_date").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "events")
        .option("rowTag", "event")
        .option("dateFormat", "dd/MM/yyyy")
        .save(custom_date_path)
    )
    df_custom_date = (
        spark.read.format("xml")
        .option("rowTag", "event")
        .option("dateFormat", "dd/MM/yyyy")
        .load(custom_date_path)
    )
    print("Read back with matching dateFormat:")
    df_custom_date.show(truncate=False)

    # ── 3. Custom timestampFormat ───────────────────────────────────
    print("\n=== 3. Custom timestampFormat (dd-MM-yyyy HH:mm:ss) ===")
    custom_ts_path = (out_dir / "events_custom_timestamp").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "events")
        .option("rowTag", "event")
        .option("timestampFormat", "dd-MM-yyyy HH:mm:ss")
        .save(custom_ts_path)
    )
    df_custom_ts = (
        spark.read.format("xml")
        .option("rowTag", "event")
        .option("timestampFormat", "dd-MM-yyyy HH:mm:ss")
        .load(custom_ts_path)
    )
    print("Read back with matching timestampFormat:")
    df_custom_ts.show(truncate=False)

    # ── 4. Both dateFormat and timestampFormat together ──────────────
    print("\n=== 4. Combined dateFormat + timestampFormat ===")
    combined_path = (out_dir / "events_combined_formats").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "events")
        .option("rowTag", "event")
        .option("dateFormat", "yyyy/MM/dd")
        .option("timestampFormat", "yyyy/MM/dd HH:mm")
        .save(combined_path)
    )
    df_combined = (
        spark.read.format("xml")
        .option("rowTag", "event")
        .option("dateFormat", "yyyy/MM/dd")
        .option("timestampFormat", "yyyy/MM/dd HH:mm")
        .load(combined_path)
    )
    print("Read back with both formats:")
    df_combined.show(truncate=False)

    # ── 5. Pre-format as strings for full control ───────────────────
    print("\n=== 5. Pre-format dates as strings (full control) ===")
    preformat_path = (out_dir / "events_preformatted").as_posix()
    (
        df.select(
            col("event_id"),
            col("title"),
            date_format(col("event_date"), "MMMM d, yyyy").alias("event_date"),
            date_format(col("start_time"), "h:mm a 'on' EEEE").alias("start_time"),
            date_format(col("end_time"), "h:mm a 'on' EEEE").alias("end_time"),
            col("ticket_price"),
        )
        .write.format("xml")
        .mode("overwrite")
        .option("rootTag", "events")
        .option("rowTag", "event")
        .save(preformat_path)
    )
    print("Pre-formatted output (human-readable dates):")
    spark.read.format("xml").option("rowTag", "event").load(preformat_path).show(truncate=False)

    spark.stop()

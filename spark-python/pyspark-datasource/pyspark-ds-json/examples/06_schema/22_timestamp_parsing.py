"""Timestamp and date parsing — handling mixed and inconsistent time formats in JSON.

Demonstrates strategies for parsing timestamps when JSON contains ISO formats,
epoch milliseconds, custom date strings, empty strings, and invalid text in the
same field.

Key concepts:
    - timestampFormat option for known consistent formats
    - Read as StringType first when formats are mixed
    - Conditional parsing with when/otherwise chains
    - try_to_timestamp with format patterns for custom formats
    - from_unixtime for epoch seconds/milliseconds
    - coalesce across multiple parse attempts (try-parse pattern)

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.timestamp_parsing")


if __name__ == "__main__":
    spark = get_spark("timestamp-parsing")

    # =========================================================================
    # 1. Known consistent format — timestampFormat option
    # =========================================================================
    print_header("1. Known Format — timestampFormat Option")

    iso_file = DATA_HOME + "/timestamps_iso.json"
    write_json_lines(
        iso_file,
        [
            '{"id": 1, "event_time": "2026-08-01T10:15:30Z"}',
            '{"id": 2, "event_time": "2026-08-02T14:30:00Z"}',
            '{"id": 3, "event_time": "2026-08-03T09:45:15Z"}',
        ],
    )
    print_path("Input (ISO format)", iso_file)

    df_iso = (
        spark.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX")
        .schema("id BIGINT, event_time TIMESTAMP")
        .json(iso_file)
    )
    print_schema(df_iso, title="Schema with TimestampType")
    print_dataframe(df_iso, title="Parsed ISO timestamps")
    print_success("timestampFormat option parses consistent formats directly into TimestampType")

    # =========================================================================
    # 2. Custom date format
    # =========================================================================
    print_header("2. Custom Date Format")

    custom_file = DATA_HOME + "/timestamps_custom.json"
    write_json_lines(
        custom_file,
        [
            '{"id": 1, "event_time": "01-08-2026 10:15:30"}',
            '{"id": 2, "event_time": "02-08-2026 14:30:00"}',
            '{"id": 3, "event_time": "03-08-2026 09:45:15"}',
        ],
    )
    print_path("Input (custom format)", custom_file)

    df_custom = (
        spark.read.option("timestampFormat", "dd-MM-yyyy HH:mm:ss")
        .schema("id BIGINT, event_time TIMESTAMP")
        .json(custom_file)
    )
    print_dataframe(df_custom, title="Parsed custom format (dd-MM-yyyy HH:mm:ss)")
    print_success("Any Java SimpleDateFormat pattern works with timestampFormat")

    # =========================================================================
    # 3. The hard scenario — mixed formats in same field
    # =========================================================================
    print_header("3. Mixed Formats in Same Field (Hard Scenario)")

    mixed_file = DATA_HOME + "/timestamps_mixed.json"
    write_json_lines(
        mixed_file,
        [
            '{"id": 1, "event_time": "2026-08-01T10:15:30Z"}',
            '{"id": 2, "event_time": "01-08-2026 10:15:30"}',
            '{"id": 3, "event_time": "1785579330000"}',
            '{"id": 4, "event_time": ""}',
            '{"id": 5, "event_time": "invalid text"}',
            '{"id": 6, "event_time": "2026/08/01 10:15:30"}',
            '{"id": 7, "event_time": null}',
        ],
    )
    print_path("Input (mixed formats)", mixed_file)

    # Read as string first
    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("event_time", StringType(), True),
        ]
    )
    df_raw = spark.read.schema(schema).json(mixed_file)
    print_dataframe(df_raw, title="Read as StringType first")
    print_warning("Mixed formats: ISO, custom, epoch ms, empty, invalid, slash-separated, null")

    # =========================================================================
    # 4. Conditional parsing with when/otherwise
    # =========================================================================
    print_header("4. Conditional Parsing")

    df_parsed = df_raw.withColumn(
        "event_ts",
        F.when(
            F.col("event_time").rlike(r"^\d{13}$"),
            F.from_unixtime(F.col("event_time").cast("bigint") / 1000).cast("timestamp"),
        )
        .when(
            F.col("event_time").rlike(r"^\d{10}$"),
            F.from_unixtime(F.col("event_time").cast("bigint")).cast("timestamp"),
        )
        .when(
            F.col("event_time").rlike(r"^\d{4}-\d{2}-\d{2}T"),
            F.expr("try_to_timestamp(event_time, \"yyyy-MM-dd'T'HH:mm:ssX\")"),
        )
        .when(
            F.col("event_time").rlike(r"^\d{2}-\d{2}-\d{4}"),
            F.expr("try_to_timestamp(event_time, 'dd-MM-yyyy HH:mm:ss')"),
        )
        .when(
            F.col("event_time").rlike(r"^\d{4}/\d{2}/\d{2}"),
            F.expr("try_to_timestamp(event_time, 'yyyy/MM/dd HH:mm:ss')"),
        )
        .otherwise(F.lit(None).cast("timestamp")),
    )

    print_dataframe(
        df_parsed.select("id", "event_time", "event_ts"),
        title="Conditional parsing results",
    )

    # Show which ones failed
    failed = df_parsed.filter(
        F.col("event_ts").isNull() & F.col("event_time").isNotNull() & (F.col("event_time") != "")
    )
    if failed.count() > 0:
        print_dataframe(failed.select("id", "event_time"), title="Failed to parse")
    print_success("when/otherwise chain: try each format in order, null for unparseable")

    # =========================================================================
    # 5. Coalesce try-parse pattern
    # =========================================================================
    print_header("5. Coalesce Try-Parse Pattern")

    df_coalesce = df_raw.withColumn(
        "event_ts",
        F.coalesce(
            F.expr("try_to_timestamp(event_time, \"yyyy-MM-dd'T'HH:mm:ssX\")"),
            F.expr("try_to_timestamp(event_time, 'dd-MM-yyyy HH:mm:ss')"),
            F.expr("try_to_timestamp(event_time, 'yyyy/MM/dd HH:mm:ss')"),
            # Epoch handling needs explicit check (can't coalesce cast failures)
            F.when(
                F.col("event_time").rlike(r"^\d{13}$"),
                F.from_unixtime(F.col("event_time").cast("bigint") / 1000).cast("timestamp"),
            ),
        ),
    )
    print_dataframe(
        df_coalesce.select("id", "event_time", "event_ts"),
        title="Coalesce try-parse (first successful wins)",
    )
    print_success("coalesce() returns the first non-null result — tries each format in order")

    # =========================================================================
    # 6. Epoch seconds vs milliseconds
    # =========================================================================
    print_header("6. Epoch Seconds vs Milliseconds")

    epoch_file = DATA_HOME + "/timestamps_epoch.json"
    write_json_lines(
        epoch_file,
        [
            '{"id": 1, "ts_seconds": 1785579330, "ts_millis": 1785579330000}',
            '{"id": 2, "ts_seconds": 1785665730, "ts_millis": 1785665730000}',
        ],
    )

    df_epoch = spark.read.json(epoch_file)

    df_epoch_parsed = df_epoch.select(
        "id",
        F.from_unixtime(F.col("ts_seconds")).cast("timestamp").alias("from_seconds"),
        F.from_unixtime(F.col("ts_millis") / 1000).cast("timestamp").alias("from_millis"),
    )
    print_dataframe(df_epoch_parsed, title="Epoch → Timestamp")
    print_success("Seconds: from_unixtime(col). Milliseconds: from_unixtime(col / 1000)")

    # =========================================================================
    # 7. dateFormat option for date-only fields
    # =========================================================================
    print_header("7. dateFormat for Date-Only Fields")

    date_file = DATA_HOME + "/timestamps_dates.json"
    write_json_lines(
        date_file,
        [
            '{"id": 1, "birth_date": "01/08/1990"}',
            '{"id": 2, "birth_date": "15/03/1985"}',
            '{"id": 3, "birth_date": "22/12/2000"}',
        ],
    )

    df_dates = (
        spark.read.option("dateFormat", "dd/MM/yyyy")
        .schema("id BIGINT, birth_date DATE")
        .json(date_file)
    )
    print_schema(df_dates, title="DateType schema")
    print_dataframe(df_dates, title="Parsed dates with dateFormat option")
    print_success("Use dateFormat for DATE columns, timestampFormat for TIMESTAMP columns")

    # =========================================================================
    # 8. Timezone handling
    # =========================================================================
    print_header("8. Timezone Handling")

    tz_file = DATA_HOME + "/timestamps_timezone.json"
    write_json_lines(
        tz_file,
        [
            '{"id": 1, "event_time": "2026-08-01T10:15:30+05:30"}',
            '{"id": 2, "event_time": "2026-08-01T10:15:30-04:00"}',
            '{"id": 3, "event_time": "2026-08-01T10:15:30Z"}',
        ],
    )

    # Default: converts to session timezone
    df_tz = (
        spark.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")
        .schema("id BIGINT, event_time TIMESTAMP")
        .json(tz_file)
    )
    session_tz = spark.conf.get("spark.sql.session.timeZone")
    logger.info("Session timezone: %s", session_tz)
    print_dataframe(df_tz, title=f"Timestamps normalized to session TZ ({session_tz})")
    print_success("Spark converts all timezone-aware timestamps to the session timezone")

    # =========================================================================
    # 9. Flagging unparseable values
    # =========================================================================
    print_header("9. Production Pattern — Flag Unparseable Values")

    df_flagged = df_raw.withColumn(
        "event_ts",
        F.coalesce(
            F.expr("try_to_timestamp(event_time, \"yyyy-MM-dd'T'HH:mm:ssX\")"),
            F.expr("try_to_timestamp(event_time, 'dd-MM-yyyy HH:mm:ss')"),
            F.expr("try_to_timestamp(event_time, 'yyyy/MM/dd HH:mm:ss')"),
            F.when(
                F.col("event_time").rlike(r"^\d{13}$"),
                F.from_unixtime(F.col("event_time").cast("bigint") / 1000).cast("timestamp"),
            ),
        ),
    ).withColumn(
        "parse_status",
        F.when(F.col("event_time").isNull(), "null_input")
        .when(F.col("event_time") == "", "empty_input")
        .when(F.col("event_ts").isNotNull(), "success")
        .otherwise("parse_failed"),
    )

    print_dataframe(
        df_flagged.select("id", "event_time", "event_ts", "parse_status"),
        title="With parse status flag",
    )

    # Summary
    status_summary = df_flagged.groupBy("parse_status").count()
    print_dataframe(status_summary, title="Parse status summary")
    print_success(
        "Flag each record's parse status for monitoring — "
        "alert when parse_failed exceeds threshold"
    )

    spark.stop()

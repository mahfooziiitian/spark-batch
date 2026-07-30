"""Formatting options — timestamps, dates, timezones, and line separators.

Demonstrates JSON read/write options that control how temporal values
are formatted and how records are delimited.

Key concepts:
    - timestampFormat: custom timestamp pattern (default: yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX])
    - timestampNTZFormat: timestamp without timezone (default: yyyy-MM-dd'T'HH:mm:ss[.SSS])
    - dateFormat: custom date pattern (default: yyyy-MM-dd)
    - timeZone: timezone ID for timestamp interpretation
    - lineSep: record delimiter (default: \n, also \r\n, custom)
    - enableDateTimeParsingFallback: Spark 1.x/2.0 backward compatibility

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
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
logger = get_logger("example.formatting")

OUTPUT_DIR = Path(DATA_HOME) / "output" / "examples" / "04_properties" / "03_formatting"


def _write_records_file(name: str, records: list[dict[str, object]]) -> str:
    """Write JSON lines data for a section and return the file path."""
    path = OUTPUT_DIR / f"{name}.json"
    lines = [json.dumps(record) for record in records]
    return write_json_lines(str(path), lines)


def _first_part_file(directory: Path) -> Path:
    """Return the first Spark JSON part file under a directory."""
    return next(path for path in directory.iterdir() if path.name.startswith("part-"))


if __name__ == "__main__":
    spark = get_spark("formatting-options")

    # =========================================================================
    # 1. timestampFormat
    # =========================================================================
    print_header("1. timestampFormat")

    timestamp_path = _write_records_file(
        "timestamp_format",
        [
            {"event": "start", "observed_at": "2024-03-15 10:30:00"},
            {"event": "end", "observed_at": "2024-03-15 17:45:30"},
        ],
    )
    print_path("Input", timestamp_path)
    logger.info("Reading timestamp data from %s", timestamp_path)

    timestamp_schema = StructType(
        [
            StructField("event", StringType(), True),
            StructField("observed_at", TimestampType(), True),
        ]
    )
    df_timestamp = (
        spark.read.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").schema(timestamp_schema).json(timestamp_path)
    )

    print_schema(df_timestamp, title="timestampFormat Schema")
    print_dataframe(df_timestamp, title="timestampFormat Data")
    print_success("Custom timestampFormat parsed timestamp strings into TimestampType")

    # =========================================================================
    # 2. dateFormat
    # =========================================================================
    print_header("2. dateFormat")

    date_path = _write_records_file(
        "date_format",
        [
            {"name": "Alice", "birth_date": "15/03/2024"},
            {"name": "Bob", "birth_date": "16/03/2024"},
        ],
    )
    print_path("Input", date_path)
    logger.info("Reading date data from %s", date_path)

    date_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("birth_date", DateType(), True),
        ]
    )
    df_date = spark.read.option("dateFormat", "dd/MM/yyyy").schema(date_schema).json(date_path)

    print_schema(df_date, title="dateFormat Schema")
    print_dataframe(df_date, title="dateFormat Data")
    print_success("Custom dateFormat parsed dd/MM/yyyy strings into DateType")

    # =========================================================================
    # 3. timestampNTZFormat
    # =========================================================================
    print_header("3. timestampNTZFormat")

    ntz_path = _write_records_file(
        "timestamp_ntz_format",
        [
            {"event": "open", "captured_at": "2024-03-15T10:30:00.123"},
            {"event": "close", "captured_at": "2024-03-15T17:45:30.456"},
        ],
    )
    print_path("Input", ntz_path)
    logger.info("Reading timestamp_ntz data from %s", ntz_path)

    ntz_schema = StructType(
        [
            StructField("event", StringType(), True),
            StructField("captured_at", TimestampNTZType(), True),
        ]
    )
    df_ntz = spark.read.option("timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS").schema(ntz_schema).json(ntz_path)

    print_schema(df_ntz, title="timestampNTZFormat Schema")
    print_dataframe(df_ntz, title="timestampNTZFormat Data")
    print_success("timestampNTZFormat parsed local timestamps without a timezone component")

    # =========================================================================
    # 4. timeZone
    # =========================================================================
    print_header("4. timeZone")

    timezone_path = _write_records_file(
        "timezone",
        [
            {"event": "meeting", "scheduled_at": "2024-03-15T10:30:00"},
            {"event": "deploy", "scheduled_at": "2024-03-15T18:00:00"},
        ],
    )
    print_path("Input", timezone_path)
    logger.info("Reading timezone-sensitive data from %s", timezone_path)

    timezone_schema = StructType(
        [
            StructField("event", StringType(), True),
            StructField("scheduled_at", TimestampType(), True),
        ]
    )
    df_timezone_ny = (
        spark.read.option("timeZone", "America/New_York")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
        .schema(timezone_schema)
        .json(timezone_path)
    )
    df_timezone_kolkata = (
        spark.read.option("timeZone", "Asia/Kolkata")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
        .schema(timezone_schema)
        .json(timezone_path)
    )

    print_schema(df_timezone_ny, title="timeZone Schema")
    print_dataframe(df_timezone_ny, title="Read with America/New_York")
    print_dataframe(df_timezone_kolkata, title="Read with Asia/Kolkata")
    print_warning("Different timeZone values interpret the same zone-less timestamp text as different instants.")

    # =========================================================================
    # 5. lineSep
    # =========================================================================
    print_header("5. lineSep")

    df_line_sep = spark.createDataFrame(
        [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
        ],
        ["id", "name"],
    )

    newline_dir = OUTPUT_DIR / "line_sep_lf"
    crlf_dir = OUTPUT_DIR / "line_sep_crlf"

    df_line_sep.coalesce(1).write.mode("overwrite").option("lineSep", "\n").json(str(newline_dir))
    df_line_sep.coalesce(1).write.mode("overwrite").option("lineSep", "\r\n").json(str(crlf_dir))

    print_path("LF Output", str(newline_dir))
    print_path("CRLF Output", str(crlf_dir))

    df_line_sep_lf = spark.read.option("lineSep", "\n").json(str(newline_dir))
    df_line_sep_crlf = spark.read.option("lineSep", "\r\n").json(str(crlf_dir))

    lf_part = _first_part_file(newline_dir)
    crlf_part = _first_part_file(crlf_dir)
    logger.debug("LF raw bytes sample: %s", lf_part.read_bytes())
    logger.debug("CRLF raw bytes sample: %s", crlf_part.read_bytes())

    print_schema(df_line_sep_lf, title="lineSep Schema")
    print_dataframe(df_line_sep_lf, title="Read Back with \\n")
    print_dataframe(df_line_sep_crlf, title="Read Back with \\r\\n")
    print_success("Default JSON lineSep is \\n; CRLF files should be read with the matching separator")

    # =========================================================================
    # 6. enableDateTimeParsingFallback
    # =========================================================================
    print_header("6. enableDateTimeParsingFallback")

    fallback_path = _write_records_file(
        "date_time_parsing_fallback",
        [
            {"event": "legacy_iso", "occurred_at": "2024-03-15T10:30:00.000+00:00"},
            {"event": "legacy_space", "occurred_at": "2024-03-16 11:45:00"},
            {"event": "date_only", "occurred_at": "2024-03-17"},
        ],
    )
    print_path("Input", fallback_path)
    logger.info("Reading mixed temporal values from %s", fallback_path)

    fallback_schema = StructType(
        [
            StructField("event", StringType(), True),
            StructField("occurred_at", TimestampType(), True),
        ]
    )
    df_fallback = spark.read.option("enableDateTimeParsingFallback", "true").schema(fallback_schema).json(fallback_path)

    print_schema(df_fallback, title="enableDateTimeParsingFallback Schema")
    print_dataframe(df_fallback, title="Fallback Parsing Data")
    print_warning("enableDateTimeParsingFallback keeps compatibility with Spark 1.x/2.0 date-time parsing rules.")

    spark.stop()

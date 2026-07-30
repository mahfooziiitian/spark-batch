"""Null field handling — control how nulls appear in JSON output and schema.

Demonstrates options for handling null values during JSON read and write:
ignoreNullFields (write) and dropFieldIfAllNull (read).

Key concepts:
    - ignoreNullFields (write): omit null-valued fields from JSON output
    - dropFieldIfAllNull (read): exclude columns that are entirely null during schema inference
    - Default: ignoreNullFields=true in Spark 3.x+, dropFieldIfAllNull=false

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile

from pyspark.sql import DataFrame, Row

from pys_json import (
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.null_fields")

OUT_DIR = os.path.join(tempfile.gettempdir(), "pys_json", "null_fields")


def _reset_output_dir(path: str) -> None:
    """Recreate an output directory from scratch."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def _read_part_files(directory: str) -> list[str]:
    """Return raw JSON lines written by Spark part files."""
    part_files = sorted(
        filename for filename in os.listdir(directory) if filename.startswith("part-") and not filename.endswith(".crc")
    )

    raw_lines: list[str] = []
    for filename in part_files:
        file_path = os.path.join(directory, filename)
        with open(file_path, encoding="utf-8") as handle:
            raw_lines.extend(line.strip() for line in handle if line.strip())
    return raw_lines


def _log_raw_json(label: str, raw_lines: list[str]) -> None:
    """Log raw JSON lines for visual comparison."""
    logger.info("%s raw JSON line count: %s", label, len(raw_lines))
    for line in raw_lines:
        logger.info("%s raw JSON: %s", label, line)


def _write_json_lines(path: str, records: list[dict[str, object]]) -> None:
    """Write newline-delimited JSON records."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def _display_dataset(df: DataFrame, schema_title: str, data_title: str) -> None:
    """Render schema and rows with the shared example helpers."""
    print_schema(df, title=schema_title)
    print_dataframe(df, title=data_title)


if __name__ == "__main__":
    spark = get_spark("null-fields")

    logger.info("Output directory: %s", OUT_DIR)

    null_rows = [
        Row(id=1, name="Alice", age=30, email=None),
        Row(id=2, name="Bob", age=None, email="bob@example.com"),
        Row(id=3, name="Cara", age=None, email=None),
    ]
    null_df = spark.createDataFrame(null_rows)

    try:
        # =========================================================================
        # 1. ignoreNullFields=true (default)
        # =========================================================================
        print_header("1. ignoreNullFields=true (default)")

        out_ignore_true = os.path.join(OUT_DIR, "ignore_null_fields_true")
        _reset_output_dir(out_ignore_true)

        _display_dataset(null_df, "Input Schema", "Input Data")
        null_df.coalesce(1).write.mode("overwrite").option("ignoreNullFields", "true").json(out_ignore_true)

        raw_ignore_true = _read_part_files(out_ignore_true)
        _log_raw_json("ignoreNullFields=true", raw_ignore_true)
        print_warning("Raw JSON logged above omits keys whose values were null.")

        df_ignore_true = spark.read.json(out_ignore_true)
        _display_dataset(df_ignore_true, "Read-Back Schema", "Read-Back Data")

        # =========================================================================
        # 2. ignoreNullFields=false
        # =========================================================================
        print_header("2. ignoreNullFields=false")

        out_ignore_false = os.path.join(OUT_DIR, "ignore_null_fields_false")
        _reset_output_dir(out_ignore_false)

        null_df.coalesce(1).write.mode("overwrite").option("ignoreNullFields", "false").json(out_ignore_false)

        raw_ignore_false = _read_part_files(out_ignore_false)
        _log_raw_json("ignoreNullFields=false", raw_ignore_false)
        print_warning('Raw JSON logged above preserves missing values as explicit "null" entries.')

        df_ignore_false = spark.read.json(out_ignore_false)
        _display_dataset(df_ignore_false, "Read-Back Schema", "Read-Back Data")

        # =========================================================================
        # 3. dropFieldIfAllNull=true (read)
        # =========================================================================
        print_header("3. dropFieldIfAllNull=true (read)")

        input_dir = os.path.join(OUT_DIR, "drop_field_if_all_null")
        input_file = os.path.join(input_dir, "records.json")
        _reset_output_dir(input_dir)

        all_null_records = [
            {"id": 1, "name": "Alpha", "comment": None, "status": "new"},
            {"id": 2, "name": "Beta", "comment": None, "status": "active"},
            {"id": 3, "name": "Gamma", "comment": None, "status": "done"},
        ]
        _write_json_lines(input_file, all_null_records)
        _log_raw_json("dropFieldIfAllNull input", [json.dumps(record) for record in all_null_records])

        df_drop_true = spark.read.option("dropFieldIfAllNull", "true").json(input_file)
        _display_dataset(df_drop_true, "Schema with dropFieldIfAllNull=true", "Data with All-Null Field Dropped")
        logger.info("Columns with dropFieldIfAllNull=true: %s", df_drop_true.columns)
        print_warning("The comment column is excluded because it is null in every record during schema inference.")

        # =========================================================================
        # 4. dropFieldIfAllNull=false (default)
        # =========================================================================
        print_header("4. dropFieldIfAllNull=false (default)")

        df_drop_false = spark.read.json(input_file)
        _display_dataset(df_drop_false, "Default Inferred Schema", "Data with All-Null Field Retained")
        logger.info("Columns with dropFieldIfAllNull=false: %s", df_drop_false.columns)

        print_success(
            "Use ignoreNullFields to control JSON output size and use dropFieldIfAllNull to simplify inferred schemas."
        )
    finally:
        spark.stop()

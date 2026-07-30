"""Character encoding — read and write JSON with different encodings.

Demonstrates the encoding option for reading JSON files in various
character sets and writing with specific encodings.

Key concepts:
    - encoding option: UTF-8 (default), UTF-16, UTF-16BE, UTF-16LE, UTF-32
    - Read: spark.read.option("encoding", "UTF-16").json(path)
    - Write: df.write.option("encoding", "UTF-16").json(path)
    - multiLine is usually needed for non-UTF-8 files
    - JSON built-in functions ignore this option

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.encoding")


def _reset_path(path: Path) -> None:
    """Remove an existing output directory so each run starts clean."""
    if path.exists():
        shutil.rmtree(path)
        logger.debug("Removed existing path %s", path)


def _write_and_read(
    spark: SparkSession,
    df: DataFrame,
    path: Path,
    encoding: str,
    multiline: bool = False,
) -> DataFrame:
    """Write a DataFrame as JSON and read it back with the requested encoding."""
    _reset_path(path)
    logger.info("Writing JSON with encoding=%s to %s", encoding, path)
    df.coalesce(1).write.mode("overwrite").option("encoding", encoding).json(str(path))

    reader = spark.read.option("encoding", encoding)
    if multiline:
        reader = reader.option("multiLine", True)

    logger.info("Reading JSON with encoding=%s multiLine=%s from %s", encoding, multiline, path)
    return reader.json(str(path))


def _aligned_rows(df: DataFrame, columns: list[str]) -> list[tuple]:
    """Collect rows using a fixed column order for reliable comparisons."""
    return [tuple(row[column] for column in columns) for row in df.select(*columns).collect()]


def main() -> None:
    """Run the encoding example."""
    output_root = Path(DATA_HOME) / "output" / "examples" / "04_properties" / "encoding"
    output_root.mkdir(parents=True, exist_ok=True)

    columns = ["id", "latin_name", "cjk_text", "city"]
    records = [
        {
            "id": 1,
            "latin_name": "Ñoño",
            "cjk_text": "日本語",
            "city": "München",
        }
    ]

    reference_file = output_root / "reference" / "source.json"
    write_json_lines(
        str(reference_file),
        [json.dumps(record, ensure_ascii=False) for record in records],
    )
    logger.info("Wrote reference JSON lines file to %s", reference_file)

    source_df = spark.createDataFrame(records).select(*columns)

    print_warning("Outputs are written under DATA_HOME/output because temporary directories are not allowed here.")

    print_header("1. UTF-8 (default)")
    print_schema(source_df, title="Source Schema")
    print_dataframe(source_df, title="Source Unicode Data")
    utf8_df = _write_and_read(spark, source_df, output_root / "utf8", "UTF-8")
    print_schema(utf8_df, title="UTF-8 Read Schema")
    print_dataframe(utf8_df, title="UTF-8 Read Back")
    print_success("UTF-8 is Spark's default JSON encoding, so no read option is required.")

    print_header("2. UTF-16")
    utf16_df = _write_and_read(spark, source_df, output_root / "utf16", "UTF-16", multiline=True)
    print_schema(utf16_df, title="UTF-16 Read Schema")
    print_dataframe(utf16_df, title="UTF-16 Read Back")
    print_warning("UTF-16 writes a BOM and Spark reads it back reliably when multiLine=True is enabled.")

    print_header("3. UTF-16BE / UTF-16LE")
    utf16be_df = _write_and_read(spark, source_df, output_root / "utf16be", "UTF-16BE", multiline=True)
    utf16le_df = _write_and_read(spark, source_df, output_root / "utf16le", "UTF-16LE", multiline=True)
    print_dataframe(utf16be_df, title="UTF-16BE Read Back")
    print_dataframe(utf16le_df, title="UTF-16LE Read Back")
    logger.info("UTF-16BE uses big-endian byte order; UTF-16LE uses little-endian byte order.")
    print_success("Both UTF-16 byte orders preserve the same characters when read with the matching encoding.")

    print_header("4. UTF-32")
    utf32_df = _write_and_read(spark, source_df, output_root / "utf32", "UTF-32", multiline=True)
    print_schema(utf32_df, title="UTF-32 Read Schema")
    print_dataframe(utf32_df, title="UTF-32 Read Back")
    print_warning("UTF-32 uses the most space and is rarely used for JSON interchange.")

    print_header("5. Encoding round-trip verification")
    round_trip_df = _write_and_read(spark, source_df, output_root / "round_trip_utf16", "UTF-16", multiline=True)
    if _aligned_rows(source_df, columns) == _aligned_rows(round_trip_df, columns):
        print_success("UTF-16 round-trip verification passed: the original Unicode data was preserved.")
    else:
        print_warning("UTF-16 round-trip verification failed: the read-back data differs from the source.")


if __name__ == "__main__":
    spark = get_spark("encoding-example")
    try:
        main()
    finally:
        spark.stop()

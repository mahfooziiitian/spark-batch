"""Parsing rules — relaxed JSON parsing options.

Demonstrates options that relax the strict JSON parser to handle
non-standard JSON formats commonly found in real-world data.

Key concepts:
    - allowComments: parse JSON with // and /* */ comments
    - allowUnquotedFieldNames: accept {name: "value"} without quoted keys
    - allowSingleQuotes: accept {'name': 'value'} with single quotes
    - allowNumericLeadingZeros: accept 007, 0123
    - allowNonNumericNumbers: accept NaN, Infinity, -Infinity
    - allowBackslashEscapingAnyCharacter: accept \\q, \\' etc.
    - All options default to false

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from __future__ import annotations

from pathlib import Path

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
logger = get_logger("example.parsing_rules")

BASE_DIR = Path(DATA_HOME) / "examples" / "04_properties" / "05_parsing_rules"


def _write_raw_file(name: str, content: str) -> str:
    """Write raw text content for examples that need invalid JSON syntax."""
    path = BASE_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    logger.debug("Wrote raw example file to %s", path)
    return str(path)


def _write_lines_file(name: str, lines: list[str]) -> str:
    """Write newline-delimited JSON-like records for an example."""
    path = BASE_DIR / name
    logger.debug("Writing %d records to %s", len(lines), path)
    return write_json_lines(str(path), lines)


if __name__ == "__main__":
    spark = get_spark("parsing-rules")

    print_header("1. allowComments")
    comments_file = _write_raw_file(
        "comments.json",
        '// This is a comment\n{"id": 1, "name": "Alice"}\n/* Block comment */\n{"id": 2, "name": "Bob"}\n',
    )
    logger.info("Reading comment-friendly JSON from %s", comments_file)
    df_comments = spark.read.option("allowComments", "true").option("multiLine", False).json(comments_file)
    print_schema(df_comments, title="Comments Schema")
    print_dataframe(df_comments, title="allowComments Result")
    print_success("Spark ignored both line comments and block comments")

    print_header("2. allowUnquotedFieldNames")
    unquoted_file = _write_lines_file(
        "unquoted_fields.json",
        [
            '{id: 1, name: "Alice"}',
            '{id: 2, name: "Bob"}',
        ],
    )
    logger.info("Reading unquoted field names from %s", unquoted_file)
    df_unquoted = spark.read.option("allowUnquotedFieldNames", "true").json(unquoted_file)
    print_schema(df_unquoted, title="Unquoted Fields Schema")
    print_dataframe(df_unquoted, title="allowUnquotedFieldNames Result")
    print_success("Spark accepted object keys without double quotes")

    print_header("3. allowSingleQuotes")
    single_quotes_file = _write_lines_file(
        "single_quotes.json",
        [
            "{'id': 1, 'name': 'Alice'}",
            "{'id': 2, 'name': 'Bob'}",
        ],
    )
    logger.info("Reading single-quoted JSON from %s", single_quotes_file)
    df_single_quotes = spark.read.option("allowSingleQuotes", "true").json(single_quotes_file)
    print_schema(df_single_quotes, title="Single Quotes Schema")
    print_dataframe(df_single_quotes, title="allowSingleQuotes Result")
    print_warning("Spark already enables allowSingleQuotes by default; this option is explicit for clarity")

    print_header("4. allowNumericLeadingZeros")
    leading_zeros_file = _write_lines_file(
        "leading_zeros.json",
        [
            '{"id": 007, "code": 0042}',
            '{"id": 010, "code": 0009}',
        ],
    )
    logger.info("Reading numeric values with leading zeros from %s", leading_zeros_file)
    df_leading_zeros = spark.read.option("allowNumericLeadingZeros", "true").json(leading_zeros_file)
    print_schema(df_leading_zeros, title="Leading Zeros Schema")
    print_dataframe(df_leading_zeros, title="allowNumericLeadingZeros Result")
    print_warning("Leading zeros are accepted on input, but Spark parses them as numeric values like 7 and 42")

    print_header("5. allowNonNumericNumbers")
    non_numeric_file = _write_lines_file(
        "non_numeric_numbers.json",
        [
            '{"temp": NaN, "max": Infinity, "min": -Infinity}',
            '{"temp": 21.5, "max": 30.0, "min": 18.0}',
        ],
    )
    logger.info("Reading NaN and Infinity values from %s", non_numeric_file)
    df_non_numeric = spark.read.option("allowNonNumericNumbers", "true").json(non_numeric_file)
    print_schema(df_non_numeric, title="Non-Numeric Numbers Schema")
    print_dataframe(df_non_numeric, title="allowNonNumericNumbers Result")
    print_success("Special floating-point literals were parsed as Spark double values")

    print_header("6. allowBackslashEscapingAnyCharacter")
    backslash_file = _write_raw_file(
        "backslash_escaping.json",
        r'{"path": "C:\logs\qa\data", "note": "It\'s fine"}' + "\n",
    )
    logger.info("Reading backslash-escaped characters from %s", backslash_file)
    df_backslash = spark.read.option("allowBackslashEscapingAnyCharacter", "true").json(backslash_file)
    print_schema(df_backslash, title="Backslash Escaping Schema")
    print_dataframe(df_backslash, title="allowBackslashEscapingAnyCharacter Result")
    print_warning("Backslashes act as escape prefixes here, so literal backslashes are not preserved as-is")

    print_success("Relaxed parsing options help ingest real-world messy JSON before downstream cleanup")
    spark.stop()

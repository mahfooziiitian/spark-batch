"""Additional parsing options — control chars, trailing commas, and file filters.

Demonstrates JSON parsing options not covered by other property scripts,
plus generic DataSource file-level options that apply to JSON reads.

Key concepts:
    - allowUnquotedControlChars: accept unescaped control characters in strings
    - allowTrailingComma: accept trailing commas in objects and arrays (Spark 3.5+)
    - pathGlobFilter: filter input files by glob pattern
    - recursiveFileLookup: recurse into subdirectories
    - modifiedBefore / modifiedAfter: filter files by modification timestamp

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.additional_options")


if __name__ == "__main__":
    spark = get_spark("additional-options")
    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "additional_opts")
    os.makedirs(out_dir, exist_ok=True)

    # =========================================================================
    # 1. allowUnquotedControlChars
    # =========================================================================
    print_header("1. allowUnquotedControlChars")

    # JSON with unescaped tab character inside a string value
    ctrl_file = DATA_HOME + "/opts_control_chars.json"
    # Write raw bytes containing a literal tab in a string
    Path(ctrl_file).write_text(
        '{"id": 1, "note": "line1\tcolumn2"}\n'
        '{"id": 2, "note": "has\nnewline"}\n',
        encoding="utf-8",
    )

    schema = "id INT, note STRING"

    # Without the option — strict parser may reject control chars
    df_strict = (
        spark.read
        .schema("id INT, note STRING, _corrupt_record STRING")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(ctrl_file)
    ).cache()
    strict_bad = df_strict.filter("_corrupt_record IS NOT NULL").count()
    logger.info("Strict mode: %d records with control chars treated as corrupt", strict_bad)
    df_strict.unpersist()

    # With allowUnquotedControlChars=true
    df_relaxed = (
        spark.read
        .schema(schema)
        .option("allowUnquotedControlChars", "true")
        .json(ctrl_file)
    )
    print_dataframe(df_relaxed, title="allowUnquotedControlChars=true")
    print_success(
        "allowUnquotedControlChars allows tab, newline, etc. inside JSON string values"
    )

    # =========================================================================
    # 2. allowTrailingComma (Spark 3.5+)
    # =========================================================================
    print_header("2. allowTrailingComma")

    trailing_file = DATA_HOME + "/opts_trailing_comma.json"
    write_json_lines(
        trailing_file,
        [
            '{"id": 1, "name": "Alice",}',
            '{"id": 2, "tags": ["a", "b",],}',
        ],
    )

    # Without option — trailing commas are invalid JSON
    df_no_trailing = (
        spark.read
        .schema("id INT, name STRING, tags ARRAY<STRING>, _corrupt_record STRING")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(trailing_file)
    ).cache()
    corrupt_count = df_no_trailing.filter("_corrupt_record IS NOT NULL").count()
    logger.info("Without allowTrailingComma: %d corrupt records", corrupt_count)
    df_no_trailing.unpersist()

    # With allowTrailingComma=true
    df_trailing = (
        spark.read
        .schema("id INT, name STRING, tags ARRAY<STRING>")
        .option("allowTrailingComma", "true")
        .json(trailing_file)
    )
    print_dataframe(df_trailing, title="allowTrailingComma=true")
    print_success("allowTrailingComma tolerates {\"key\": \"val\",} and [1, 2,]")

    # =========================================================================
    # 3. pathGlobFilter — filter input files by pattern
    # =========================================================================
    print_header("3. pathGlobFilter")

    glob_dir = os.path.join(out_dir, "glob_test")
    os.makedirs(glob_dir, exist_ok=True)

    # Create mixed files in one directory
    write_json_lines(
        os.path.join(glob_dir, "users.json"),
        ['{"id": 1, "type": "user"}', '{"id": 2, "type": "user"}'],
    )
    write_json_lines(
        os.path.join(glob_dir, "orders.json"),
        ['{"id": 100, "type": "order"}', '{"id": 101, "type": "order"}'],
    )
    write_json_lines(
        os.path.join(glob_dir, "metadata.csv"),
        ["id,type", "1,user"],  # Not JSON — should be filtered out
    )

    # Read only *users*.json
    df_glob = (
        spark.read
        .schema("id INT, type STRING")
        .option("pathGlobFilter", "*.json")
        .json(glob_dir)
    )
    print_dataframe(df_glob, title="pathGlobFilter=*.json (excludes .csv)")

    # More specific filter
    df_users_only = (
        spark.read
        .schema("id INT, type STRING")
        .option("pathGlobFilter", "users*")
        .json(glob_dir)
    )
    print_dataframe(df_users_only, title="pathGlobFilter=users* (only user files)")
    print_success("pathGlobFilter selects files matching a glob within the input path")

    # =========================================================================
    # 4. recursiveFileLookup — recurse into subdirectories
    # =========================================================================
    print_header("4. recursiveFileLookup")

    recursive_dir = os.path.join(out_dir, "recursive_test")
    sub1 = os.path.join(recursive_dir, "region_us")
    sub2 = os.path.join(recursive_dir, "region_eu")
    os.makedirs(sub1, exist_ok=True)
    os.makedirs(sub2, exist_ok=True)

    write_json_lines(
        os.path.join(sub1, "data.json"),
        ['{"id": 1, "region": "us"}', '{"id": 2, "region": "us"}'],
    )
    write_json_lines(
        os.path.join(sub2, "data.json"),
        ['{"id": 3, "region": "eu"}', '{"id": 4, "region": "eu"}'],
    )

    # Without recursiveFileLookup — only reads top-level (partition discovery)
    # With recursiveFileLookup=true — reads all subdirectories, no partition inference
    df_recursive = (
        spark.read
        .schema("id INT, region STRING")
        .option("recursiveFileLookup", "true")
        .json(recursive_dir)
    )
    print_dataframe(df_recursive, title="recursiveFileLookup=true")
    logger.info("Total rows from subdirectories: %d", df_recursive.count())
    print_success(
        "recursiveFileLookup=true reads all nested subdirectories; "
        "disables partition inference"
    )

    # =========================================================================
    # 5. modifiedBefore / modifiedAfter — file timestamp filtering
    # =========================================================================
    print_header("5. modifiedBefore / modifiedAfter")

    ts_dir = os.path.join(out_dir, "timestamp_test")
    os.makedirs(ts_dir, exist_ok=True)

    # Create a file
    old_file = os.path.join(ts_dir, "old_data.json")
    write_json_lines(old_file, ['{"id": 1, "age": "old"}'])

    # Small delay then create another
    time.sleep(1)
    cutoff_time = "2099-01-01T00:00:00"  # Far future — all files are "before" this

    new_file = os.path.join(ts_dir, "new_data.json")
    write_json_lines(new_file, ['{"id": 2, "age": "new"}'])

    # modifiedBefore — files modified before the timestamp
    df_before = (
        spark.read
        .schema("id INT, age STRING")
        .option("modifiedBefore", cutoff_time)
        .option("recursiveFileLookup", "true")
        .json(ts_dir)
    )
    print_dataframe(df_before, title=f"modifiedBefore={cutoff_time}")

    # modifiedAfter with a past timestamp — should include all
    df_after = (
        spark.read
        .schema("id INT, age STRING")
        .option("modifiedAfter", "2020-01-01T00:00:00")
        .option("recursiveFileLookup", "true")
        .json(ts_dir)
    )
    print_dataframe(df_after, title="modifiedAfter=2020-01-01 (includes all)")
    print_success(
        "modifiedBefore/After filter files by OS modification timestamp "
        "(format: yyyy-MM-dd'T'HH:mm:ss)"
    )

    # =========================================================================
    # 6. Combining options
    # =========================================================================
    print_header("6. Combining Multiple Options")

    combo_dir = os.path.join(out_dir, "combo_test")
    sub_a = os.path.join(combo_dir, "2026", "07")
    os.makedirs(sub_a, exist_ok=True)

    write_json_lines(
        os.path.join(sub_a, "events.json"),
        [
            '{"id": 1, "note": "has\ttab",}',
            '{"id": 2, "note": "normal",}',
        ],
    )
    write_json_lines(
        os.path.join(sub_a, "skip.txt"),
        ["not json"],
    )

    df_combo = (
        spark.read
        .schema("id INT, note STRING")
        .option("allowUnquotedControlChars", "true")
        .option("allowTrailingComma", "true")
        .option("pathGlobFilter", "*.json")
        .option("recursiveFileLookup", "true")
        .json(combo_dir)
    )
    print_dataframe(df_combo, title="Combined: control chars + trailing comma + glob + recursive")
    print_success("All options compose cleanly for production ingestion")

    # =========================================================================
    # Summary
    # =========================================================================
    print_header("Summary — Additional JSON Options")

    summary = [
        ("allowUnquotedControlChars", "Read", "Accept unescaped \\t \\n in strings"),
        ("allowTrailingComma", "Read", "Accept trailing commas in objects/arrays"),
        ("pathGlobFilter", "Read", "Filter input files by glob pattern"),
        ("recursiveFileLookup", "Read", "Recurse into subdirectories"),
        ("modifiedBefore", "Read", "Files modified before timestamp"),
        ("modifiedAfter", "Read", "Files modified after timestamp"),
    ]
    df_summary = spark.createDataFrame(summary, ["Option", "Direction", "Description"])
    print_dataframe(df_summary, title="Additional options reference")

    spark.stop()

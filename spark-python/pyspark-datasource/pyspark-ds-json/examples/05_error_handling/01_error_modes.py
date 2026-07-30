"""Error handling modes — PERMISSIVE, DROPMALFORMED, and FAILFAST.

Demonstrates the three JSON parsing modes that control how Spark handles
corrupt or malformed records during JSON reads.

Key concepts:
    - PERMISSIVE (default): keeps corrupt records, stores raw text in _corrupt_record
    - DROPMALFORMED: silently drops corrupt records
    - FAILFAST: throws an exception on first corrupt record
    - columnNameOfCorruptRecord: custom column name for corrupt text
    - Schema must include _corrupt_record field to capture corrupt rows

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pys_json import (
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
logger = get_logger("example.error_modes")

# Shared test data — a mix of valid and corrupt records
CORRUPT_DATA = [
    '{"name": "Alice", "age": 30}',
    '{"name": "Bob", "age": "twenty-five"}',
    '{"name": "Charlie", "age": 35}',
    "{INVALID JSON}",
    '{"name": "Diana", "age": 28}',
]

SCHEMA_WITH_CORRUPT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

SCHEMA_WITHOUT_CORRUPT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)


if __name__ == "__main__":
    spark = get_spark("error-modes")

    # Write shared test data
    import os
    import tempfile

    data_file = os.path.join(tempfile.gettempdir(), "pys_json", "error_modes", "data.json")
    write_json_lines(data_file, CORRUPT_DATA)
    logger.info("Wrote %s records (%s corrupt) to %s", len(CORRUPT_DATA), 2, data_file)

    # =========================================================================
    # 1. PERMISSIVE mode (default)
    # =========================================================================
    print_header("1. PERMISSIVE Mode (Default)")

    df_permissive = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(SCHEMA_WITH_CORRUPT)
        .json(data_file)
    )
    print_schema(df_permissive, title="PERMISSIVE Schema")

    # Cache to avoid UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN
    df_permissive = df_permissive.cache()
    print_dataframe(df_permissive, title="All Records (corrupt kept)")

    # Filter corrupt records
    df_corrupt = df_permissive.filter(F.col("_corrupt_record").isNotNull())
    print_dataframe(df_corrupt, title="Corrupt Records Only")
    logger.info("Found %s corrupt records", df_corrupt.count())

    # Filter valid records
    df_valid = df_permissive.filter(F.col("_corrupt_record").isNull())
    print_dataframe(df_valid, title="Valid Records Only")
    print_success("PERMISSIVE keeps all rows — corrupt text goes to _corrupt_record column")

    # =========================================================================
    # 2. PERMISSIVE without _corrupt_record in schema
    # =========================================================================
    print_header("2. PERMISSIVE Without _corrupt_record Column")

    df_no_corrupt_col = spark.read.option("mode", "PERMISSIVE").schema(SCHEMA_WITHOUT_CORRUPT).json(data_file)
    print_dataframe(df_no_corrupt_col, title="PERMISSIVE (no _corrupt_record)")
    print_warning(
        "Without _corrupt_record in schema, corrupt rows are silently dropped (same behavior as DROPMALFORMED)"
    )

    # =========================================================================
    # 3. PERMISSIVE with schema inference
    # =========================================================================
    print_header("3. PERMISSIVE with Schema Inference")

    df_inferred = spark.read.option("mode", "PERMISSIVE").json(data_file)
    print_schema(df_inferred, title="Inferred Schema")
    df_inferred = df_inferred.cache()
    print_dataframe(df_inferred, title="Inferred Schema Results")
    print_success("Schema inference auto-adds _corrupt_record column")

    # =========================================================================
    # 4. DROPMALFORMED mode
    # =========================================================================
    print_header("4. DROPMALFORMED Mode")

    df_drop = spark.read.option("mode", "DROPMALFORMED").schema(SCHEMA_WITH_CORRUPT).json(data_file)
    df_drop = df_drop.cache()
    print_dataframe(df_drop, title="DROPMALFORMED Results")
    logger.info(
        "Kept %s of %s records after dropping malformed",
        df_drop.count(),
        len(CORRUPT_DATA),
    )
    print_success("DROPMALFORMED silently discards unparseable rows")

    # =========================================================================
    # 5. FAILFAST mode
    # =========================================================================
    print_header("5. FAILFAST Mode")

    try:
        df_fail = spark.read.option("mode", "FAILFAST").schema(SCHEMA_WITHOUT_CORRUPT).json(data_file)
        # Action triggers parsing — must call an action to trigger the error
        df_fail.collect()
        print_dataframe(df_fail, title="FAILFAST Results")
    except Exception as e:
        error_msg = str(e)
        # Show just the first line of the error
        first_line = error_msg.split("\n")[0]
        logger.error("FAILFAST raised: %s", first_line)
        print_warning("FAILFAST throws an exception on the first corrupt record")

    # =========================================================================
    # 6. Mode comparison summary
    # =========================================================================
    print_header("6. Mode Comparison")

    summary_data = [
        ("PERMISSIVE", "Keeps row", "Stores in _corrupt_record", "Default, data recovery"),
        ("DROPMALFORMED", "Drops row", "No trace", "Clean output, lossy"),
        ("FAILFAST", "Throws exception", "Stops pipeline", "Strict validation"),
    ]
    df_summary = spark.createDataFrame(
        summary_data,
        ["Mode", "Corrupt Record", "Behavior", "Use Case"],
    )
    print_dataframe(df_summary, title="Error Handling Mode Comparison")
    print_success("Choose the mode that matches your data quality requirements")

    spark.stop()

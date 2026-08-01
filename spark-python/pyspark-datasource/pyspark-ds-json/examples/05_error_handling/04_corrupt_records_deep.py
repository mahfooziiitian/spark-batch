"""Corrupt records handling — production patterns for PERMISSIVE, DROPMALFORMED, FAILFAST.

Demonstrates the three parse modes in depth, the critical requirement of including
_corrupt_record in the schema, separating good/bad records, error classification,
and production quarantine patterns.

Key concepts:
    - PERMISSIVE (default): bad records stored in _corrupt_record column
    - DROPMALFORMED: silently drops bad records (dangerous for production)
    - FAILFAST: throws exception on first bad record
    - _corrupt_record MUST be in schema or bad records are silently lost
    - Always .cache() before querying _corrupt_record column
    - Combine with error classification for actionable monitoring

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
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
logger = get_logger("example.corrupt_records")


if __name__ == "__main__":
    spark = get_spark("corrupt-records")

    # =========================================================================
    # 1. Sample data with various corruption types
    # =========================================================================
    print_header("1. Input Data — Various Corruption Types")

    corrupt_file = DATA_HOME + "/corrupt_records.json"
    write_json_lines(
        corrupt_file,
        [
            '{"id": 1, "name": "Alice"}',
            '{"id": 2, "name": "Bob"',
            '{"id": 3, "name": "Charlie"}',
            'NOT JSON AT ALL',
            '{"id": 5, "name": "Eve"}',
            '',
            '{"id": "WRONG_TYPE", "name": "Frank"}',
            '{"id": 7, "name": "Grace"}',
            '{"extra_field": "ignored", "id": 8, "name": "Hank"}',
        ],
    )
    print_path("Input (mixed valid/invalid)", corrupt_file)
    logger.info(
        "Contains: valid, truncated JSON, non-JSON, empty line, type mismatch, extra fields"
    )

    # =========================================================================
    # 2. PERMISSIVE mode — the correct approach
    # =========================================================================
    print_header("2. PERMISSIVE Mode (Default — Recommended)")

    schema_with_corrupt = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    df_permissive = (
        spark.read.schema(schema_with_corrupt)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(corrupt_file)
        .cache()
    )
    print_schema(df_permissive, title="Schema with _corrupt_record")
    print_dataframe(df_permissive, title="PERMISSIVE — all records preserved")

    # Separate good and bad
    good_df = df_permissive.filter(F.col("_corrupt_record").isNull())
    bad_df = df_permissive.filter(F.col("_corrupt_record").isNotNull())

    print_dataframe(good_df.select("id", "name"), title="Good records")
    print_dataframe(bad_df.select("_corrupt_record"), title="Bad records (quarantine)")
    logger.info("Good: %s, Bad: %s, Total: %s", good_df.count(), bad_df.count(), df_permissive.count())
    print_success(
        "PERMISSIVE preserves ALL records — good ones parsed, bad ones in _corrupt_record"
    )

    # =========================================================================
    # 3. The critical mistake — missing _corrupt_record in schema
    # =========================================================================
    print_header("3. Critical Mistake — Missing _corrupt_record in Schema")

    schema_without_corrupt = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )

    df_no_corrupt = (
        spark.read.schema(schema_without_corrupt)
        .option("mode", "PERMISSIVE")
        .json(corrupt_file)
    )
    print_dataframe(df_no_corrupt, title="Without _corrupt_record in schema")
    logger.info("Records: %s", df_no_corrupt.count())
    print_warning(
        "Without _corrupt_record in schema: bad records become rows of all nulls — "
        "CANNOT distinguish them from legitimately null records!"
    )

    # =========================================================================
    # 4. DROPMALFORMED mode
    # =========================================================================
    print_header("4. DROPMALFORMED Mode")

    df_dropped = (
        spark.read.schema(schema_without_corrupt)
        .option("mode", "DROPMALFORMED")
        .json(corrupt_file)
    )
    print_dataframe(df_dropped, title="DROPMALFORMED — bad records silently removed")
    logger.info("Records remaining: %s (dropped %s)", df_dropped.count(), df_permissive.count() - df_dropped.count())
    print_warning(
        "DROPMALFORMED silently discards bad records — "
        "no way to know what was lost. Dangerous for production."
    )

    # =========================================================================
    # 5. FAILFAST mode
    # =========================================================================
    print_header("5. FAILFAST Mode")

    try:
        df_failfast = (
            spark.read.schema(schema_without_corrupt)
            .option("mode", "FAILFAST")
            .json(corrupt_file)
        )
        _ = df_failfast.count()
        logger.error("Expected exception was not raised")
    except Exception as e:
        error_msg = str(e)[:150]
        logger.info("FAILFAST threw: %s...", error_msg)
        print_success("FAILFAST throws exception on first bad record — use for validation jobs only")

    # =========================================================================
    # 6. Custom corrupt record column name
    # =========================================================================
    print_header("6. Custom Corrupt Record Column Name")

    schema_custom = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("_error_raw", StringType(), True),
        ]
    )

    df_custom = (
        spark.read.schema(schema_custom)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_error_raw")
        .json(corrupt_file)
        .cache()
    )
    bad_custom = df_custom.filter(F.col("_error_raw").isNotNull())
    print_dataframe(bad_custom.select("_error_raw"), title="Custom column name: _error_raw")
    print_success("columnNameOfCorruptRecord can be any name — just include it in the schema")
    df_custom.unpersist()

    # =========================================================================
    # 7. Error classification on corrupt records
    # =========================================================================
    print_header("7. Error Classification")

    df_classified = bad_df.withColumn(
        "error_type",
        F.when(F.trim(F.col("_corrupt_record")) == "", "empty_line")
        .when(~F.trim(F.col("_corrupt_record")).startswith("{"), "not_json")
        .when(
            ~F.col("_corrupt_record").contains("}"),
            "truncated_json",
        )
        .otherwise("parse_error"),
    )
    print_dataframe(
        df_classified.select("_corrupt_record", "error_type"),
        title="Classified errors",
    )
    print_success("Classify corrupt records for targeted investigation and upstream fixes")

    # =========================================================================
    # 8. Type mismatch detection
    # =========================================================================
    print_header("8. Type Mismatch Detection")

    # Records where id has wrong type get corrupted
    type_file = DATA_HOME + "/corrupt_type_mismatch.json"
    write_json_lines(
        type_file,
        [
            '{"id": 1, "amount": 100.50}',
            '{"id": 2, "amount": "NOT_A_NUMBER"}',
            '{"id": 3, "amount": 200.75}',
            '{"id": 4, "amount": null}',
            '{"id": 5, "amount": ""}',
        ],
    )

    type_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("amount", IntegerType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    df_types = (
        spark.read.schema(type_schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(type_file)
        .cache()
    )
    print_dataframe(df_types, title="Type mismatch detection")

    type_errors = df_types.filter(F.col("_corrupt_record").isNotNull())
    logger.info("Type mismatches detected: %s", type_errors.count())
    print_success(
        "PERMISSIVE catches type mismatches too — "
        "amount='NOT_A_NUMBER' flagged as corrupt when schema expects IntegerType"
    )
    df_types.unpersist()

    # =========================================================================
    # 9. Production pattern — split and write
    # =========================================================================
    print_header("9. Production Pattern — Split and Route")

    production_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    df_prod = (
        spark.read.schema(production_schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(corrupt_file)
        .withColumn("source_file", F.input_file_name())
        .withColumn("processed_at", F.current_timestamp())
        .cache()
    )

    # Route good records
    df_good_final = df_prod.filter(F.col("_corrupt_record").isNull()).select(
        "id", "name", "source_file", "processed_at"
    )

    # Route bad records with metadata
    df_bad_final = df_prod.filter(F.col("_corrupt_record").isNotNull()).select(
        "_corrupt_record", "source_file", "processed_at"
    )

    print_dataframe(df_good_final, title="→ Write to silver table")
    print_dataframe(df_bad_final, title="→ Write to quarantine table")

    # Metrics
    total = df_prod.count()
    good = df_good_final.count()
    bad = df_bad_final.count()
    logger.info("Total: %s | Good: %s (%.0f%%) | Bad: %s (%.0f%%)", total, good, good / total * 100, bad, bad / total * 100)

    df_prod.unpersist()
    print_success(
        "Production: PERMISSIVE + _corrupt_record → split → "
        "good to silver, bad to quarantine. Monitor bad/total ratio."
    )

    # =========================================================================
    # 10. Mode comparison
    # =========================================================================
    print_header("10. Mode Comparison")

    modes = [
        ("PERMISSIVE", "Keeps all records, bad ones in _corrupt_record", "Production pipelines"),
        ("DROPMALFORMED", "Silently drops bad records", "Quick exploration only"),
        ("FAILFAST", "Throws on first bad record", "Data validation jobs"),
    ]
    df_modes = spark.createDataFrame(modes, ["Mode", "Behavior", "Use Case"])
    print_dataframe(df_modes, title="Parse Mode Comparison")
    print_success(
        "Always use PERMISSIVE in production. "
        "Always include _corrupt_record in schema. Always .cache() before filtering."
    )

    spark.stop()

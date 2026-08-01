"""Optional fields and sparse JSON — handling missing, null, and empty values.

Demonstrates strategies for working with JSON where fields are inconsistently
present, and how to distinguish between missing fields, explicit nulls, empty
strings, and parse failures.

Key concepts:
    - Spark fills missing JSON fields with null automatically
    - Cannot distinguish "field missing" from "field: null" after parsing
    - Keep raw JSON to detect true absence vs explicit null
    - get_json_object returns null for both missing AND null fields
    - Validation flags: missing_or_null, empty, present, parse_failed
    - coalesce and default values for sparse data

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
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
logger = get_logger("example.optional_fields")


if __name__ == "__main__":
    spark = get_spark("optional-fields")

    # =========================================================================
    # 1. Basic behavior — missing fields become null
    # =========================================================================
    print_header("1. Missing Fields → Null")

    sparse_file = DATA_HOME + "/optional_fields_sparse.json"
    write_json_lines(
        sparse_file,
        [
            '{"id": 1, "email": "a@test.com"}',
            '{"id": 2, "phone": "9999999999"}',
            '{"id": 3}',
            '{"id": 4, "email": "d@test.com", "phone": "8888888888"}',
        ],
    )
    print_path("Input", sparse_file)

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
        ]
    )

    df = spark.read.schema(schema).json(sparse_file)
    print_schema(df, title="Schema with all optional fields")
    print_dataframe(df, title="Missing fields filled with null")
    print_success("Spark auto-fills missing fields with null — no error, no data loss")

    # =========================================================================
    # 2. The ambiguity problem
    # =========================================================================
    print_header("2. The Ambiguity Problem")

    ambiguous_file = DATA_HOME + "/optional_fields_ambiguous.json"
    write_json_lines(
        ambiguous_file,
        [
            '{"id": 1, "email": "a@test.com"}',
            '{"id": 2, "email": null}',
            '{"id": 3, "email": ""}',
            '{"id": 4}',
        ],
    )
    print_path("Input (4 different cases)", ambiguous_file)

    df_ambig = spark.read.schema(schema).json(ambiguous_file)
    print_dataframe(df_ambig, title="After parsing — cases 1, 2, 4 look the same!")
    print_warning(
        "After parsing: 'field missing' and 'field: null' both become null. "
        "Cannot distinguish them without the raw JSON."
    )

    # =========================================================================
    # 3. Solution: keep raw JSON + validation flags
    # =========================================================================
    print_header("3. Validation Flags from Raw JSON")

    # Read as text to preserve raw
    raw_df = spark.read.text(ambiguous_file).withColumnRenamed("value", "raw_json")

    # Extract field and determine status
    df_validated = raw_df.withColumn(
        "id", F.get_json_object(F.col("raw_json"), "$.id").cast("bigint")
    ).withColumn(
        "email_raw", F.get_json_object(F.col("raw_json"), "$.email")
    ).withColumn(
        "email_status",
        F.when(
            ~F.col("raw_json").contains('"email"'),
            F.lit("missing"),
        )
        .when(F.col("email_raw").isNull(), "explicit_null")
        .when(F.col("email_raw") == "", "empty")
        .otherwise("present"),
    )

    print_dataframe(
        df_validated.select("id", "email_raw", "email_status"),
        title="Validation flags distinguish all cases",
    )
    print_success(
        "Check raw JSON for field presence: missing vs null vs empty vs present"
    )

    # =========================================================================
    # 4. Generic field validator function
    # =========================================================================
    print_header("4. Generic Field Validator")

    def add_field_status(df, raw_col: str, field_name: str):
        """Add a status column for a JSON field distinguishing missing/null/empty/present."""
        json_path = f"$.{field_name}"
        field_col = f"{field_name}_raw"
        status_col = f"{field_name}_status"

        return df.withColumn(
            field_col, F.get_json_object(F.col(raw_col), json_path)
        ).withColumn(
            status_col,
            F.when(
                ~F.col(raw_col).contains(f'"{field_name}"'),
                F.lit("missing"),
            )
            .when(F.col(field_col).isNull(), "explicit_null")
            .when(F.col(field_col) == "", "empty")
            .otherwise("present"),
        )

    df_multi = raw_df.withColumn(
        "id", F.get_json_object(F.col("raw_json"), "$.id").cast("bigint")
    )
    df_multi = add_field_status(df_multi, "raw_json", "email")
    df_multi = add_field_status(df_multi, "raw_json", "phone")

    print_dataframe(
        df_multi.select("id", "email_status", "phone_status"),
        title="Status for multiple optional fields",
    )
    print_success("Reusable function validates any field across the dataset")

    # =========================================================================
    # 5. Default values with coalesce
    # =========================================================================
    print_header("5. Default Values with coalesce")

    df_defaults = df.select(
        "id",
        F.coalesce(F.col("email"), F.lit("no-email@placeholder.com")).alias("email"),
        F.coalesce(F.col("phone"), F.lit("000-000-0000")).alias("phone"),
    )
    print_dataframe(df_defaults, title="With default values (simple)")
    print_success("coalesce() fills nulls with defaults — simple but loses null semantics")

    # =========================================================================
    # 6. Conditional defaults based on status
    # =========================================================================
    print_header("6. Conditional Defaults Based on Status")

    df_smart = df_validated.select(
        "id",
        F.when(F.col("email_status") == "present", F.col("email_raw"))
        .when(F.col("email_status") == "empty", F.lit("[EMPTY]"))
        .when(F.col("email_status") == "explicit_null", F.lit("[NULL]"))
        .when(F.col("email_status") == "missing", F.lit("[NOT_PROVIDED]"))
        .alias("email_display"),
        F.col("email_status"),
    )
    print_dataframe(df_smart, title="Conditional defaults based on status")
    print_success("Different handling per status: present, empty, null, missing")

    # =========================================================================
    # 7. Sparsity analysis — which fields are populated?
    # =========================================================================
    print_header("7. Sparsity Analysis")

    # Read a larger sparse dataset
    analysis_file = DATA_HOME + "/optional_fields_analysis.json"
    write_json_lines(
        analysis_file,
        [
            '{"id": 1, "name": "A", "email": "a@t.com"}',
            '{"id": 2, "name": "B", "phone": "111"}',
            '{"id": 3, "name": "C"}',
            '{"id": 4, "name": "D", "email": "d@t.com", "phone": "444", "address": "NYC"}',
            '{"id": 5, "name": "E", "address": "LA"}',
            '{"id": 6, "name": "F", "email": "f@t.com"}',
            '{"id": 7, "name": "G"}',
            '{"id": 8, "name": "H", "phone": "888"}',
            '{"id": 9, "name": "I", "email": "i@t.com", "phone": "999"}',
            '{"id": 10, "name": "J"}',
        ],
    )

    df_analysis = spark.read.json(analysis_file)
    total_rows = df_analysis.count()

    # Calculate non-null percentage for each column
    sparsity_data = []
    for col_name in sorted(df_analysis.columns):
        non_null = df_analysis.filter(F.col(col_name).isNotNull()).count()
        pct = (non_null / total_rows) * 100
        sparsity_data.append((col_name, non_null, total_rows, f"{pct:.0f}%"))

    df_sparsity = spark.createDataFrame(
        sparsity_data, ["field", "non_null", "total", "fill_rate"]
    )
    print_dataframe(df_sparsity, title="Field sparsity analysis")
    print_success("Sparsity analysis reveals which fields are optional in practice")

    # =========================================================================
    # 8. Schema with rescued data column
    # =========================================================================
    print_header("8. Rescued Data Column for Unexpected Fields")

    # Define a narrow schema — unexpected fields go to _rescued_data
    narrow_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
    )

    df_rescued = (
        spark.read.schema(narrow_schema)
        .option("columnNameOfCorruptRecord", "_rescued_data")
        .json(analysis_file)
    )
    # Note: columnNameOfCorruptRecord captures full corrupt lines, not extra fields
    # For extra fields, need rescuedDataColumn (Databricks) or read as text

    # Alternative: read with narrow schema, extra fields silently dropped
    df_narrow = spark.read.schema(narrow_schema).json(analysis_file)
    print_dataframe(df_narrow, title="Narrow schema — extra fields silently dropped")
    print_warning(
        "Extra fields (email, phone, address) are silently dropped with narrow schema. "
        "Use wide schema or raw JSON to preserve them."
    )

    # =========================================================================
    # 9. Best practices summary
    # =========================================================================
    print_header("9. Best Practices")

    practices = [
        ("Wide schema", "Include all known optional fields", "Fields appear as null"),
        ("Raw JSON + flags", "Keep raw_json column", "Distinguish missing vs null"),
        ("coalesce defaults", "Simple null filling", "When null semantics don't matter"),
        ("Status columns", "missing/null/empty/present", "Data quality reporting"),
        ("Sparsity analysis", "Count non-nulls per field", "Understand data completeness"),
    ]
    df_practices = spark.createDataFrame(practices, ["Pattern", "When", "Result"])
    print_dataframe(df_practices, title="Optional Fields Best Practices")
    print_success(
        "For most cases: use wide schema (nulls are fine). "
        "For data quality: keep raw JSON and add validation flags."
    )

    spark.stop()

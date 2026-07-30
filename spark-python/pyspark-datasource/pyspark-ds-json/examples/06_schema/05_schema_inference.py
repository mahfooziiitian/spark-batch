"""Schema inference in PySpark — automatic type detection from JSON data.

Demonstrates how Spark infers schemas from JSON files when no explicit schema
is provided. Covers inference behavior, sampling ratio, type promotion rules,
and how to inspect/override inferred schemas.

Key concepts:
    - Default schema inference reads ALL data (samplingRatio=1.0)
    - samplingRatio < 1.0 speeds up inference on large files but may miss fields
    - Type promotion: int → long → double → string (when conflicts arise)
    - primitivesAsString forces all leaf values to StringType
    - prefersDecimal infers floating-point as DecimalType instead of DoubleType
    - dropFieldIfAllNull removes columns that are entirely null

Inference rules:
    - Numbers without decimals → LongType (not IntegerType)
    - Numbers with decimals → DoubleType
    - "true"/"false" → BooleanType
    - Nested objects → StructType
    - Arrays → ArrayType (element type inferred from all elements)
    - Conflicting types across records → StringType (fallback)

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html#schema-inference
"""

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.schema_inference")


if __name__ == "__main__":
    spark = get_spark("schema-inference")

    # --- 1. Basic schema inference ---
    print_header("1. Basic Schema Inference")

    data_file = DATA_HOME + "/schema_inference_demo.json"
    write_json_lines(
        data_file,
        [
            '{"name": "Alice", "age": 30, "salary": 75000.50, "active": true}',
            '{"name": "Bob", "age": 25, "salary": 65000.00, "active": false, "department": "Engineering"}',
            '{"name": "Charlie", "age": 35, "salary": 90000.75, "active": true, "tags": ["senior", "lead"]}',
        ],
    )
    print_path("Input", data_file)

    df_inferred = spark.read.json(data_file)
    print_schema(df_inferred, title="Inferred Schema (no hints)")
    print_dataframe(df_inferred, title="Basic Inference Result")

    # --- 2. Sampling ratio for large files ---
    print_header("2. Sampling Ratio")

    df_sampled = spark.read.option("samplingRatio", "0.5").json(data_file)
    print_schema(df_sampled, title="Schema with samplingRatio=0.5")
    logger.info("May miss optional fields like 'tags' or 'department'")

    # --- 3. primitivesAsString ---
    print_header("3. primitivesAsString")

    df_strings = spark.read.option("primitivesAsString", "true").json(data_file)
    print_schema(df_strings, title="All Primitives as StringType")
    print_dataframe(df_strings, title="primitivesAsString=true")

    # --- 4. prefersDecimal ---
    print_header("4. prefersDecimal")

    df_decimal = spark.read.option("prefersDecimal", "true").json(data_file)
    print_schema(df_decimal, title="Floating-point as DecimalType")

    # --- 5. Type conflicts ---
    print_header("5. Type Conflict Resolution")

    conflict_file = DATA_HOME + "/schema_inference_conflict.json"
    write_json_lines(
        conflict_file,
        [
            '{"value": 42}',
            '{"value": "hello"}',
            '{"value": true}',
        ],
    )
    print_path("Input", conflict_file)

    df_conflict = spark.read.json(conflict_file)
    print_schema(df_conflict, title="Conflicting Types → StringType")
    print_dataframe(df_conflict, title="Type Conflict Result")

    # --- 6. Nested object inference ---
    print_header("6. Nested Object Inference")

    nested_file = DATA_HOME + "/schema_inference_nested.json"
    write_json_lines(
        nested_file,
        [
            '{"user": {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, "score": 95}',
            '{"user": {"name": "Bob", "address": {"city": "LA", "zip": "90001"}}, "score": 88}',
        ],
    )
    print_path("Input", nested_file)

    df_nested = spark.read.json(nested_file)
    print_schema(df_nested, title="Nested Struct Inference")
    print_dataframe(df_nested, title="Nested Objects")

    # --- 7. dropFieldIfAllNull ---
    print_header("7. dropFieldIfAllNull")

    null_file = DATA_HOME + "/schema_inference_nulls.json"
    write_json_lines(
        null_file,
        [
            '{"name": "Alice", "age": 30, "phantom": null}',
            '{"name": "Bob", "age": 25, "phantom": null}',
        ],
    )

    df_with_nulls = spark.read.json(null_file)
    print_schema(df_with_nulls, title="Without dropFieldIfAllNull (phantom present)")

    df_dropped = spark.read.option("dropFieldIfAllNull", "true").json(null_file)
    print_schema(df_dropped, title="With dropFieldIfAllNull=true (phantom removed)")
    print_success("All-null column 'phantom' successfully removed")

    # --- 8. Export inferred schema ---
    print_header("8. Export Inferred Schema for Reuse")

    inferred_schema = df_inferred.schema
    logger.info("DDL: %s", inferred_schema.simpleString())
    logger.info("JSON:\n%s", inferred_schema.json())

    df_explicit = spark.read.schema(inferred_schema).json(data_file)
    print_schema(df_explicit, title="Re-read with Explicit Schema (no inference)")
    print_success("Schema captured and reused — no inference overhead on subsequent reads")

    spark.stop()

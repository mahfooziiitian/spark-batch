"""Primitive, Array, and Struct in Same Root — handling inconsistent JSON root types.

Demonstrates strategies for reading JSON files with mixed root structures
(objects, arrays, primitives) that Spark cannot handle with a single read call.

Key concepts:
    - Spark expects consistent root type across JSON files
    - Mixed roots (object, array, primitive) cause read failures
    - Read as text first, classify by root type, process separately
    - multiLine=true for array-rooted files
    - from_json for parsing classified text into structured columns
    - Union results after separate processing

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
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
logger = get_logger("example.mixed_root_structures")


if __name__ == "__main__":
    import os

    spark = get_spark("mixed-root-structures")
    base_dir = DATA_HOME + "/mixed_root"
    os.makedirs(base_dir, exist_ok=True)

    # =========================================================================
    # 1. The problem — inconsistent root types
    # =========================================================================
    print_header("1. The Problem — Inconsistent Root Types")

    # File 1: JSON Lines (object per line)
    file_objects = os.path.join(base_dir, "objects.json")
    write_json_lines(
        file_objects,
        [
            '{"id": 1, "name": "Alice"}',
            '{"id": 2, "name": "Bob"}',
        ],
    )

    # File 2: JSON array (multiLine)
    file_array = os.path.join(base_dir, "array.json")
    with open(file_array, "w") as f:
        f.write('[{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Diana"}]')

    # File 3: Primitive string
    file_primitive = os.path.join(base_dir, "primitive.json")
    with open(file_primitive, "w") as f:
        f.write('"just a string"')

    print_path("File 1 (objects)", file_objects)
    print_path("File 2 (array)", file_array)
    print_path("File 3 (primitive)", file_primitive)

    # Try reading all together — demonstrates the problem
    try:
        df_all = spark.read.json([file_objects, file_array, file_primitive])
        print_dataframe(df_all, title="Mixed read attempt")
        print_warning("Spark may produce unexpected results or errors with mixed root types")
    except Exception as e:
        logger.warning("Mixed read failed: %s", str(e)[:100])
        print_warning("Spark cannot reliably read files with different root types together")

    # =========================================================================
    # 2. Strategy: read as text, classify root type
    # =========================================================================
    print_header("2. Read as Text and Classify")

    raw_df = spark.read.text([file_objects, file_array, file_primitive])
    print_dataframe(raw_df, title="Raw text lines")

    classified_df = raw_df.withColumn(
        "json_type",
        F.when(F.trim(F.col("value")).startswith("{"), "object")
        .when(F.trim(F.col("value")).startswith("["), "array")
        .otherwise("primitive"),
    )
    print_dataframe(classified_df, title="Classified by root type")

    # Count by type
    type_counts = classified_df.groupBy("json_type").count()
    print_dataframe(type_counts, title="Records per type")
    print_success("Read as text first, then classify using startsWith on trimmed value")

    # =========================================================================
    # 3. Process objects
    # =========================================================================
    print_header("3. Process Object Records")

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
    )

    df_objects = classified_df.filter(F.col("json_type") == "object").select(
        F.from_json(F.col("value"), schema).alias("data")
    ).select("data.*")
    print_dataframe(df_objects, title="Parsed objects")
    logger.info("Object records: %s", df_objects.count())

    # =========================================================================
    # 4. Process arrays
    # =========================================================================
    print_header("4. Process Array Records")

    array_schema = ArrayType(schema)

    df_arrays = (
        classified_df.filter(F.col("json_type") == "array")
        .select(F.from_json(F.col("value"), array_schema).alias("data"))
        .select(F.explode_outer("data").alias("item"))
        .select("item.*")
    )
    print_dataframe(df_arrays, title="Parsed arrays (exploded)")
    logger.info("Array-sourced records: %s", df_arrays.count())

    # =========================================================================
    # 5. Process primitives
    # =========================================================================
    print_header("5. Process Primitive Records")

    df_primitives = classified_df.filter(F.col("json_type") == "primitive").select(
        F.regexp_replace(F.col("value"), '^"|"$', "").alias("raw_value")
    )
    print_dataframe(df_primitives, title="Primitive values (stripped quotes)")
    print_warning("Primitive JSON values cannot be structured — log or handle as errors")

    # =========================================================================
    # 6. Union structured results
    # =========================================================================
    print_header("6. Union All Structured Results")

    df_unified = df_objects.unionByName(df_arrays)
    print_dataframe(df_unified, title="Unified result (objects + arrays)")
    logger.info("Total structured records: %s", df_unified.count())
    print_success("Process each root type separately, then union for final result")

    # =========================================================================
    # 7. Robust production pattern
    # =========================================================================
    print_header("7. Production Pattern — Full Pipeline")

    # More realistic: mixed file with JSON Lines and occasional arrays
    mixed_file = os.path.join(base_dir, "production_mixed.json")
    write_json_lines(
        mixed_file,
        [
            '{"id": 10, "name": "Eve", "score": 95}',
            '{"id": 11, "name": "Frank", "score": 88}',
            '[{"id": 12, "name": "Grace", "score": 72}, {"id": 13, "name": "Hank", "score": 91}]',
            '{"id": 14, "name": "Ivy", "score": 66}',
            'null',
            '""',
            '{"id": 15, "name": "Jack", "score": 100}',
        ],
    )
    print_path("Mixed production file", mixed_file)

    prod_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("score", LongType(), True),
        ]
    )

    raw = spark.read.text(mixed_file)
    classified = raw.withColumn("trimmed", F.trim(F.col("value"))).withColumn(
        "root_type",
        F.when(F.col("trimmed").startswith("{"), "object")
        .when(F.col("trimmed").startswith("["), "array")
        .otherwise("skip"),
    )

    # Parse objects
    parsed_objects = (
        classified.filter(F.col("root_type") == "object")
        .select(F.from_json(F.col("value"), prod_schema).alias("data"))
        .select("data.*")
    )

    # Parse arrays
    parsed_arrays = (
        classified.filter(F.col("root_type") == "array")
        .select(F.from_json(F.col("value"), ArrayType(prod_schema)).alias("data"))
        .select(F.explode_outer("data").alias("item"))
        .select("item.*")
    )

    # Skipped records (for logging/alerting)
    skipped = classified.filter(F.col("root_type") == "skip")
    skipped_count = skipped.count()
    logger.warning("Skipped %s non-object/non-array lines", skipped_count)

    # Final unified result
    df_final = parsed_objects.unionByName(parsed_arrays)
    print_dataframe(df_final, title="Production pipeline result")
    logger.info("Total records parsed: %s (skipped: %s)", df_final.count(), skipped_count)
    print_success(
        "Production pattern: text → classify → parse objects + arrays → union. "
        "Log skipped records for investigation."
    )

    # =========================================================================
    # 8. Alternative: multiLine for pure array files
    # =========================================================================
    print_header("8. Alternative — multiLine for Array Files")

    df_multiline = spark.read.option("multiLine", "true").schema(schema).json(file_array)
    print_dataframe(df_multiline, title="Array file read with multiLine=true")
    print_success(
        "If ALL files are array-rooted, use multiLine=true. "
        "For mixed roots, use the text-classify-parse pattern."
    )

    spark.stop()

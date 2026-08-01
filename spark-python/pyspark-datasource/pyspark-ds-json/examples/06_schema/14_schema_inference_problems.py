"""Schema inference problems — why automatic inference is expensive and dangerous.

Demonstrates the pitfalls of relying on automatic schema inference for JSON data,
including type instability, primitive-to-struct evolution, performance overhead,
and the recommended explicit-schema approach.

Key concepts:
    - Schema inference reads ALL data (expensive for large datasets)
    - Mixed types in a field cause fallback to StringType
    - Primitive-to-struct evolution breaks pipelines silently
    - Explicit schemas with PERMISSIVE mode catch data issues early
    - samplingRatio < 1.0 can miss fields and cause inconsistent schemas

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html#schema-inference
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
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
logger = get_logger("example.schema_inference_problems")


if __name__ == "__main__":
    spark = get_spark("schema-inference-problems")

    # =========================================================================
    # 1. Mixed types cause unexpected StringType
    # =========================================================================
    print_header("1. Mixed Types → StringType Fallback")

    mixed_file = DATA_HOME + "/inference_problems_mixed.json"
    write_json_lines(
        mixed_file,
        [
            '{"id": 1, "amount": 100}',
            '{"id": 2, "amount": 100.25}',
            '{"id": 3, "amount": "UNKNOWN"}',
        ],
    )
    print_path("Input", mixed_file)

    df_inferred = spark.read.json(mixed_file)
    print_schema(df_inferred, title="Inferred Schema (amount becomes StringType)")
    print_dataframe(df_inferred, title="All values coerced to string")
    print_warning(
        "One bad record ('UNKNOWN') forces entire 'amount' column to StringType — "
        "numeric operations will fail downstream"
    )

    # =========================================================================
    # 2. Explicit schema catches the problem
    # =========================================================================
    print_header("2. Explicit Schema — The Better Approach")

    explicit_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("amount", DoubleType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    df_explicit = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(explicit_schema)
        .json(mixed_file)
        .cache()
    )
    print_schema(df_explicit, title="Explicit Schema (amount is DoubleType)")
    print_dataframe(df_explicit, title="PERMISSIVE mode detects bad records")

    corrupt_count = df_explicit.filter(F.col("_corrupt_record").isNotNull()).count()
    logger.info("Corrupt records detected: %s", corrupt_count)
    print_success(
        "Explicit schema + PERMISSIVE mode isolates bad records without losing good data"
    )

    # =========================================================================
    # 3. Primitive-to-struct evolution (hardest scenario)
    # =========================================================================
    print_header("3. Primitive → Struct Evolution (Hard Scenario)")

    # Day 1: amount is a simple number
    day1_file = DATA_HOME + "/inference_problems_day1.json"
    write_json_lines(
        day1_file,
        [
            '{"id": 1, "amount": 100}',
            '{"id": 2, "amount": 200.50}',
        ],
    )

    # Day 2: amount becomes a struct
    day2_file = DATA_HOME + "/inference_problems_day2.json"
    write_json_lines(
        day2_file,
        [
            '{"id": 3, "amount": {"value": 150, "currency": "USD"}}',
            '{"id": 4, "amount": {"value": 300.75, "currency": "EUR"}}',
        ],
    )

    print_path("Day 1 data", day1_file)
    print_path("Day 2 data", day2_file)

    # Reading day 1 alone — works fine
    df_day1 = spark.read.json(day1_file)
    print_schema(df_day1, title="Day 1 Schema (amount is LongType/DoubleType)")
    print_dataframe(df_day1, title="Day 1 — Simple numeric amount")

    # Reading day 2 alone — works fine (different schema)
    df_day2 = spark.read.json(day2_file)
    print_schema(df_day2, title="Day 2 Schema (amount is StructType)")
    print_dataframe(df_day2, title="Day 2 — Structured amount")

    # Reading both together — inference widens to StringType
    df_combined = spark.read.json([day1_file, day2_file])
    print_schema(df_combined, title="Combined Inferred Schema (conflict!)")
    print_dataframe(df_combined, title="Combined — both days together")
    print_warning(
        "Primitive→Struct conflict: Spark widens 'amount' to StringType, "
        "destroying type information for both formats"
    )

    # =========================================================================
    # 4. Handling primitive-to-struct with explicit schema
    # =========================================================================
    print_header("4. Solution — Read as String, Parse Conditionally")

    # Read amount as string to preserve both formats
    flexible_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("amount", StringType(), True),
        ]
    )

    df_flexible = spark.read.schema(flexible_schema).json([day1_file, day2_file])
    print_dataframe(df_flexible, title="Read with StringType amount")

    # Parse conditionally based on content
    df_normalized = df_flexible.withColumn(
        "amount_value",
        F.when(
            F.col("amount").startswith("{"),
            F.get_json_object(F.col("amount"), "$.value").cast("double"),
        ).otherwise(F.col("amount").cast("double")),
    ).withColumn(
        "currency",
        F.when(
            F.col("amount").startswith("{"),
            F.get_json_object(F.col("amount"), "$.currency"),
        ).otherwise(F.lit("USD")),
    )

    print_dataframe(
        df_normalized.select("id", "amount_value", "currency"),
        title="Normalized — both formats unified",
    )
    print_success(
        "Read evolved field as StringType, then parse conditionally with "
        "get_json_object() to handle both primitive and struct formats"
    )

    # =========================================================================
    # 5. Performance cost of inference
    # =========================================================================
    print_header("5. Performance Cost of Inference")

    import time

    large_file = DATA_HOME + "/inference_problems_large.json"
    records = [f'{{"id": {i}, "value": {i * 1.5}, "label": "item_{i}"}}' for i in range(10000)]
    write_json_lines(large_file, records)
    print_path("Large file (10K records)", large_file)

    # With inference
    start = time.time()
    df_inf = spark.read.json(large_file)
    _ = df_inf.count()
    infer_time = time.time() - start

    # With explicit schema
    perf_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("label", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )
    start = time.time()
    df_sch = spark.read.schema(perf_schema).json(large_file)
    _ = df_sch.count()
    schema_time = time.time() - start

    logger.info("With inference:       %.3fs", infer_time)
    logger.info("With explicit schema: %.3fs", schema_time)
    logger.info("Speedup: %.1fx", infer_time / schema_time if schema_time > 0 else 0)
    print_success(
        "Explicit schemas skip the inference pass — "
        "faster and more predictable for large datasets"
    )

    # =========================================================================
    # 6. samplingRatio trap
    # =========================================================================
    print_header("6. samplingRatio Trap")

    sampling_file = DATA_HOME + "/inference_problems_sampling.json"
    # First 99 records have only id+name, last record introduces a new field
    sampling_records = [f'{{"id": {i}, "name": "user_{i}"}}' for i in range(99)]
    sampling_records.append('{"id": 100, "name": "admin", "role": "superuser", "permissions": ["read","write"]}')
    write_json_lines(sampling_file, sampling_records)

    df_full_sample = spark.read.option("samplingRatio", "1.0").json(sampling_file)
    df_partial_sample = spark.read.option("samplingRatio", "0.1").json(sampling_file)

    print_schema(df_full_sample, title="Full Sampling (1.0) — sees all fields")
    print_schema(df_partial_sample, title="Partial Sampling (0.1) — may miss rare fields")
    print_warning(
        "samplingRatio < 1.0 may miss fields that appear only in rare records — "
        "schema becomes non-deterministic across runs"
    )

    spark.stop()

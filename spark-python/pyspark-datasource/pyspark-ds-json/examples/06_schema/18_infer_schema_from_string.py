"""Inferring schema from JSON string — schema_of_json and production strategies.

Demonstrates using schema_of_json() to quickly derive schemas from sample JSON,
its limitations, and production-ready approaches using multiple samples and
schema merging.

Key concepts:
    - schema_of_json() parses a JSON string literal and returns DDL schema
    - Useful for quick prototyping and schema discovery
    - Only sees ONE sample — may miss optional fields or type variations
    - For production: collect multiple samples, merge schemas
    - Combine with from_json() to parse string columns using derived schema

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.schema_of_json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    _parse_datatype_string,
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
logger = get_logger("example.infer_schema_from_string")


if __name__ == "__main__":
    spark = get_spark("infer-schema-from-string")

    # =========================================================================
    # 1. Basic schema_of_json usage
    # =========================================================================
    print_header("1. Basic schema_of_json")

    sample = '{"id": 1, "name": "Alice", "amount": 10.5}'

    schema_df = spark.range(1).select(
        F.schema_of_json(F.lit(sample)).alias("schema")
    )
    print_dataframe(schema_df, title="Inferred DDL schema from sample")

    schema_ddl = schema_df.collect()[0]["schema"]
    logger.info("DDL: %s", schema_ddl)
    print_success("schema_of_json() returns a DDL string you can use with spark.read.schema()")

    # =========================================================================
    # 2. Using the derived schema to read files
    # =========================================================================
    print_header("2. Use Derived Schema to Read Files")

    data_file = DATA_HOME + "/infer_string_data.json"
    write_json_lines(
        data_file,
        [
            '{"id": 1, "name": "Alice", "amount": 10.5}',
            '{"id": 2, "name": "Bob", "amount": 20.0}',
            '{"id": 3, "name": "Charlie", "amount": 15.75}',
        ],
    )
    print_path("Input", data_file)

    df = spark.read.schema(_parse_datatype_string(schema_ddl)).json(data_file)
    print_schema(df, title="Read with schema_of_json-derived schema")
    print_dataframe(df, title="Data loaded with derived schema")
    print_success("Workflow: sample → schema_of_json → DDL string → spark.read.schema()")

    # =========================================================================
    # 3. Limitation: single sample misses fields
    # =========================================================================
    print_header("3. Limitation — Single Sample Misses Fields")

    # Sample 1 has basic fields
    sample1 = '{"id": 1, "name": "Alice", "amount": 10.5}'
    # Sample 2 has additional optional fields
    sample2 = '{"id": 2, "name": "Bob", "amount": 20.0, "email": "bob@co.com", "tags": ["vip"]}'

    schema1 = spark.range(1).select(F.schema_of_json(F.lit(sample1)).alias("s")).collect()[0]["s"]
    schema2 = spark.range(1).select(F.schema_of_json(F.lit(sample2)).alias("s")).collect()[0]["s"]

    logger.info("Schema from sample 1: %s", schema1)
    logger.info("Schema from sample 2: %s", schema2)
    print_warning(
        "Sample 1 misses 'email' and 'tags' — "
        "schema_of_json only sees what's in the single sample you provide"
    )

    # =========================================================================
    # 4. Production approach: multiple samples → merged schema
    # =========================================================================
    print_header("4. Production — Multiple Samples, Merged Schema")

    from pys_json.schema import merge_schemas

    samples = [
        '{"id": 1, "name": "Alice", "amount": 10.5}',
        '{"id": 2, "name": "Bob", "amount": 20.0, "email": "bob@co.com"}',
        '{"id": 3, "name": "Charlie", "tags": ["vip", "premium"], "active": true}',
    ]

    # Derive schema from each sample
    schemas = []
    for i, s in enumerate(samples):
        ddl = spark.range(1).select(F.schema_of_json(F.lit(s)).alias("s")).collect()[0]["s"]
        parsed = _parse_datatype_string(ddl)
        schemas.append(parsed)
        logger.info("Sample %s schema: %s", i + 1, ddl)

    # Merge all schemas
    merged = merge_schemas(*schemas)
    print_schema(spark.createDataFrame([], merged), title="Merged schema from 3 samples")

    # Read with merged schema
    multi_file = DATA_HOME + "/infer_string_multi.json"
    write_json_lines(multi_file, samples)

    df_merged = spark.read.schema(merged).json(multi_file)
    print_dataframe(df_merged, title="Read with merged schema (all fields present)")
    print_success("Merge schemas from multiple samples for production coverage")

    # =========================================================================
    # 5. schema_of_json with nested structures
    # =========================================================================
    print_header("5. Nested Structures")

    nested_sample = '{"user": {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, "scores": [95, 87]}'
    nested_ddl = spark.range(1).select(
        F.schema_of_json(F.lit(nested_sample)).alias("s")
    ).collect()[0]["s"]

    logger.info("Nested DDL: %s", nested_ddl)
    nested_schema = _parse_datatype_string(nested_ddl)
    print_schema(spark.createDataFrame([], nested_schema), title="Nested schema from sample")
    print_success("schema_of_json handles nested objects and arrays correctly")

    # =========================================================================
    # 6. Combine with from_json for string columns
    # =========================================================================
    print_header("6. Combine with from_json for String Columns")

    # Simulate a DataFrame with a JSON string column (e.g., from Kafka)
    raw_data = [
        ("event1", '{"user": "Alice", "action": "click", "ts": 1700000000}'),
        ("event2", '{"user": "Bob", "action": "purchase", "ts": 1700000100}'),
    ]
    df_raw = spark.createDataFrame(raw_data, ["event_id", "payload"])
    print_dataframe(df_raw, title="Raw data with JSON string column")

    # Derive schema from one payload sample
    payload_sample = raw_data[0][1]
    payload_ddl = spark.range(1).select(
        F.schema_of_json(F.lit(payload_sample)).alias("s")
    ).collect()[0]["s"]
    logger.info("Payload schema: %s", payload_ddl)

    # Parse with from_json
    payload_schema = _parse_datatype_string(payload_ddl)
    df_parsed = df_raw.select(
        "event_id",
        F.from_json(F.col("payload"), payload_schema).alias("data"),
    ).select(
        "event_id",
        F.col("data.user").alias("user"),
        F.col("data.action").alias("action"),
        F.col("data.ts").alias("timestamp"),
    )
    print_dataframe(df_parsed, title="Parsed with from_json using derived schema")
    print_success("schema_of_json + from_json: derive schema from sample, parse string columns")

    # =========================================================================
    # 7. Type options with schema_of_json
    # =========================================================================
    print_header("7. Type Options")

    # prefersDecimal changes numeric inference
    decimal_sample = '{"price": 19.99, "quantity": 5}'

    default_ddl = spark.range(1).select(
        F.schema_of_json(F.lit(decimal_sample)).alias("s")
    ).collect()[0]["s"]

    decimal_ddl = spark.range(1).select(
        F.schema_of_json(F.lit(decimal_sample), {"prefersDecimal": "true"}).alias("s")
    ).collect()[0]["s"]

    logger.info("Default:        %s", default_ddl)
    logger.info("prefersDecimal: %s", decimal_ddl)
    print_success("Pass options dict to schema_of_json to control type inference behavior")

    # =========================================================================
    # 8. Recommended workflow summary
    # =========================================================================
    print_header("8. Recommended Workflow")

    workflow = [
        ("1. Prototype", "schema_of_json(lit(sample))", "Quick discovery"),
        ("2. Validate", "Collect 5-10 diverse samples", "Cover optional fields"),
        ("3. Merge", "merge_schemas(*parsed_schemas)", "Union of all fields"),
        ("4. Harden", "StructType(...) in code", "Stable production schema"),
        ("5. Detect", "PERMISSIVE + _corrupt_record", "Catch violations at runtime"),
    ]
    df_workflow = spark.createDataFrame(workflow, ["Step", "Action", "Purpose"])
    print_dataframe(df_workflow, title="Schema Discovery → Production Workflow")
    print_success(
        "Use schema_of_json for discovery, then harden into explicit StructType for production"
    )

    spark.stop()

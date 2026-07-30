"""schema_of_json — infer schema dynamically from a JSON sample string.

Demonstrates schema_of_json() for automatically deriving a DDL schema
string from a sample JSON value. Useful during development to bootstrap
schemas before hardcoding them for production, and critical for dynamic
pipelines (e.g., Kafka consumers) where schema discovery is needed at runtime.

Key concepts:
    - schema_of_json(lit(json_sample)) → DDL schema string column
    - Infers types from a single sample value
    - Output can be used directly with from_json()
    - SQL variant: SELECT schema_of_json('{"id":1,"name":"John"}')
    - Dynamic pipelines: infer schema → apply to streaming column
    - For production, prefer explicit schemas over inference

Signature:
    schema_of_json(json_str, options={}) → Column

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.schema_of_json.html
"""

from pyspark.sql import functions as F

from pys_json import (
    get_spark,
    print_dataframe,
    print_header,
    print_success,
    set_log_level,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.schema_of_json")


if __name__ == "__main__":
    spark = get_spark("schema-of-json")

    # =========================================================================
    # 1. Basic schema inference
    # =========================================================================
    print_header("1. Infer Schema from Sample")

    sample = '{"name": "Alice", "age": 30, "salary": 85000.50}'
    schema_ddl = spark.range(1).select(F.schema_of_json(F.lit(sample))).collect()[0][0]
    logger.info("Sample: %s", sample)
    logger.info("Inferred DDL: %s", schema_ddl)

    # =========================================================================
    # 2. Use inferred schema with from_json
    # =========================================================================
    print_header("2. Apply Inferred Schema")

    data = [
        (1, '{"name": "Alice", "age": 30, "salary": 85000.50}'),
        (2, '{"name": "Bob", "age": 25, "salary": 65000.00}'),
    ]
    df = spark.createDataFrame(data, ["id", "json_str"])

    # Inline: schema_of_json directly in from_json
    schema_col = F.schema_of_json(F.lit(sample))
    df_parsed = df.withColumn("parsed", F.from_json("json_str", schema_ddl))
    print_dataframe(df_parsed, title="Parsed with Inferred Schema")

    # =========================================================================
    # 3. Nested JSON schema inference
    # =========================================================================
    print_header("3. Nested JSON Schema")

    nested_sample = '{"user": {"name": "Alice", "address": {"city": "NYC"}}, "active": true}'
    nested_ddl = spark.range(1).select(F.schema_of_json(F.lit(nested_sample))).collect()[0][0]
    logger.info("Nested sample: %s", nested_sample)
    logger.info("Nested DDL: %s", nested_ddl)
    print_success("Nested structs are fully inferred")

    # =========================================================================
    # 4. Array schema inference
    # =========================================================================
    print_header("4. Array Schema Inference")

    array_sample = '{"tags": ["a", "b"], "scores": [1, 2, 3]}'
    array_ddl = spark.range(1).select(F.schema_of_json(F.lit(array_sample))).collect()[0][0]
    logger.info("Array sample: %s", array_sample)
    logger.info("Array DDL: %s", array_ddl)
    print_success("Arrays and their element types are inferred")

    # =========================================================================
    # 5. Caveats — type ambiguity
    # =========================================================================
    print_header("5. Caveats")

    # Integer vs Long
    int_sample = '{"count": 42}'
    int_ddl = spark.range(1).select(F.schema_of_json(F.lit(int_sample))).collect()[0][0]
    logger.info("42 infers as: %s", int_ddl)

    # Float vs Decimal
    float_sample = '{"price": 9.99}'
    float_ddl = spark.range(1).select(F.schema_of_json(F.lit(float_sample))).collect()[0][0]
    logger.info("9.99 infers as: %s", float_ddl)

    # Null value — type unknown
    null_sample = '{"value": null}'
    null_ddl = spark.range(1).select(F.schema_of_json(F.lit(null_sample))).collect()[0][0]
    logger.info("null infers as: %s", null_ddl)

    print_success("Single-sample inference may miss optional fields or choose wrong numeric types")

    # =========================================================================
    # 6. SQL-based schema_of_json (dynamic discovery)
    # =========================================================================
    print_header("6. SQL-Based schema_of_json")

    # Direct SQL — useful for ad-hoc discovery in notebooks
    sql_result = spark.sql("""
        SELECT schema_of_json('{"id":1,"name":"John","active":true}') AS inferred_schema
    """)
    print_dataframe(sql_result, title="SQL schema_of_json Result")

    # Extract the DDL string for programmatic use
    sql_ddl = sql_result.collect()[0][0]
    logger.info("SQL DDL output: %s", sql_ddl)

    # Multi-field complex example via SQL
    complex_sql = spark.sql("""
        SELECT schema_of_json(
            '{"order_id":101,"items":[{"sku":"A1","qty":2}],"total":59.99}'
        ) AS complex_schema
    """)
    print_dataframe(complex_sql, title="Complex SQL Schema Inference")

    # =========================================================================
    # 7. Dynamic schema for streaming / Kafka payloads
    # =========================================================================
    print_header("7. Dynamic Schema for Streaming Payloads")

    # Simulate a Kafka-like scenario: payload arrives as string
    kafka_sample = '{"event":"click","user_id":42,"ts":"2024-01-15T10:30:00Z","metadata":{"page":"/home"}}'

    # Step 1: Infer schema dynamically from a sample message
    dynamic_ddl = spark.range(1).select(F.schema_of_json(F.lit(kafka_sample))).collect()[0][0]
    logger.info("Kafka payload DDL: %s", dynamic_ddl)

    # Step 2: Apply to a batch of messages (simulating Kafka topic)
    messages = [
        (1, '{"event":"click","user_id":42,"ts":"2024-01-15T10:30:00Z","metadata":{"page":"/home"}}'),
        (2, '{"event":"view","user_id":99,"ts":"2024-01-15T10:31:00Z","metadata":{"page":"/cart"}}'),
        (3, '{"event":"purchase","user_id":42,"ts":"2024-01-15T10:32:00Z","metadata":{"page":"/checkout"}}'),
    ]
    df_kafka = spark.createDataFrame(messages, ["offset", "value"])

    # Step 3: Parse using the dynamically inferred schema
    df_parsed_kafka = df_kafka.withColumn("parsed", F.from_json(F.col("value"), dynamic_ddl)).select(
        "offset", "parsed.*"
    )
    print_dataframe(df_parsed_kafka, title="Kafka Messages Parsed with Dynamic Schema")

    # =========================================================================
    # 8. schema_of_json with options (timestamp format)
    # =========================================================================
    print_header("8. schema_of_json with Options")

    ts_sample = '{"created":"2024-01-15 10:30:00","amount":99.5}'
    # Without options — timestamp stays as string
    default_ddl = spark.range(1).select(F.schema_of_json(F.lit(ts_sample))).collect()[0][0]
    logger.info("Default inference: %s", default_ddl)

    # With timestampFormat option — may influence type resolution
    options_ddl = (
        spark.range(1)
        .select(
            F.schema_of_json(
                F.lit(ts_sample),
                {"timestampFormat": "yyyy-MM-dd HH:mm:ss"},
            )
        )
        .collect()[0][0]
    )
    logger.info("With timestampFormat: %s", options_ddl)
    print_success("Options can influence type inference for dates/timestamps")

    # =========================================================================
    # 9. Compare: schema_of_json vs spark.read.json inference
    # =========================================================================
    print_header("9. schema_of_json vs spark.read.json Inference")

    compare_sample = '{"id":1,"name":"Alice","scores":[95,87,92]}'

    # Method 1: schema_of_json (returns DDL string)
    method1_ddl = spark.range(1).select(F.schema_of_json(F.lit(compare_sample))).collect()[0][0]
    logger.info("schema_of_json DDL: %s", method1_ddl)

    # Method 2: spark.read.json with parallelize (returns StructType object)
    method2_schema = spark.read.json(spark.sparkContext.parallelize([compare_sample])).schema
    logger.info("spark.read.json schema: %s", method2_schema.simpleString())
    logger.info("spark.read.json DDL: %s", method2_schema.toDDL())

    # Key difference: schema_of_json returns a Column/string (usable in expressions)
    # spark.read.json().schema returns a StructType object (usable in DataFrame API)
    print_success(
        "schema_of_json → DDL string (for SQL/expressions), spark.read.json().schema → StructType (for DataFrame API)"
    )

    spark.stop()

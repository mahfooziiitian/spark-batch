"""Schema from JSON payload — infer schema from a sample for Kafka/streaming.

Demonstrates generating a StructType schema from a JSON string sample,
then applying it to parse streaming or string columns with from_json().
This is the go-to pattern for Kafka consumers where the value column
contains JSON strings.

Key concepts:
    - Infer schema from a JSON sample using spark.read.json + parallelize
    - Apply inferred schema to from_json() for Kafka value parsing
    - schema_of_json() alternative (returns DDL string)
    - Cache schema for reuse across multiple columns/streams
    - Production pattern: infer once at startup, apply to all batches

Reference:
    https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pys_json import (
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
)
from pys_json._logging import get_logger
from pys_json.schema import schema_to_ddl

set_log_level("DEBUG")
logger = get_logger("example.schema_from_payload")


if __name__ == "__main__":
    spark = get_spark("schema-from-payload")
    sc = spark.sparkContext

    # =========================================================================
    # 1. Infer schema from a JSON sample (parallelize trick)
    # =========================================================================
    print_header("1. Infer Schema from JSON Payload")

    # Simulate a Kafka message payload
    json_sample = '{"user_id": "u-123", "event": "click", "timestamp": "2024-03-15T10:30:00Z", "properties": {"page": "/home", "duration_ms": 1200}}'

    # Infer schema by reading the sample as a JSON dataset
    schema = spark.read.json(sc.parallelize([json_sample])).schema

    print_schema(spark.createDataFrame([], schema), title="Inferred Schema from Payload")
    logger.info("DDL: %s", schema_to_ddl(schema))
    print_success("Schema inferred from a single JSON sample — no files needed")

    # =========================================================================
    # 2. Apply schema with from_json (Kafka pattern)
    # =========================================================================
    print_header("2. Apply to from_json (Kafka Consumer Pattern)")

    # Simulate Kafka messages (key, value as raw JSON strings)
    kafka_messages = [
        (
            "k1",
            '{"user_id": "u-123", "event": "click", "timestamp": "2024-03-15T10:30:00Z", "properties": {"page": "/home", "duration_ms": 1200}}',
        ),
        (
            "k2",
            '{"user_id": "u-456", "event": "view", "timestamp": "2024-03-15T10:31:00Z", "properties": {"page": "/products", "duration_ms": 800}}',
        ),
        (
            "k3",
            '{"user_id": "u-789", "event": "purchase", "timestamp": "2024-03-15T10:32:00Z", "properties": {"page": "/checkout", "duration_ms": 3500}}',
        ),
    ]

    df_kafka = spark.createDataFrame(kafka_messages, ["key", "value"])

    # Parse the JSON value column using the inferred schema
    df_parsed = df_kafka.select(
        "key",
        F.from_json(F.col("value"), schema).alias("data"),
    ).select("key", "data.*")

    print_schema(df_parsed, title="Parsed Kafka Messages")
    print_dataframe(df_parsed, title="Kafka Events")
    print_success("from_json(col('value'), schema) parses Kafka JSON values into typed columns")

    # =========================================================================
    # 3. Nested field access after parsing
    # =========================================================================
    print_header("3. Accessing Nested Fields")

    df_flat = df_parsed.select(
        "key",
        "user_id",
        "event",
        "timestamp",
        F.col("properties.page").alias("page"),
        F.col("properties.duration_ms").alias("duration_ms"),
    )
    print_dataframe(df_flat, title="Flattened Events")

    # =========================================================================
    # 4. schema_of_json alternative
    # =========================================================================
    print_header("4. schema_of_json() Alternative")

    schema_ddl = (
        spark.range(1)
        .select(
            F.schema_of_json(F.lit(json_sample)),
        )
        .collect()[0][0]
    )
    logger.info("schema_of_json DDL: %s", schema_ddl)

    # Use DDL string directly with from_json
    df_ddl = df_kafka.select(
        "key",
        F.from_json(F.col("value"), schema_ddl).alias("data"),
    ).select("key", "data.*")
    print_dataframe(df_ddl, title="Parsed with DDL String")
    print_success("schema_of_json() returns DDL string — usable directly in from_json()")

    # =========================================================================
    # 5. Multiple sample inference (robust schema)
    # =========================================================================
    print_header("5. Multi-Sample Inference (Robust Schema)")

    # When records have different fields, infer from multiple samples
    samples = [
        '{"user_id": "u-123", "event": "click", "metadata": {"browser": "Chrome"}}',
        '{"user_id": "u-456", "event": "purchase", "amount": 99.99, "metadata": {"browser": "Firefox", "os": "Linux"}}',
        '{"user_id": "u-789", "event": "signup", "metadata": {"browser": "Safari"}, "referrer": "google.com"}',
    ]

    robust_schema = spark.read.json(sc.parallelize(samples)).schema
    print_schema(spark.createDataFrame([], robust_schema), title="Robust Schema (union of all samples)")
    print_warning("Pass multiple sample payloads to capture all possible fields")

    # =========================================================================
    # 6. Production pattern: schema registry simulation
    # =========================================================================
    print_header("6. Production Pattern — Reusable Schema")

    # In production: infer once, serialize, reuse
    schema_json = schema.json()  # Serialize to JSON string
    logger.info("Serialized schema (%s chars)", len(schema_json))

    # Deserialize and reuse (e.g., loaded from a schema registry or config)
    import json

    restored_schema = StructType.fromJson(json.loads(schema_json))

    df_restored = df_kafka.select(
        "key",
        F.from_json(F.col("value"), restored_schema).alias("data"),
    ).select("key", "data.*")
    print_dataframe(df_restored, title="Parsed with Restored Schema")
    print_success(
        "Production pattern: infer schema once → serialize to JSON → store in registry/config → deserialize at runtime"
    )

    # =========================================================================
    # 7. Kafka structured streaming template
    # =========================================================================
    print_header("7. Kafka Structured Streaming Template")

    print_warning("Template only — requires a running Kafka broker")
    logger.info(
        "Template:\n"
        "  schema = spark.read.json(sc.parallelize([sample])).schema\n"
        "  df = (spark.readStream\n"
        "      .format('kafka')\n"
        "      .option('subscribe', 'topic')\n"
        "      .load()\n"
        "      .select(from_json(col('value').cast('string'), schema).alias('data'))\n"
        "      .select('data.*'))"
    )

    # Simulated batch equivalent
    kafka_schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    df_stream_sim = spark.createDataFrame(kafka_messages, kafka_schema)
    df_stream_parsed = df_stream_sim.select(
        "key",
        F.from_json(F.col("value").cast("string"), schema).alias("data"),
    ).select("key", "data.*")
    print_dataframe(df_stream_parsed, title="Simulated Kafka Stream Output")
    print_success("Same pattern works for batch and streaming — schema is the key")

    spark.stop()

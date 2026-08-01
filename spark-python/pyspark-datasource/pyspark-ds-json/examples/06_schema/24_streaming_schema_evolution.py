"""Schema evolution in streaming JSON — medallion architecture for evolving schemas.

Demonstrates the bronze/silver/gold pattern for handling JSON schema changes in
streaming pipelines, including raw ingestion, versioned parsing, and error isolation.

Key concepts:
    - Streaming pipelines require stable schemas
    - Bronze layer: ingest raw JSON as string (schema-agnostic)
    - Silver layer: parse with try_to_timestamp/from_json, capture errors
    - Gold layer: analytics-ready model with clean types
    - Schema versioning via detection logic
    - Rescued data column for unexpected fields

Note:
    This example simulates streaming with batch reads for local demonstration.
    In production, replace spark.read with spark.readStream + cloudFiles/Kafka.

Reference:
    https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
"""

import os
import tempfile

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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
logger = get_logger("example.streaming_schema_evolution")


if __name__ == "__main__":
    spark = get_spark("streaming-schema-evolution")
    base_dir = os.path.join(tempfile.gettempdir(), "pys_json", "streaming_evolution")
    os.makedirs(base_dir, exist_ok=True)

    # =========================================================================
    # 1. The problem — schema changes over time
    # =========================================================================
    print_header("1. Schema Changes Over Time")

    day1_file = os.path.join(base_dir, "day1.json")
    day2_file = os.path.join(base_dir, "day2.json")
    day3_file = os.path.join(base_dir, "day3.json")

    write_json_lines(day1_file, [
        '{"id": 1, "name": "Alice"}',
        '{"id": 2, "name": "Bob"}',
    ])
    write_json_lines(day2_file, [
        '{"id": 3, "name": "Charlie", "email": "charlie@test.com"}',
        '{"id": 4, "name": "Diana", "email": "diana@test.com"}',
    ])
    write_json_lines(day3_file, [
        '{"id": 5, "name": {"first": "Eve", "last": "Smith"}}',
        '{"id": 6, "name": {"first": "Frank", "last": "Jones"}, "email": "frank@test.com"}',
    ])

    print_path("Day 1 (basic)", day1_file)
    print_path("Day 2 (+ email)", day2_file)
    print_path("Day 3 (name → struct)", day3_file)
    print_warning(
        "Day 1: name is STRING. Day 3: name becomes STRUCT. "
        "A fixed schema pipeline would break on Day 3."
    )

    # =========================================================================
    # 2. Bronze layer — raw ingestion (schema-agnostic)
    # =========================================================================
    print_header("2. Bronze Layer — Raw Ingestion")

    # Simulate streaming: read all files as text
    bronze_df = (
        spark.read.text(base_dir)
        .withColumnRenamed("value", "raw_json")
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_time", F.current_timestamp())
    )
    print_schema(bronze_df, title="Bronze schema (always stable)")
    print_dataframe(bronze_df.select("raw_json", "source_file"), title="Bronze layer data")
    logger.info("Bronze records: %s", bronze_df.count())
    print_success(
        "Bronze stores raw JSON as STRING — never breaks regardless of schema changes. "
        "Add metadata: source_file, ingestion_time."
    )

    # =========================================================================
    # 3. Schema version detection
    # =========================================================================
    print_header("3. Schema Version Detection")

    bronze_classified = bronze_df.withColumn(
        "schema_version",
        F.when(
            F.get_json_object(F.col("raw_json"), "$.name").startswith("{"),
            F.lit(3),
        )
        .when(
            F.get_json_object(F.col("raw_json"), "$.email").isNotNull(),
            F.lit(2),
        )
        .otherwise(F.lit(1)),
    )
    print_dataframe(
        bronze_classified.select("raw_json", "schema_version"),
        title="Records classified by schema version",
    )
    print_success("Detect schema version from content to route parsing logic")

    # =========================================================================
    # 4. Silver layer — versioned parsing
    # =========================================================================
    print_header("4. Silver Layer — Versioned Parsing")

    # V1 schema: name is string
    v1_records = bronze_classified.filter(F.col("schema_version") == 1)
    df_v1 = v1_records.select(
        F.get_json_object(F.col("raw_json"), "$.id").cast("bigint").alias("id"),
        F.get_json_object(F.col("raw_json"), "$.name").alias("name"),
        F.lit(None).cast("string").alias("email"),
        F.col("ingestion_time"),
    )

    # V2 schema: name is string + email
    v2_records = bronze_classified.filter(F.col("schema_version") == 2)
    df_v2 = v2_records.select(
        F.get_json_object(F.col("raw_json"), "$.id").cast("bigint").alias("id"),
        F.get_json_object(F.col("raw_json"), "$.name").alias("name"),
        F.get_json_object(F.col("raw_json"), "$.email").alias("email"),
        F.col("ingestion_time"),
    )

    # V3 schema: name is struct
    v3_records = bronze_classified.filter(F.col("schema_version") == 3)
    df_v3 = v3_records.select(
        F.get_json_object(F.col("raw_json"), "$.id").cast("bigint").alias("id"),
        F.concat_ws(
            " ",
            F.get_json_object(F.col("raw_json"), "$.name.first"),
            F.get_json_object(F.col("raw_json"), "$.name.last"),
        ).alias("name"),
        F.get_json_object(F.col("raw_json"), "$.email").alias("email"),
        F.col("ingestion_time"),
    )

    # Union all versions into silver
    silver_df = df_v1.unionByName(df_v2).unionByName(df_v3)
    print_schema(silver_df, title="Silver schema (unified)")
    print_dataframe(silver_df.select("id", "name", "email"), title="Silver layer — all versions unified")
    print_success("Each schema version gets its own parsing logic, unified into stable silver schema")

    # =========================================================================
    # 5. Error isolation pattern
    # =========================================================================
    print_header("5. Error Isolation — Quarantine Bad Records")

    # Simulate bad records mixed with good
    mixed_file = os.path.join(base_dir, "mixed_quality.json")
    write_json_lines(mixed_file, [
        '{"id": 10, "name": "Good", "email": "good@test.com"}',
        '{"id": "NOT_A_NUMBER", "name": "Bad ID"}',
        '{"broken json',
        '{"id": 12, "name": "Also Good"}',
    ])

    bronze_mixed = (
        spark.read.text(mixed_file)
        .withColumnRenamed("value", "raw_json")
        .withColumn("ingestion_time", F.current_timestamp())
    )

    # Attempt to parse, flag errors
    silver_attempt = bronze_mixed.withColumn(
        "parsed_id", F.get_json_object(F.col("raw_json"), "$.id")
    ).withColumn(
        "parsed_name", F.get_json_object(F.col("raw_json"), "$.name")
    ).withColumn(
        "parse_status",
        F.when(F.col("parsed_id").isNull(), "parse_failed")
        .when(F.col("parsed_id").rlike(r"^\d+$"), "success")
        .otherwise("invalid_type"),
    )

    # Good records → silver
    silver_good = silver_attempt.filter(F.col("parse_status") == "success").select(
        F.col("parsed_id").cast("bigint").alias("id"),
        F.col("parsed_name").alias("name"),
        F.col("ingestion_time"),
    )

    # Bad records → quarantine
    quarantine = silver_attempt.filter(F.col("parse_status") != "success").select(
        "raw_json", "parse_status", "ingestion_time"
    )

    print_dataframe(silver_good, title="Silver (good records)")
    print_dataframe(quarantine, title="Quarantine (bad records)")
    print_success("Route parse failures to quarantine table for investigation")

    # =========================================================================
    # 6. Streaming code pattern (for reference)
    # =========================================================================
    print_header("6. Production Streaming Pattern (Reference)")

    streaming_code = """
    # Bronze: ingest raw (schema-agnostic)
    bronze = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load("/raw/events/")
        .withColumnRenamed("value", "raw_json")
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_time", current_timestamp())
    )

    # Silver: parse with error handling
    silver = bronze.withColumn(
        "parsed", from_json(col("raw_json"), silver_schema)
    ).withColumn(
        "parse_ok", col("parsed").isNotNull()
    )

    # Write good records to silver
    silver.filter(col("parse_ok")).select("parsed.*", "ingestion_time")
        .writeStream.format("delta").start("/silver/events")

    # Write bad records to quarantine
    silver.filter(~col("parse_ok")).select("raw_json", "ingestion_time")
        .writeStream.format("delta").start("/quarantine/events")
    """

    logger.info("Production streaming pattern:\n%s", streaming_code)
    print_success(
        "In production: readStream → bronze (text) → silver (parsed) → "
        "quarantine (failures). Schema changes never break the bronze layer."
    )

    # =========================================================================
    # 7. Gold layer — analytics-ready model
    # =========================================================================
    print_header("7. Gold Layer — Analytics-Ready")

    gold_df = silver_df.select(
        "id",
        F.upper(F.col("name")).alias("name_upper"),
        F.coalesce(F.col("email"), F.lit("unknown")).alias("email"),
    ).filter(F.col("id").isNotNull())

    print_dataframe(gold_df, title="Gold layer (clean, typed, enriched)")
    print_success(
        "Gold layer: business logic applied on stable silver schema — "
        "immune to upstream JSON format changes"
    )

    # =========================================================================
    # 8. Medallion architecture summary
    # =========================================================================
    print_header("8. Medallion Architecture Summary")

    summary = [
        ("Bronze", "raw_json STRING, ingestion_time TIMESTAMP, source_file STRING", "Never breaks"),
        ("Silver", "id BIGINT, name STRING, email STRING, ingestion_time TIMESTAMP", "Versioned parsing"),
        ("Gold", "Business model with clean types and enrichment", "Analytics-ready"),
        ("Quarantine", "raw_json STRING, parse_status STRING, ingestion_time TIMESTAMP", "Error isolation"),
    ]
    df_summary = spark.createDataFrame(summary, ["Layer", "Schema", "Purpose"])
    print_dataframe(df_summary, title="Medallion Architecture for Evolving JSON")
    print_success(
        "Bronze absorbs all changes. Silver normalizes versions. "
        "Gold serves analytics. Quarantine isolates errors."
    )

    spark.stop()

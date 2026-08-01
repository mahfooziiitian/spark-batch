"""Production-grade Spark JSON parser — complete medallion pipeline with error handling.

Demonstrates a full bronze/silver/gold pipeline for enterprise JSON processing
including raw ingestion, controlled parsing, error routing, schema versioning,
deduplication, and quality checks.

Key concepts:
    - Bronze: raw_json + metadata (source_system, batch_id, ingestion_time)
    - Silver: from_json with explicit schema, parse_status, error_reason
    - Gold: clean types, deduplicated, quality checks passed
    - Reusable parse function pattern
    - Error routing to quarantine
    - Schema versioning for evolving inputs

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

import os
import tempfile
import uuid

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    MapType,
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
logger = get_logger("example.production_parser")


# =============================================================================
# Reusable production functions
# =============================================================================


def ingest_bronze(
    spark,
    path: str,
    source_system: str,
    batch_id: str,
) -> DataFrame:
    """Ingest raw JSON into bronze layer with metadata."""
    return (
        spark.read.text(path)
        .withColumnRenamed("value", "raw_json")
        .withColumn("source_system", F.lit(source_system))
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
    )


def parse_silver(
    bronze_df: DataFrame,
    schema: StructType,
    schema_version: int = 1,
) -> DataFrame:
    """Parse bronze raw JSON into silver with error handling."""
    return (
        bronze_df.withColumn("parsed", F.from_json(F.col("raw_json"), schema))
        .withColumn(
            "parse_status",
            F.when(F.col("parsed").isNull(), F.lit("FAILED")).otherwise(F.lit("SUCCESS")),
        )
        .withColumn(
            "error_reason",
            F.when(F.col("parsed").isNull(), F.lit("JSON parse failure"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn("schema_version", F.lit(schema_version))
        .withColumn("processed_at", F.current_timestamp())
    )


def extract_gold(silver_df: DataFrame, fields: list[str]) -> DataFrame:
    """Extract gold fields from silver, filtering only successful parses."""
    return (
        silver_df.filter(F.col("parse_status") == "SUCCESS")
        .select(*[F.col(f"parsed.{f}").alias(f) for f in fields])
    )


if __name__ == "__main__":
    spark = get_spark("production-parser")
    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "production")
    os.makedirs(out_dir, exist_ok=True)

    # =========================================================================
    # 1. Sample input data (simulating real-world messy JSON)
    # =========================================================================
    print_header("1. Input Data — Realistic Mix")

    input_file = DATA_HOME + "/production_events.json"
    write_json_lines(
        input_file,
        [
            '{"id": "evt-001", "event_type": "purchase", "event_time": "2026-08-01T10:15:30Z", "payload": {"item": "laptop", "price": "999.99"}}',
            '{"id": "evt-002", "event_type": "click", "event_time": "2026-08-01T10:16:00Z", "payload": {"page": "/products", "duration": "5.2"}}',
            '{"id": "evt-003", "event_type": "signup", "event_time": "2026-08-01T10:17:00Z", "payload": {"email": "user@co.com"}}',
            'THIS IS NOT JSON AT ALL',
            '{"id": "evt-005", "event_type": "purchase", "event_time": "2026-08-01T10:18:00Z", "payload": {"item": "mouse", "price": "29.99"}}',
            '',
            '{"id": "evt-006"',
            '{"id": "evt-007", "event_type": "click", "event_time": "2026-08-01T10:20:00Z", "payload": {"page": "/checkout"}}',
            '{"id": "evt-001", "event_type": "purchase", "event_time": "2026-08-01T10:15:30Z", "payload": {"item": "laptop", "price": "999.99"}}',
        ],
    )
    print_path("Input (mixed quality)", input_file)
    logger.info("Input has: valid JSON, corrupt lines, empty lines, truncated JSON, duplicates")

    # =========================================================================
    # 2. Bronze layer
    # =========================================================================
    print_header("2. Bronze Layer — Raw Ingestion")

    batch_id = str(uuid.uuid4())[:8]
    bronze_df = ingest_bronze(spark, input_file, "event-service", batch_id)
    print_schema(bronze_df, title="Bronze schema")
    print_dataframe(
        bronze_df.select("raw_json", "source_system", "batch_id").limit(5),
        title="Bronze layer (first 5 rows)",
    )
    logger.info("Bronze records: %s", bronze_df.count())
    print_success("Bronze stores everything — no parsing, no data loss")

    # =========================================================================
    # 3. Silver layer — parse with error handling
    # =========================================================================
    print_header("3. Silver Layer — Controlled Parsing")

    event_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("payload", MapType(StringType(), StringType()), True),
        ]
    )

    silver_df = parse_silver(bronze_df, event_schema, schema_version=1)
    print_schema(silver_df, title="Silver schema")

    # Show parse results
    status_summary = silver_df.groupBy("parse_status").count()
    print_dataframe(status_summary, title="Parse status summary")

    silver_success = silver_df.filter(F.col("parse_status") == "SUCCESS")
    print_dataframe(
        silver_success.select("parsed.id", "parsed.event_type", "parse_status").limit(5),
        title="Silver — successful parses",
    )

    silver_failed = silver_df.filter(F.col("parse_status") == "FAILED")
    print_dataframe(
        silver_failed.select("raw_json", "error_reason").limit(5),
        title="Silver — failed parses (quarantine)",
    )
    print_success("Silver separates good records from bad — both preserved for investigation")

    # =========================================================================
    # 4. Gold layer — clean, deduplicated, validated
    # =========================================================================
    print_header("4. Gold Layer — Business-Ready Data")

    gold_df = (
        extract_gold(silver_df, ["id", "event_type", "event_time", "payload"])
        .dropDuplicates(["id"])
        .filter(F.col("id").isNotNull())
        .filter(F.col("event_type").isNotNull())
    )
    print_dataframe(gold_df, title="Gold layer (deduplicated, validated)")
    logger.info("Gold records: %s (from %s bronze)", gold_df.count(), bronze_df.count())
    print_success("Gold: deduplicated by ID, null checks passed, clean types")

    # =========================================================================
    # 5. Quality metrics
    # =========================================================================
    print_header("5. Quality Metrics")

    total = bronze_df.count()
    success = silver_df.filter(F.col("parse_status") == "SUCCESS").count()
    failed = silver_df.filter(F.col("parse_status") == "FAILED").count()
    gold_count = gold_df.count()
    duplicates = success - gold_count

    metrics = [
        ("Bronze (total input)", str(total)),
        ("Silver SUCCESS", str(success)),
        ("Silver FAILED", str(failed)),
        ("Duplicates removed", str(duplicates)),
        ("Gold (final)", str(gold_count)),
        ("Parse success rate", f"{success / total * 100:.1f}%"),
        ("Data loss (corrupt)", f"{failed / total * 100:.1f}%"),
    ]
    df_metrics = spark.createDataFrame(metrics, ["Metric", "Value"])
    print_dataframe(df_metrics, title="Pipeline Quality Metrics")
    print_success("Track metrics at each layer for monitoring and alerting")

    # =========================================================================
    # 6. Enhanced error classification
    # =========================================================================
    print_header("6. Enhanced Error Classification")

    silver_enhanced = bronze_df.withColumn(
        "parsed", F.from_json(F.col("raw_json"), event_schema)
    ).withColumn(
        "trimmed", F.trim(F.col("raw_json"))
    ).withColumn(
        "error_class",
        F.when(F.col("trimmed") == "", "empty_line")
        .when(~F.col("trimmed").startswith("{"), "not_json_object")
        .when(F.col("parsed").isNull(), "malformed_json")
        .when(F.col("parsed.id").isNull(), "missing_required_field")
        .otherwise("valid"),
    )

    error_summary = silver_enhanced.groupBy("error_class").count().orderBy("error_class")
    print_dataframe(error_summary, title="Detailed error classification")
    print_success("Classify errors for targeted fixes: empty, not JSON, malformed, missing fields")

    # =========================================================================
    # 7. Write outputs (simulated)
    # =========================================================================
    print_header("7. Output Destinations")

    # Silver output
    silver_path = os.path.join(out_dir, "silver")
    silver_output = silver_df.select(
        "parsed.*", "parse_status", "error_reason", "schema_version",
        "raw_json", "source_system", "batch_id", "processed_at",
    )
    silver_output.write.mode("overwrite").option("compression", "none").parquet(silver_path)
    logger.info("Silver written to: %s", silver_path)

    # Quarantine output
    quarantine_path = os.path.join(out_dir, "quarantine")
    silver_failed.select(
        "raw_json", "error_reason", "source_system", "batch_id", "ingestion_time"
    ).write.mode("overwrite").option("compression", "none").parquet(quarantine_path)
    logger.info("Quarantine written to: %s", quarantine_path)

    # Gold output
    gold_path = os.path.join(out_dir, "gold")
    gold_df.write.mode("overwrite").option("compression", "none").parquet(gold_path)
    logger.info("Gold written to: %s", gold_path)

    destinations = [
        ("Silver", silver_path, "All parsed records + metadata"),
        ("Quarantine", quarantine_path, "Failed records for investigation"),
        ("Gold", gold_path, "Clean business-ready data"),
    ]
    df_dest = spark.createDataFrame(destinations, ["Layer", "Path", "Contents"])
    print_dataframe(df_dest, title="Output destinations")
    print_success("Three outputs: silver (all), quarantine (errors), gold (clean)")

    # =========================================================================
    # 8. Scenarios difficulty summary
    # =========================================================================
    print_header("8. Hardest JSON Scenarios Summary")

    scenarios = [
        ("Schema inference", "High", "Always use explicit schema"),
        ("Deeply nested JSON", "High", "Flatten one level at a time"),
        ("Dynamic keys", "High", "Use MapType"),
        ("Type conflicts", "Very High", "Read unstable fields as STRING"),
        ("Schema evolution", "Very High", "Bronze raw → Silver parsed"),
        ("Multiple arrays", "Very High", "Flatten independently, join back"),
        ("Timestamps", "High", "Read as STRING, parse conditionally"),
        ("Numeric precision", "High", "DECIMAL for money, STRING for IDs"),
        ("Bad/non-standard JSON", "Medium", "Spark options + pre-processing"),
        ("Streaming evolution", "Very High", "Stable bronze + versioned silver"),
    ]
    df_scenarios = spark.createDataFrame(scenarios, ["Scenario", "Difficulty", "Best Practice"])
    print_dataframe(df_scenarios, title="JSON Scenarios Difficulty Matrix")
    print_success(
        "Production pattern: Bronze (raw) → Silver (parsed + errors) → Gold (clean). "
        "This architecture handles ALL scenarios above."
    )

    spark.stop()

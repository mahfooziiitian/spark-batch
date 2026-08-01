"""JSON Schema composition keywords (allOf, anyOf, oneOf) in Spark.

Demonstrates how to handle JSON Schema composition keywords as validation
patterns in Spark, since Spark has no native union type.

Key concepts:
    - allOf: merge schemas + enforce all validations
    - anyOf: parse each branch, valid if match_count >= 1
    - oneOf: parse each branch, valid if match_count == 1
    - Discriminator-based polymorphic parsing
    - Primitive union types (string OR integer) → read as STRING
    - Envelope pattern for event-driven architectures
    - Reusable apply_oneof / apply_anyof utility functions

Reference:
    https://json-schema.org/understanding-json-schema/reference/combining
"""

import os
import tempfile

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.composition_keywords")


# =============================================================================
# Reusable utilities
# =============================================================================


def apply_oneof(df, json_col: str, branches: dict[str, str | StructType]):
    """Parse JSON column against multiple branch schemas; valid if exactly one matches.

    Args:
        df: DataFrame with a raw JSON string column
        json_col: name of the JSON string column
        branches: dict mapping branch_name → schema (DDL string or StructType)

    Returns:
        DataFrame with branch_parsed columns, match_count, and oneof_status
    """
    result_df = df
    for branch_name, branch_schema in branches.items():
        result_df = result_df.withColumn(
            f"{branch_name}_parsed",
            F.from_json(F.col(json_col), branch_schema),
        )

    match_expr = " + ".join(
        f"int({name}_parsed is not null)" for name in branches
    )
    result_df = result_df.withColumn(
        "match_count", F.expr(match_expr)
    ).withColumn(
        "oneof_status",
        F.when(F.col("match_count") == 1, F.lit("VALID"))
        .when(F.col("match_count") == 0, F.lit("NO_MATCH"))
        .otherwise(F.lit("MULTIPLE_MATCH")),
    )
    return result_df


def apply_anyof(df, json_col: str, branches: dict[str, str | StructType]):
    """Parse JSON column against multiple branches; valid if at least one matches."""
    result_df = df
    for branch_name, branch_schema in branches.items():
        result_df = result_df.withColumn(
            f"{branch_name}_parsed",
            F.from_json(F.col(json_col), branch_schema),
        )

    match_expr = " + ".join(
        f"int({name}_parsed is not null)" for name in branches
    )
    result_df = result_df.withColumn(
        "match_count", F.expr(match_expr)
    ).withColumn(
        "anyof_status",
        F.when(F.col("match_count") >= 1, F.lit("VALID"))
        .otherwise(F.lit("NO_MATCH")),
    )
    return result_df


if __name__ == "__main__":
    spark = get_spark("composition-keywords")
    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "composition")
    os.makedirs(out_dir, exist_ok=True)

    # =========================================================================
    # 1. allOf — Schema Merging
    # =========================================================================
    print_header("1. allOf — Merge Schemas + Combined Validations")

    # allOf merges two sub-schemas: base event fields + source tracking fields
    # JSON Schema: allOf: [{event_id, event_time}, {source_system}]
    # Spark: single merged StructType with all fields required

    allof_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("source_system", StringType(), False),
    ])

    allof_file = DATA_HOME + "/composition_allof.json"
    write_json_lines(
        allof_file,
        [
            '{"event_id": "e1", "event_time": "2026-08-01T10:00:00Z", "source_system": "crm"}',
            '{"event_id": "e2", "event_time": "2026-08-01T11:00:00Z", "source_system": "erp"}',
            '{"event_id": "e3", "event_time": "2026-08-01T12:00:00Z"}',
        ],
    )

    df_allof = spark.read.schema(allof_schema).json(allof_file)
    print_schema(df_allof, title="allOf merged schema")
    print_dataframe(df_allof, title="allOf data (null = missing required field)")

    # Validate allOf: all required fields must be present
    validated_allof = df_allof.withColumn(
        "allof_valid",
        F.when(
            F.col("event_id").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("source_system").isNotNull(),
            F.lit("VALID"),
        ).otherwise(F.lit("MISSING_REQUIRED")),
    )
    print_dataframe(validated_allof, title="allOf validation")
    print_success("allOf = merged schema + enforce all required fields from every sub-schema")

    # =========================================================================
    # 2. oneOf — Discriminator-Based Polymorphic Parsing
    # =========================================================================
    print_header("2. oneOf — Payment Type Discriminator")

    card_schema = "payment_type STRING, card_number STRING, expiry STRING"
    bank_schema = "payment_type STRING, account_number STRING, ifsc STRING"

    oneof_file = DATA_HOME + "/composition_oneof.json"
    write_json_lines(
        oneof_file,
        [
            '{"payment_type":"card","card_number":"4111-1111-1111-1111","expiry":"12/30"}',
            '{"payment_type":"bank","account_number":"9876543210","ifsc":"HDFC0001234"}',
            '{"payment_type":"card","account_number":"9999"}',
            '{"payment_type":"unknown","x":"y"}',
        ],
    )

    raw_df = (
        spark.read.text(oneof_file)
        .withColumnRenamed("value", "raw_json")
    )

    # Use discriminator for efficient routing
    classified_df = raw_df.withColumn(
        "payment_type",
        F.get_json_object(F.col("raw_json"), "$.payment_type"),
    )

    # Parse each branch conditionally
    parsed_df = classified_df.withColumn(
        "card_parsed",
        F.when(
            F.col("payment_type") == "card",
            F.from_json(F.col("raw_json"), card_schema),
        ),
    ).withColumn(
        "bank_parsed",
        F.when(
            F.col("payment_type") == "bank",
            F.from_json(F.col("raw_json"), bank_schema),
        ),
    )

    # Enforce oneOf: exactly one match
    validated_df = parsed_df.withColumn(
        "match_count",
        F.expr("int(card_parsed is not null) + int(bank_parsed is not null)"),
    ).withColumn(
        "oneof_status",
        F.when(F.col("match_count") == 1, F.lit("VALID"))
        .when(F.col("match_count") == 0, F.lit("NO_MATCH"))
        .otherwise(F.lit("MULTIPLE_MATCH")),
    )

    # Validate required fields within matched branch
    final_oneof = validated_df.withColumn(
        "validation_detail",
        F.when(
            (F.col("payment_type") == "card")
            & F.col("card_parsed.card_number").isNotNull()
            & F.col("card_parsed.expiry").isNotNull(),
            F.lit("VALID_CARD"),
        )
        .when(
            (F.col("payment_type") == "bank")
            & F.col("bank_parsed.account_number").isNotNull()
            & F.col("bank_parsed.ifsc").isNotNull(),
            F.lit("VALID_BANK"),
        )
        .when(F.col("match_count") == 0, F.lit("NO_MATCH"))
        .otherwise(F.lit("INCOMPLETE")),
    )

    print_dataframe(
        final_oneof.select(
            "payment_type", "match_count", "oneof_status", "validation_detail", "raw_json"
        ),
        title="oneOf validation results",
    )
    print_success("oneOf = exactly one branch matches; use discriminator for routing")

    # =========================================================================
    # 3. anyOf — Contact Validation (email OR phone OR both)
    # =========================================================================
    print_header("3. anyOf — At Least One Branch Must Match")

    anyof_file = DATA_HOME + "/composition_anyof.json"
    write_json_lines(
        anyof_file,
        [
            '{"id": 1, "email": "alice@test.com", "phone": "555-1234"}',
            '{"id": 2, "email": "bob@test.com"}',
            '{"id": 3, "phone": "555-5678"}',
            '{"id": 4}',
        ],
    )

    contact_schema = "id BIGINT, email STRING, phone STRING"
    df_contact = spark.read.schema(contact_schema).json(anyof_file)

    # anyOf: must have email OR phone (or both)
    validated_anyof = df_contact.withColumn(
        "email_match", F.col("email").isNotNull()
    ).withColumn(
        "phone_match", F.col("phone").isNotNull()
    ).withColumn(
        "match_count",
        F.expr("int(email_match) + int(phone_match)"),
    ).withColumn(
        "anyof_status",
        F.when(F.col("match_count") >= 1, F.lit("VALID"))
        .otherwise(F.lit("NO_MATCH")),
    )

    print_dataframe(
        validated_anyof.select("id", "email", "phone", "match_count", "anyof_status"),
        title="anyOf validation (email OR phone)",
    )
    print_success("anyOf = at least one branch matches (match_count >= 1)")

    # =========================================================================
    # 4. Primitive Union Types (string OR integer)
    # =========================================================================
    print_header("4. Primitive Union — Read as STRING, Cast Later")

    union_file = DATA_HOME + "/composition_primitive_union.json"
    write_json_lines(
        union_file,
        [
            '{"customer_id": 1001}',
            '{"customer_id": "CUST-1002"}',
            '{"customer_id": 3003}',
            '{"customer_id": "unknown"}',
        ],
    )

    # Read as STRING (safest for union types)
    df_union = spark.read.schema("customer_id STRING").json(union_file)

    # Classify and cast
    df_typed = df_union.withColumn(
        "is_numeric", F.col("customer_id").rlike("^[0-9]+$")
    ).withColumn(
        "customer_id_long",
        F.when(F.col("is_numeric"), F.col("customer_id").cast("long")),
    ).withColumn(
        "customer_id_str", F.col("customer_id"),
    ).withColumn(
        "type_detected",
        F.when(F.col("is_numeric"), F.lit("integer"))
        .otherwise(F.lit("string")),
    )

    print_dataframe(df_typed, title="Primitive union handling")
    print_success("Primitive unions: read as STRING in Bronze → cast in Silver")

    # =========================================================================
    # 5. Polymorphic Events — Envelope Pattern
    # =========================================================================
    print_header("5. Polymorphic Events — Envelope Pattern")

    events_file = DATA_HOME + "/composition_events.json"
    write_json_lines(
        events_file,
        [
            '{"event_id": "e1", "event_type": "user_created", "payload": {"user_id": "U1", "email": "u1@test.com"}}',
            '{"event_id": "e2", "event_type": "order_created", "payload": {"order_id": "O1", "amount": 100.50}}',
            '{"event_id": "e3", "event_type": "user_created", "payload": {"user_id": "U2", "email": "u2@test.com"}}',
            '{"event_id": "e4", "event_type": "unknown_event", "payload": {"x": "y"}}',
        ],
    )

    raw_events = (
        spark.read.text(events_file)
        .withColumnRenamed("value", "raw_json")
    )

    # Extract envelope fields
    envelope_df = raw_events.select(
        F.col("raw_json"),
        F.get_json_object(F.col("raw_json"), "$.event_id").alias("event_id"),
        F.get_json_object(F.col("raw_json"), "$.event_type").alias("event_type"),
        F.get_json_object(F.col("raw_json"), "$.payload").alias("payload_raw"),
    )

    # Parse branch-specific payloads
    user_schema = "user_id STRING, email STRING"
    order_schema = "order_id STRING, amount DOUBLE"

    parsed_events = envelope_df.withColumn(
        "user_payload",
        F.when(
            F.col("event_type") == "user_created",
            F.from_json(F.col("payload_raw"), user_schema),
        ),
    ).withColumn(
        "order_payload",
        F.when(
            F.col("event_type") == "order_created",
            F.from_json(F.col("payload_raw"), order_schema),
        ),
    )

    # Validate oneOf on payload
    validated_events = parsed_events.withColumn(
        "match_count",
        F.expr("int(user_payload is not null) + int(order_payload is not null)"),
    ).withColumn(
        "status",
        F.when(F.col("match_count") == 1, F.lit("VALID"))
        .when(F.col("match_count") == 0, F.lit("UNKNOWN_EVENT"))
        .otherwise(F.lit("MULTIPLE_MATCH")),
    )

    print_dataframe(
        validated_events.select(
            "event_id", "event_type", "match_count", "status",
            "user_payload", "order_payload",
        ),
        title="Polymorphic events with oneOf validation",
    )
    print_success("Envelope pattern: event_type discriminator → branch-specific from_json")

    # =========================================================================
    # 6. Reusable apply_oneof Utility
    # =========================================================================
    print_header("6. Reusable apply_oneof Utility Function")

    branches = {
        "card": card_schema,
        "bank": bank_schema,
    }

    raw_payments = (
        spark.read.text(oneof_file)
        .withColumnRenamed("value", "raw_json")
    )

    result_df = apply_oneof(raw_payments, "raw_json", branches)
    print_dataframe(
        result_df.select("raw_json", "match_count", "oneof_status"),
        title="apply_oneof utility result",
    )
    print_success("Reusable utility: apply_oneof(df, col, branches) → match_count + status")

    # =========================================================================
    # 7. allOf Conflict Detection
    # =========================================================================
    print_header("7. allOf Conflict Detection")

    print_warning(
        "allOf conflict: if sub-schemas define same field with different types, "
        "the merged schema is unsatisfiable. Always detect conflicts before merging."
    )

    # Demonstrate: amount as string in one sub-schema, number in another
    # Resolution: use the wider type (STRING) and validate
    conflict_file = DATA_HOME + "/composition_allof_conflict.json"
    write_json_lines(
        conflict_file,
        [
            '{"amount": "100.50"}',
            '{"amount": 200}',
            '{"amount": "INVALID"}',
        ],
    )

    # Safest: read as STRING
    df_conflict = spark.read.schema("amount STRING").json(conflict_file)
    df_validated = df_conflict.withColumn(
        "amount_decimal",
        F.expr("try_cast(amount as DECIMAL(18,2))"),
    ).withColumn(
        "is_valid_number",
        F.col("amount_decimal").isNotNull(),
    )
    print_dataframe(df_validated, title="allOf conflict resolution: read as STRING, cast later")
    print_success("allOf conflicts: use widest type (STRING) + downstream validation")

    # =========================================================================
    # Summary
    # =========================================================================
    print_header("Summary — Composition Keywords in Spark")

    summary_data = [
        ("allOf", "merge schemas", "Merge all fields; validate all constraints"),
        ("anyOf", "match_count >= 1", "At least one branch must match"),
        ("oneOf", "match_count == 1", "Exactly one branch must match"),
        ("Primitive union", "read as STRING", "Cast and validate in Silver"),
        ("Polymorphic", "discriminator field", "Route by event_type/payment_type"),
    ]
    df_summary = spark.createDataFrame(
        summary_data, ["Keyword", "Spark Rule", "Description"]
    )
    print_dataframe(df_summary, title="JSON Schema composition → Spark patterns")

    spark.stop()

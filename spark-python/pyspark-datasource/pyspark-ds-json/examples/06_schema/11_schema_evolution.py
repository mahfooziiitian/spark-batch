"""Schema evolution — handling changing JSON schemas over time.

Demonstrates strategies for dealing with JSON data whose schema changes
across batches: new fields appear, types change, fields are removed.

Key concepts:
    - Schema merging: combine schemas from multiple sources
    - mergeSchema option: let Spark merge schemas across files
    - Adding columns with default values for missing fields
    - Handling type changes (int→string) with PERMISSIVE mode
    - Using _corrupt_record + relaxed schema for migration
    - Schema versioning patterns

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pys_json import (
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
from pys_json.schema import merge_schemas, schema_to_ddl

set_log_level("DEBUG")
logger = get_logger("example.schema_evolution")


if __name__ == "__main__":
    import os
    import tempfile

    spark = get_spark("schema-evolution")
    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "schema_evolution")

    # =========================================================================
    # 1. New fields added over time
    # =========================================================================
    print_header("1. New Fields Added Over Time")

    # Batch 1 — original schema
    batch1 = [
        '{"id": 1, "name": "Alice"}',
        '{"id": 2, "name": "Bob"}',
    ]
    # Batch 2 — email field added
    batch2 = [
        '{"id": 3, "name": "Charlie", "email": "charlie@co.com"}',
        '{"id": 4, "name": "Diana", "email": "diana@co.com"}',
    ]
    # Batch 3 — phone field added
    batch3 = [
        '{"id": 5, "name": "Eve", "email": "eve@co.com", "phone": "555-0101"}',
    ]

    b1_file = os.path.join(out_dir, "batch1.json")
    b2_file = os.path.join(out_dir, "batch2.json")
    b3_file = os.path.join(out_dir, "batch3.json")
    write_json_lines(b1_file, batch1)
    write_json_lines(b2_file, batch2)
    write_json_lines(b3_file, batch3)

    # Read all batches together — Spark infers the union schema
    df_all = spark.read.json([b1_file, b2_file, b3_file])
    print_schema(df_all, title="Inferred Union Schema")
    print_dataframe(df_all, title="All Batches (missing fields → null)")
    print_success("Spark auto-fills missing fields with null when reading multiple files")

    # =========================================================================
    # 2. Explicit schema with merge_schemas()
    # =========================================================================
    print_header("2. Merging Schemas Programmatically")

    schema_v1 = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
        ]
    )
    schema_v2 = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("email", StringType(), True),
        ]
    )
    schema_v3 = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
        ]
    )

    merged = merge_schemas(schema_v1, schema_v2, schema_v3)
    print_schema(spark.createDataFrame([], merged), title="Merged Schema (v1+v2+v3)")
    logger.info("DDL: %s", schema_to_ddl(merged))

    # Apply merged schema to read all batches
    df_merged = spark.read.schema(merged).json([b1_file, b2_file, b3_file])
    print_dataframe(df_merged, title="Read with Merged Schema")
    print_success("merge_schemas() combines fields from all versions")

    # =========================================================================
    # 3. Type changes (int → string)
    # =========================================================================
    print_header("3. Handling Type Changes")

    # Original: age is integer
    type_v1 = [
        '{"id": 1, "name": "Alice", "age": 30}',
        '{"id": 2, "name": "Bob", "age": 25}',
    ]
    # New: age is now a string range
    type_v2 = [
        '{"id": 3, "name": "Charlie", "age": "30-35"}',
        '{"id": 4, "name": "Diana", "age": "25-30"}',
    ]

    tv1_file = os.path.join(out_dir, "type_v1.json")
    tv2_file = os.path.join(out_dir, "type_v2.json")
    write_json_lines(tv1_file, type_v1)
    write_json_lines(tv2_file, type_v2)

    # Naive read — Spark infers the widest type (string)
    df_type_inferred = spark.read.json([tv1_file, tv2_file])
    print_schema(df_type_inferred, title="Inferred Schema (type conflict)")
    print_dataframe(df_type_inferred, title="Type Widened to StringType")
    print_warning("When types conflict, Spark widens to StringType — integers become strings")

    # Explicit schema + PERMISSIVE to detect issues
    strict_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    df_strict = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(strict_schema)
        .json([tv1_file, tv2_file])
        .cache()
    )
    print_dataframe(df_strict, title="Strict Schema — Type Mismatches Caught")

    corrupt_count = df_strict.filter(F.col("_corrupt_record").isNotNull()).count()
    logger.info("Detected %s records with type mismatches", corrupt_count)
    print_success("Use PERMISSIVE + _corrupt_record to detect type changes in production")

    # =========================================================================
    # 4. Adding default values for missing fields
    # =========================================================================
    print_header("4. Default Values for Missing Fields")

    df_defaults = (
        spark.read.schema(merged)
        .json(b1_file)
        .select(
            "id",
            "name",
            F.coalesce(F.col("email"), F.lit("unknown@example.com")).alias("email"),
            F.coalesce(F.col("phone"), F.lit("N/A")).alias("phone"),
        )
    )
    print_dataframe(df_defaults, title="Old Data with Defaults Applied")
    print_success("Use coalesce() to fill missing fields with default values")

    # =========================================================================
    # 5. Fields removed over time
    # =========================================================================
    print_header("5. Fields Removed Over Time")

    # V1 has legacy_field, V2 does not
    remove_v1 = [
        '{"id": 1, "name": "Alice", "legacy_code": "ABC123"}',
    ]
    remove_v2 = [
        '{"id": 2, "name": "Bob"}',
    ]

    rv1_file = os.path.join(out_dir, "remove_v1.json")
    rv2_file = os.path.join(out_dir, "remove_v2.json")
    write_json_lines(rv1_file, remove_v1)
    write_json_lines(rv2_file, remove_v2)

    # Read with union schema — removed fields appear as null in new data
    df_removed = spark.read.json([rv1_file, rv2_file])
    print_dataframe(df_removed, title="Removed Fields → null in New Rows")

    # Drop the legacy column for downstream
    df_clean = df_removed.drop("legacy_code")
    print_dataframe(df_clean, title="After Dropping Legacy Column")
    print_success("Removed fields naturally become null, then drop() for cleanup")

    # =========================================================================
    # 6. Schema versioning pattern
    # =========================================================================
    print_header("6. Schema Versioning Pattern")

    # Add a _schema_version field to each batch
    versioned_v1 = [
        '{"_schema_version": 1, "id": 1, "name": "Alice"}',
    ]
    versioned_v2 = [
        '{"_schema_version": 2, "id": 2, "name": "Bob", "email": "bob@co.com"}',
    ]
    versioned_v3 = [
        '{"_schema_version": 3, "id": 3, "name": "Charlie", "email": "c@co.com", "role": "admin"}',
    ]

    vv1 = os.path.join(out_dir, "versioned_v1.json")
    vv2 = os.path.join(out_dir, "versioned_v2.json")
    vv3 = os.path.join(out_dir, "versioned_v3.json")
    write_json_lines(vv1, versioned_v1)
    write_json_lines(vv2, versioned_v2)
    write_json_lines(vv3, versioned_v3)

    df_versioned = spark.read.json([vv1, vv2, vv3])
    print_dataframe(df_versioned, title="All Versions Together")

    # Process each version separately
    for version in [1, 2, 3]:
        count = df_versioned.filter(F.col("_schema_version") == version).count()
        logger.info("Schema v%s: %s records", version, count)

    # Normalize to latest schema
    df_normalized = df_versioned.select(
        "_schema_version",
        "id",
        "name",
        F.coalesce("email", F.lit("")).alias("email"),
        F.coalesce("role", F.lit("user")).alias("role"),
    )
    print_dataframe(df_normalized, title="Normalized to Latest Schema (v3)")
    print_success(
        "Add _schema_version to your JSON records for safe migration — "
        "route each version through its own transformation logic"
    )

    # =========================================================================
    # 7. Schema comparison across batches
    # =========================================================================
    print_header("7. Schema Comparison")

    df_b1 = spark.read.json(b1_file)
    df_b3 = spark.read.json(b3_file)

    fields_b1 = {f.name for f in df_b1.schema.fields}
    fields_b3 = {f.name for f in df_b3.schema.fields}

    added = fields_b3 - fields_b1
    removed = fields_b1 - fields_b3
    common = fields_b1 & fields_b3

    logger.info("Common fields: %s", sorted(common))
    logger.info("Added fields:  %s", sorted(added))
    logger.info("Removed fields: %s", sorted(removed))

    comparison = [
        ("common", ", ".join(sorted(common))),
        ("added", ", ".join(sorted(added))),
        ("removed", ", ".join(sorted(removed)) if removed else "(none)"),
    ]
    df_comparison = spark.createDataFrame(comparison, ["change_type", "fields"])
    print_dataframe(df_comparison, title="Schema Diff: batch1 vs batch3")
    print_success("Compare field sets across batches to detect schema drift")

    spark.stop()

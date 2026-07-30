"""Validate Spark schema JSON files (not data files).

Demonstrates how to validate a Spark schema definition file — the JSON output
of ``df.schema.json()`` or ``schema.jsonValue()`` — without needing any data.

Key concepts:
    - StructType.fromJson() for basic validation
    - Enterprise checks: duplicates, type validity, nullability, nested schemas
    - Schema drift detection against a baseline
    - Using the pys_json.schema.validate_schema_file() utility
    - Programmatic schema comparison

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType.fromJson
"""

import json
import tempfile
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pys_json import print_header, print_success, set_log_level
from pys_json._logging import get_logger
from pys_json.schema import validate_schema_file

set_log_level("DEBUG")
logger = get_logger("example.validate_schema")


def _write_json(data: object, path: Path) -> str:
    """Write JSON data to a file and return the path string."""
    path.write_text(json.dumps(data, indent=2))
    return str(path)


if __name__ == "__main__":
    tmp = Path(tempfile.mkdtemp(prefix="schema_validate_"))

    # =========================================================================
    # 1. Valid Spark schema file
    # =========================================================================
    print_header("1. Validate a Valid Spark Schema File")

    valid_schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": False, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "salary", "type": "double", "nullable": True, "metadata": {}},
        ],
    }
    valid_path = _write_json(valid_schema, tmp / "valid_schema.json")

    result = validate_schema_file(valid_path)
    logger.info("Valid: %s", result.valid)
    logger.info("Schema: %s", result.schema)
    logger.info("Errors: %s", result.errors)
    print_success("Schema file is valid")

    # =========================================================================
    # 2. Invalid JSON syntax
    # =========================================================================
    print_header("2. Invalid JSON Syntax")

    invalid_json_path = tmp / "bad_syntax.json"
    invalid_json_path.write_text('{"type": "struct", "fields": [}')  # broken JSON

    result = validate_schema_file(str(invalid_json_path))
    logger.info("Valid: %s", result.valid)
    logger.info("Errors: %s", result.errors)
    print_success("Detected invalid JSON syntax")

    # =========================================================================
    # 3. Wrong root type
    # =========================================================================
    print_header("3. Wrong Root Type")

    wrong_root = {"type": "array", "elementType": "string", "containsNull": True}
    wrong_path = _write_json(wrong_root, tmp / "wrong_root.json")

    result = validate_schema_file(wrong_path)
    logger.info("Valid: %s", result.valid)
    logger.info("Errors: %s", result.errors)
    print_success("Detected non-struct root type")

    # =========================================================================
    # 4. Duplicate field names
    # =========================================================================
    print_header("4. Duplicate Field Names")

    dupe_schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
        ],
    }
    dupe_path = _write_json(dupe_schema, tmp / "dupe_fields.json")

    result = validate_schema_file(dupe_path)
    logger.info("Valid: %s", result.valid)
    logger.info("Errors: %s", result.errors)
    print_success("Detected duplicate field names")

    # =========================================================================
    # 5. Invalid data types
    # =========================================================================
    print_header("5. Invalid Data Types")

    bad_types = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "int", "nullable": True, "metadata": {}},
            {"name": "price", "type": "number", "nullable": True, "metadata": {}},
            {"name": "valid", "type": "boolean", "nullable": True, "metadata": {}},
        ],
    }
    bad_path = _write_json(bad_types, tmp / "bad_types.json")

    result = validate_schema_file(bad_path)
    logger.info("Valid: %s", result.valid)
    logger.info("Errors: %s", result.errors)
    logger.info("Note: 'int' is not valid — use 'integer'. 'number' is not valid — use 'double'.")
    print_success("Detected invalid Spark types")

    # =========================================================================
    # 6. Nested struct validation
    # =========================================================================
    print_header("6. Nested Struct Validation")

    nested_schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": False, "metadata": {}},
            {
                "name": "address",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"name": "street", "type": "string", "nullable": True, "metadata": {}},
                        {"name": "city", "type": "string", "nullable": True, "metadata": {}},
                        {"name": "zip", "type": "string", "nullable": True, "metadata": {}},
                    ],
                },
                "nullable": True,
                "metadata": {},
            },
            {
                "name": "tags",
                "type": {"type": "array", "elementType": "string", "containsNull": True},
                "nullable": True,
                "metadata": {},
            },
        ],
    }
    nested_path = _write_json(nested_schema, tmp / "nested_schema.json")

    result = validate_schema_file(nested_path)
    logger.info("Valid: %s", result.valid)
    logger.info("Schema: %s", result.schema)
    logger.info("Schema DDL: %s", result.schema.simpleString() if result.schema else "N/A")
    print_success("Nested structs and arrays validated recursively")

    # =========================================================================
    # 7. Schema drift detection
    # =========================================================================
    print_header("7. Schema Drift Detection Against Baseline")

    # Baseline (expected) schema
    expected = StructType(
        [
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("salary", DoubleType(), True),
        ]
    )

    # Actual schema file (drifted: missing email, added phone, salary changed to string)
    drifted_schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": False, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "salary", "type": "string", "nullable": True, "metadata": {}},
            {"name": "phone", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    drifted_path = _write_json(drifted_schema, tmp / "drifted_schema.json")

    result = validate_schema_file(drifted_path, expected_schema=expected)
    logger.info("Valid (structurally): %s", result.valid)
    logger.info("Warnings (drift): %s", result.warnings)
    print_success("Schema drift detected: missing fields, extra fields, type changes")

    # =========================================================================
    # 8. Nullability warnings
    # =========================================================================
    print_header("8. Nullability Check")

    strict_schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": False, "metadata": {}},
            {"name": "name", "type": "string", "nullable": False, "metadata": {}},
            {"name": "email", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    strict_path = _write_json(strict_schema, tmp / "strict_schema.json")

    result = validate_schema_file(strict_path, check_nullability=True)
    logger.info("Valid: %s", result.valid)
    logger.info("Warnings: %s", result.warnings)
    print_success("Non-nullable fields flagged as potential read failure points")

    # =========================================================================
    # 9. Manual validation with StructType.fromJson
    # =========================================================================
    print_header("9. Manual Validation with StructType.fromJson")

    schema_file = tmp / "manual_test.json"
    schema_file.write_text(
        json.dumps(
            {
                "type": "struct",
                "fields": [
                    {"name": "user_id", "type": "integer", "nullable": True, "metadata": {}},
                    {"name": "event", "type": "string", "nullable": True, "metadata": {}},
                    {
                        "name": "metadata",
                        "type": {
                            "type": "map",
                            "keyType": "string",
                            "valueType": "string",
                            "valueContainsNull": True,
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                ],
            }
        )
    )

    with open(schema_file) as f:
        schema_dict = json.load(f)

    try:
        parsed_schema = StructType.fromJson(schema_dict)
        logger.info("Parsed successfully: %s", parsed_schema.simpleString())
        logger.info("Field count: %d", len(parsed_schema.fields))
        logger.info("Field names: %s", parsed_schema.fieldNames())
    except Exception as e:
        logger.error("Validation failed: %s", e)

    print_success("StructType.fromJson is the canonical Spark schema validator")

    # =========================================================================
    # 10. Compare two schema files
    # =========================================================================
    print_header("10. Compare Two Schema Files")

    schema_v1 = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": False, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    schema_v2 = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": False, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "email", "type": "string", "nullable": True, "metadata": {}},
            {"name": "created_at", "type": "timestamp", "nullable": True, "metadata": {}},
        ],
    }

    v1_path = _write_json(schema_v1, tmp / "schema_v1.json")
    v2_path = _write_json(schema_v2, tmp / "schema_v2.json")

    s1 = StructType.fromJson(schema_v1)
    s2 = StructType.fromJson(schema_v2)

    if s1 == s2:
        logger.info("Schemas are identical")
    else:
        logger.info("Schemas differ:")
        v1_cols = {f.name for f in s1.fields}
        v2_cols = {f.name for f in s2.fields}
        added = v2_cols - v1_cols
        removed = v1_cols - v2_cols
        if added:
            logger.info("  Added columns: %s", sorted(added))
        if removed:
            logger.info("  Removed columns: %s", sorted(removed))

        # Type changes in common fields
        v1_map = {f.name: f.dataType for f in s1.fields}
        v2_map = {f.name: f.dataType for f in s2.fields}
        for col in sorted(v1_cols & v2_cols):
            if str(v1_map[col]) != str(v2_map[col]):
                logger.info("  Type change '%s': %s → %s", col, v1_map[col], v2_map[col])

    print_success("Schema comparison complete — 10 validation patterns demonstrated")

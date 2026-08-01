"""JSON Schema ↔ Spark Schema — bidirectional conversion and production patterns.

Demonstrates converting between JSON Schema (Draft-07/2020-12) and Spark StructType,
covering all type mappings, nested structures, arrays, maps, required fields,
enums, and union types (oneOf/anyOf).

Key concepts:
    - JSON Schema is the canonical schema definition format
    - Spark StructType is the runtime execution schema
    - Bidirectional conversion: JSON Schema → Spark and Spark → JSON Schema
    - Type mapping: string→STRING, integer→BIGINT, number→DOUBLE, boolean→BOOLEAN
    - additionalProperties → MapType
    - required fields → nullable=False
    - oneOf/anyOf → StringType (safest fallback)
    - Store JSON Schema in Git as single source of truth

Reference:
    https://json-schema.org/draft/2020-12/json-schema-core
"""

import json
import os
import tempfile

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
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
logger = get_logger("example.json_schema_spark")


# =============================================================================
# Converter: JSON Schema → Spark StructType
# =============================================================================


def json_schema_to_spark(
    schema: dict, required_fields: list[str] | None = None
) -> StructType:
    """Convert a JSON Schema object to a Spark StructType."""
    if required_fields is None:
        required_fields = schema.get("required", [])

    properties = schema.get("properties", {})
    fields = []

    for name, prop in properties.items():
        nullable = name not in required_fields
        spark_type = _resolve_type(prop)
        fields.append(StructField(name, spark_type, nullable))

    # Handle additionalProperties as a MapType field
    if "additionalProperties" in schema and not properties:
        value_type = _resolve_primitive(schema["additionalProperties"])
        return StructType([StructField("_map", MapType(StringType(), value_type), True)])

    return StructType(fields)


def _resolve_type(prop: dict):
    """Resolve a JSON Schema property to a Spark DataType."""
    # Handle oneOf/anyOf → StringType (safest)
    if "oneOf" in prop or "anyOf" in prop:
        return StringType()

    json_type = prop.get("type", "string")

    if json_type == "object":
        if "additionalProperties" in prop:
            value_type = _resolve_primitive(prop["additionalProperties"])
            return MapType(StringType(), value_type)
        return json_schema_to_spark(prop, prop.get("required", []))

    if json_type == "array":
        items = prop.get("items", {"type": "string"})
        return ArrayType(_resolve_type(items))

    # Check format for special types
    fmt = prop.get("format", "")
    if fmt == "decimal":
        precision = prop.get("precision", 38)
        scale = prop.get("scale", 18)
        return DecimalType(precision, scale)

    return _resolve_primitive(prop)


def _resolve_primitive(prop: dict):
    """Map JSON Schema primitive type to Spark DataType."""
    type_map = {
        "string": StringType(),
        "integer": LongType(),
        "number": DoubleType(),
        "boolean": BooleanType(),
    }
    json_type = prop.get("type", "string")
    return type_map.get(json_type, StringType())


# =============================================================================
# Converter: Spark StructType → JSON Schema
# =============================================================================


def spark_to_json_schema(schema: StructType) -> dict:
    """Convert a Spark StructType to a JSON Schema object."""
    properties = {}
    required = []

    for field in schema.fields:
        properties[field.name] = _spark_type_to_json(field.dataType)
        if not field.nullable:
            required.append(field.name)

    result: dict = {"type": "object", "properties": properties}
    if required:
        result["required"] = required
    return result


def _spark_type_to_json(data_type) -> dict:
    """Convert a Spark DataType to a JSON Schema type."""
    if isinstance(data_type, StringType):
        return {"type": "string"}
    if isinstance(data_type, (LongType,)):
        return {"type": "integer"}
    if isinstance(data_type, DoubleType):
        return {"type": "number"}
    if isinstance(data_type, BooleanType):
        return {"type": "boolean"}
    if isinstance(data_type, DecimalType):
        return {"type": "string", "format": "decimal"}
    if isinstance(data_type, ArrayType):
        return {"type": "array", "items": _spark_type_to_json(data_type.elementType)}
    if isinstance(data_type, MapType):
        return {
            "type": "object",
            "additionalProperties": _spark_type_to_json(data_type.valueType),
        }
    if isinstance(data_type, StructType):
        return spark_to_json_schema(data_type)
    return {"type": "string"}


if __name__ == "__main__":
    spark = get_spark("json-schema-spark")
    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "schema_convert")
    os.makedirs(out_dir, exist_ok=True)

    # =========================================================================
    # 1. Basic JSON Schema → Spark conversion
    # =========================================================================
    print_header("1. JSON Schema → Spark StructType")

    customer_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "salary": {"type": "number"},
            "active": {"type": "boolean"},
        },
        "required": ["id", "name"],
    }

    spark_schema = json_schema_to_spark(customer_schema)
    print_schema(spark.createDataFrame([], spark_schema), title="Converted Spark schema")
    logger.info("JSON Schema:\n%s", json.dumps(customer_schema, indent=2))
    print_success("JSON Schema → Spark: integer→BIGINT, number→DOUBLE, required→nullable=False")

    # Use it to read data
    customer_file = DATA_HOME + "/schema_convert_customer.json"
    write_json_lines(
        customer_file,
        [
            '{"id": 1, "name": "Alice", "salary": 75000.50, "active": true}',
            '{"id": 2, "name": "Bob", "salary": 65000.00, "active": false}',
        ],
    )
    df_customer = spark.read.schema(spark_schema).json(customer_file)
    print_dataframe(df_customer, title="Data read with converted schema")

    # =========================================================================
    # 2. Nested object mapping
    # =========================================================================
    print_header("2. Nested Objects")

    nested_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "address": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"},
                    "country": {"type": "string"},
                    "zip": {"type": "string"},
                },
                "required": ["city"],
            },
        },
        "required": ["id"],
    }

    spark_nested = json_schema_to_spark(nested_schema)
    print_schema(spark.createDataFrame([], spark_nested), title="Nested struct schema")

    nested_file = DATA_HOME + "/schema_convert_nested.json"
    write_json_lines(
        nested_file,
        [
            '{"id": 1, "address": {"city": "NYC", "country": "US", "zip": "10001"}}',
            '{"id": 2, "address": {"city": "LA", "country": "US"}}',
        ],
    )
    df_nested = spark.read.schema(spark_nested).json(nested_file)
    print_dataframe(df_nested, title="Nested data")
    print_success("Nested object → StructType with inner StructType")

    # =========================================================================
    # 3. Array and Array of Struct
    # =========================================================================
    print_header("3. Arrays and Array of Struct")

    order_schema = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "sku": {"type": "string"},
                        "qty": {"type": "integer"},
                    },
                },
            },
        },
        "required": ["order_id"],
    }

    spark_order = json_schema_to_spark(order_schema)
    print_schema(spark.createDataFrame([], spark_order), title="Array schema")

    order_file = DATA_HOME + "/schema_convert_order.json"
    write_json_lines(
        order_file,
        [
            '{"order_id": "O1", "tags": ["rush", "vip"], "items": [{"sku": "A1", "qty": 2}, {"sku": "B2", "qty": 1}]}',
        ],
    )
    df_order = spark.read.schema(spark_order).json(order_file)
    print_dataframe(df_order, title="Array data")
    print_success("array→ArrayType, array of objects→ArrayType(StructType)")

    # =========================================================================
    # 4. Dynamic keys → MapType
    # =========================================================================
    print_header("4. additionalProperties → MapType")

    metrics_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "metrics": {
                "type": "object",
                "additionalProperties": {"type": "number"},
            },
        },
        "required": ["id"],
    }

    spark_metrics = json_schema_to_spark(metrics_schema)
    print_schema(spark.createDataFrame([], spark_metrics), title="MapType schema")

    metrics_file = DATA_HOME + "/schema_convert_metrics.json"
    write_json_lines(
        metrics_file,
        [
            '{"id": 1, "metrics": {"cpu": 80.5, "memory": 65.2, "disk": 90.0}}',
            '{"id": 2, "metrics": {"gpu": 40.0, "network": 100.0}}',
        ],
    )
    df_metrics = spark.read.schema(spark_metrics).json(metrics_file)
    print_dataframe(df_metrics, title="Dynamic keys as MapType")
    print_success("additionalProperties → MapType(StringType, valueType)")

    # =========================================================================
    # 5. Decimal mapping
    # =========================================================================
    print_header("5. Decimal Type Mapping")

    finance_schema = {
        "type": "object",
        "properties": {
            "txn_id": {"type": "string"},
            "amount": {"type": "string", "format": "decimal", "precision": 18, "scale": 2},
            "fee_rate": {"type": "string", "format": "decimal", "precision": 10, "scale": 6},
        },
        "required": ["txn_id", "amount"],
    }

    spark_finance = json_schema_to_spark(finance_schema)
    print_schema(spark.createDataFrame([], spark_finance), title="Financial schema")
    print_success("format:'decimal' → DecimalType(precision, scale) for exact arithmetic")

    # =========================================================================
    # 6. oneOf/anyOf → StringType
    # =========================================================================
    print_header("6. oneOf/anyOf → StringType (Union Types)")

    union_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "value": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "integer"},
                ],
            },
        },
        "required": ["id"],
    }

    spark_union = json_schema_to_spark(union_schema)
    print_schema(spark.createDataFrame([], spark_union), title="Union type schema")
    print_warning("Spark has no union type — oneOf/anyOf mapped to StringType (safest)")

    # =========================================================================
    # 7. Spark → JSON Schema (reverse)
    # =========================================================================
    print_header("7. Spark StructType → JSON Schema (Reverse)")

    sample_schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("scores", ArrayType(LongType()), True),
            StructField(
                "address",
                StructType(
                    [
                        StructField("city", StringType(), False),
                        StructField("zip", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    json_output = spark_to_json_schema(sample_schema)
    logger.info("Generated JSON Schema:\n%s", json.dumps(json_output, indent=2))

    # Write to file
    schema_file = os.path.join(out_dir, "generated.schema.json")
    with open(schema_file, "w") as f:
        json.dump(json_output, f, indent=2)
    print_path("Generated JSON Schema file", schema_file)
    print_success("Spark → JSON Schema: nullable=False→required, StructType→object")

    # =========================================================================
    # 8. Production pattern — schema from Git
    # =========================================================================
    print_header("8. Production — Schema from Git File")

    # Simulate reading schema from a Git-managed file
    schema_dir = os.path.join(out_dir, "schemas")
    os.makedirs(schema_dir, exist_ok=True)

    event_schema_def = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Event",
        "type": "object",
        "properties": {
            "event_id": {"type": "string"},
            "event_type": {"type": "string", "enum": ["click", "purchase", "signup"]},
            "timestamp": {"type": "string", "format": "date-time"},
            "user_id": {"type": "string"},
            "payload": {
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
        },
        "required": ["event_id", "event_type", "timestamp"],
    }

    schema_path = os.path.join(schema_dir, "event.schema.json")
    with open(schema_path, "w") as f:
        json.dump(event_schema_def, f, indent=2)
    print_path("Schema file (in Git)", schema_path)

    # Load and convert at runtime
    with open(schema_path) as f:
        loaded_schema = json.load(f)

    spark_event_schema = json_schema_to_spark(loaded_schema)
    print_schema(spark.createDataFrame([], spark_event_schema), title="Runtime Spark schema")

    # Read data with it
    event_file = DATA_HOME + "/schema_convert_events.json"
    write_json_lines(
        event_file,
        [
            '{"event_id": "e1", "event_type": "click", "timestamp": "2026-08-01T10:00:00Z", "user_id": "u1", "payload": {"page": "/home"}}',
            '{"event_id": "e2", "event_type": "purchase", "timestamp": "2026-08-01T10:05:00Z", "user_id": "u2", "payload": {"item": "laptop", "amount": "999"}}',
        ],
    )
    df_events = spark.read.schema(spark_event_schema).json(event_file)
    print_dataframe(df_events, title="Events read with Git-managed schema")
    print_success(
        "Production: JSON Schema in Git → load at runtime → convert to Spark StructType → read data"
    )

    # =========================================================================
    # 9. Type mapping reference
    # =========================================================================
    print_header("9. Complete Type Mapping Reference")

    mappings = [
        ("string", "StringType()", "STRING"),
        ("integer", "LongType()", "BIGINT"),
        ("number", "DoubleType()", "DOUBLE"),
        ("boolean", "BooleanType()", "BOOLEAN"),
        ("string+format:decimal", "DecimalType(p,s)", "DECIMAL(p,s)"),
        ("object", "StructType([...])", "STRUCT<...>"),
        ("object+additionalProperties", "MapType(K,V)", "MAP<K,V>"),
        ("array", "ArrayType(T)", "ARRAY<T>"),
        ("oneOf/anyOf", "StringType()", "STRING"),
        ("required field", "nullable=False", "NOT NULL"),
        ("optional field", "nullable=True", "(default)"),
    ]
    df_map = spark.createDataFrame(mappings, ["JSON Schema", "Spark API", "DDL"])
    print_dataframe(df_map, title="JSON Schema ↔ Spark Type Mapping")
    print_success(
        "Maintain JSON Schema as single source of truth. "
        "Generate Spark, Avro, OpenAPI schemas from it."
    )

    spark.stop()

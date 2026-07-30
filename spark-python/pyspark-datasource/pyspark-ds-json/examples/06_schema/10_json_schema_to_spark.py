"""JSON Schema → PySpark StructType conversion.

Demonstrates converting standard JSON Schema (draft-04/07/2020-12) files
into PySpark StructType schemas using from_json_schema().

Key concepts:
    - from_json_schema(dict | str) → StructType
    - Handles nested objects, arrays, format hints, required fields
    - Maps string+date-time → TimestampType, integer+int32 → IntegerType
    - additionalProperties → MapType, array → ArrayType
    - Read a .json schema file and use it directly with spark.read.schema()

Reference:
    https://json-schema.org/understanding-json-schema
"""

from pys_json import (
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger
from pys_json.schema import from_json_schema, schema_to_ddl

set_log_level("DEBUG")
logger = get_logger("example.json_schema_convert")


if __name__ == "__main__":
    import os
    import tempfile

    spark = get_spark("json-schema-convert")
    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "json_schema_convert")

    # =========================================================================
    # 1. Simple flat schema
    # =========================================================================
    print_header("1. Simple Flat Schema")

    flat_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer", "format": "int32"},
            "active": {"type": "boolean"},
        },
        "required": ["name"],
    }

    spark_schema = from_json_schema(flat_schema)
    print_schema(spark.createDataFrame([], spark_schema), title="Flat Schema")
    logger.info("DDL: %s", schema_to_ddl(spark_schema))
    print_success("string→StringType, integer+int32→IntegerType, boolean→BooleanType")

    # Read JSON data with the converted schema
    data = [
        '{"name": "Alice", "age": 30, "active": true}',
        '{"name": "Bob", "age": 25, "active": false}',
    ]
    data_file = os.path.join(out_dir, "flat.json")
    write_json_lines(data_file, data)

    df = spark.read.schema(spark_schema).json(data_file)
    print_dataframe(df, title="Read with Converted Schema")

    # =========================================================================
    # 2. Nested object schema
    # =========================================================================
    print_header("2. Nested Object Schema")

    nested_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer", "format": "int64"},
            "name": {"type": "string"},
            "address": {
                "type": "object",
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"},
                    "zip": {"type": "string"},
                    "country": {"type": "string"},
                },
                "required": ["street", "city"],
            },
        },
        "required": ["id", "name"],
    }

    spark_nested = from_json_schema(nested_schema)
    print_schema(spark.createDataFrame([], spark_nested), title="Nested Schema")

    nested_data = [
        '{"id": 1, "name": "Alice", "address": {"street": "123 Main St", "city": "NYC", "zip": "10001", "country": "US"}}',
        '{"id": 2, "name": "Bob", "address": {"street": "456 Oak Ave", "city": "LA", "zip": "90001"}}',
    ]
    nested_file = os.path.join(out_dir, "nested.json")
    write_json_lines(nested_file, nested_data)

    df_nested = spark.read.schema(spark_nested).json(nested_file)
    print_dataframe(df_nested, title="Nested Data")
    print_success("Nested objects → StructType with required fields marked non-nullable")

    # =========================================================================
    # 3. Arrays and format hints
    # =========================================================================
    print_header("3. Arrays and Format Hints")

    array_schema = {
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "created_at": {"type": "string", "format": "date-time"},
            "birth_date": {"type": "string", "format": "date"},
            "scores": {
                "type": "array",
                "items": {"type": "number"},
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
    }

    spark_array = from_json_schema(array_schema)
    print_schema(spark.createDataFrame([], spark_array), title="Array + Format Schema")
    print_success("date-time→TimestampType, date→DateType, array<number>→ArrayType(DoubleType)")

    # =========================================================================
    # 4. Array of objects
    # =========================================================================
    print_header("4. Array of Objects")

    array_obj_schema = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "product": {"type": "string"},
                        "quantity": {"type": "integer", "format": "int32"},
                        "price": {"type": "number", "format": "double"},
                    },
                },
            },
        },
    }

    spark_arr_obj = from_json_schema(array_obj_schema)
    print_schema(spark.createDataFrame([], spark_arr_obj), title="Array of Objects Schema")

    arr_obj_data = [
        '{"order_id": "ORD-001", "items": [{"product": "Widget", "quantity": 3, "price": 9.99}, {"product": "Gadget", "quantity": 1, "price": 29.99}]}',
    ]
    arr_obj_file = os.path.join(out_dir, "array_obj.json")
    write_json_lines(arr_obj_file, arr_obj_data)

    df_arr_obj = spark.read.schema(spark_arr_obj).json(arr_obj_file)
    print_dataframe(df_arr_obj, title="Array of Objects Data")

    # =========================================================================
    # 5. Dynamic keys (additionalProperties → MapType)
    # =========================================================================
    print_header("5. Dynamic Keys → MapType")

    map_schema_def = {
        "type": "object",
        "properties": {
            "metrics_id": {"type": "string"},
            "values": {
                "type": "object",
                "additionalProperties": {"type": "number"},
            },
        },
    }

    spark_map = from_json_schema(map_schema_def)
    print_schema(spark.createDataFrame([], spark_map), title="MapType Schema")
    print_success("additionalProperties → MapType(StringType, DoubleType)")

    # =========================================================================
    # 6. Nullable types (["string", "null"])
    # =========================================================================
    print_header("6. Nullable Types")

    nullable_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "email": {"type": ["string", "null"]},
            "age": {"type": ["integer", "null"], "format": "int32"},
        },
        "required": ["name"],
    }

    spark_nullable = from_json_schema(nullable_schema)
    print_schema(spark.createDataFrame([], spark_nullable), title="Nullable Schema")
    print_success('["string", "null"] → StringType(nullable=True)')

    # =========================================================================
    # 7. From JSON Schema file
    # =========================================================================
    print_header("7. Loading from a .json Schema File")

    import json

    schema_file = os.path.join(out_dir, "user_schema.json")
    file_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "User",
        "type": "object",
        "properties": {
            "id": {"type": "integer", "format": "int64"},
            "username": {"type": "string"},
            "email": {"type": ["string", "null"]},
            "profile": {
                "type": "object",
                "properties": {
                    "bio": {"type": "string"},
                    "website": {"type": "string"},
                },
            },
            "roles": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["id", "username"],
    }
    os.makedirs(os.path.dirname(schema_file), exist_ok=True)
    with open(schema_file, "w") as f:
        json.dump(file_schema, f, indent=2)
    logger.info("Wrote schema file to %s", schema_file)

    # Load and convert
    with open(schema_file) as f:
        loaded_schema = json.load(f)

    spark_from_file = from_json_schema(loaded_schema)
    print_schema(spark.createDataFrame([], spark_from_file), title="Schema from File")
    logger.info("DDL: %s", schema_to_ddl(spark_from_file))
    print_success("Load any JSON Schema file → from_json_schema() → spark.read.schema()")

    spark.stop()

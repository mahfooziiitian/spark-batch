"""Schema builder utilities for JSON datasource.

Provides helper functions for constructing, modifying, and validating PySpark schemas
commonly used with JSON data.
"""

from __future__ import annotations

import json as _json
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql.types import DataType


def with_corrupt_record(
    schema: StructType,
    column_name: str = "_corrupt_record",
) -> StructType:
    """Append a corrupt record column to an existing schema.

    Required for PERMISSIVE mode to capture malformed records.

    Args:
        schema: Base schema to extend.
        column_name: Name of the corrupt record column.

    Returns:
        New StructType with the corrupt record field appended.
    """
    return StructType([*schema.fields, StructField(column_name, StringType(), True)])


def with_rescued_data(
    schema: StructType,
    column_name: str = "_rescued_data",
) -> StructType:
    """Append a rescued data column to an existing schema.

    Useful for schema evolution — captures fields not matching the schema.

    Args:
        schema: Base schema to extend.
        column_name: Name of the rescued data column.

    Returns:
        New StructType with the rescued data field appended.
    """
    return StructType([*schema.fields, StructField(column_name, StringType(), True)])


def map_schema(
    key_type: DataType | None = None,
    value_type: DataType | None = None,
) -> MapType:
    """Create a MapType schema for dynamic-key JSON.

    Args:
        key_type: Type for map keys (default: StringType).
        value_type: Type for map values (default: StringType).

    Returns:
        MapType schema.

    Example:
        >>> schema = StructType([StructField("data", map_schema())])
    """
    if key_type is None:
        key_type = StringType()
    if value_type is None:
        value_type = StringType()
    return MapType(key_type, value_type)


def array_of_structs(fields: list[tuple[str, DataType]]) -> ArrayType:
    """Create an ArrayType(StructType(...)) schema.

    Args:
        fields: List of (name, type) tuples defining the struct fields.

    Returns:
        ArrayType containing a StructType.

    Example:
        >>> items_schema = array_of_structs([("id", IntegerType()), ("name", StringType())])
    """
    struct = StructType([StructField(name, dtype, True) for name, dtype in fields])
    return ArrayType(struct, True)


def array_of(element_type: DataType) -> ArrayType:
    """Create a simple ArrayType with the given element type.

    Args:
        element_type: Type of array elements.

    Returns:
        ArrayType schema.
    """
    return ArrayType(element_type, True)


def nested_struct(**fields: DataType) -> StructType:
    """Create a StructType from keyword arguments.

    Args:
        **fields: Field names mapped to their types.

    Returns:
        StructType schema.

    Example:
        >>> address_schema = nested_struct(street=StringType(), city=StringType(), zip=StringType())
    """
    return StructType([StructField(name, dtype, True) for name, dtype in fields.items()])


def schema_from_dict(d: dict[str, str]) -> StructType:
    """Create a StructType from a dict mapping field names to type names.

    Supports: string, int, long, double, boolean, timestamp, array<string>, map<string,string>.

    Args:
        d: Dict mapping field names to type name strings.

    Returns:
        StructType schema.

    Example:
        >>> schema = schema_from_dict({"name": "string", "age": "int", "scores": "array<long>"})
    """
    type_map: dict[str, DataType] = {
        "string": StringType(),
        "str": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "long": LongType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "float": DoubleType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "timestamp": TimestampType(),
    }

    fields = []
    for name, type_str in d.items():
        type_str_lower = type_str.lower().strip()
        if type_str_lower in type_map:
            fields.append(StructField(name, type_map[type_str_lower], True))
        elif type_str_lower.startswith("array<") and type_str_lower.endswith(">"):
            inner = type_str_lower[6:-1].strip()
            inner_type = type_map.get(inner, StringType())
            fields.append(StructField(name, ArrayType(inner_type, True), True))
        elif type_str_lower.startswith("map<") and type_str_lower.endswith(">"):
            parts = type_str_lower[4:-1].split(",", 1)
            k_type = type_map.get(parts[0].strip(), StringType())
            v_type = type_map.get(parts[1].strip(), StringType()) if len(parts) > 1 else StringType()
            fields.append(StructField(name, MapType(k_type, v_type), True))
        else:
            fields.append(StructField(name, StringType(), True))

    return StructType(fields)


def schema_to_json(schema: StructType) -> str:
    """Serialize a StructType schema to a JSON string.

    Args:
        schema: Schema to serialize.

    Returns:
        JSON string representation of the schema.
    """
    return schema.json()


def schema_from_json(json_str: str) -> StructType:
    """Deserialize a StructType schema from a JSON string.

    Args:
        json_str: JSON schema string (as produced by schema.json()).

    Returns:
        StructType schema.
    """
    return StructType.fromJson(_json.loads(json_str))


def schema_to_ddl(schema: StructType) -> str:
    """Convert a StructType to DDL string notation.

    Args:
        schema: Schema to convert.

    Returns:
        DDL string (e.g., "name STRING, age INT").
    """
    return schema.simpleString()[7:-1]  # Strip "struct<...>"


def merge_schemas(base: StructType, *others: StructType) -> StructType:
    """Merge multiple schemas, combining fields (later schemas override on conflict).

    Args:
        base: Base schema.
        *others: Additional schemas to merge into base.

    Returns:
        Merged StructType with all fields.
    """
    field_map: dict[str, StructField] = {f.name: f for f in base.fields}
    for schema in others:
        for f in schema.fields:
            field_map[f.name] = f
    return StructType(list(field_map.values()))


def select_fields(schema: StructType, *field_names: str) -> StructType:
    """Create a subset schema with only the specified fields.

    Args:
        schema: Source schema.
        *field_names: Names of fields to keep.

    Returns:
        New StructType with only the specified fields.

    Raises:
        KeyError: If a field name is not found in the schema.
    """
    field_map = {f.name: f for f in schema.fields}
    selected = []
    for name in field_names:
        if name not in field_map:
            raise KeyError(f"Field '{name}' not found in schema. Available: {list(field_map.keys())}")
        selected.append(field_map[name])
    return StructType(selected)


def drop_fields(schema: StructType, *field_names: str) -> StructType:
    """Create a schema excluding the specified fields.

    Args:
        schema: Source schema.
        *field_names: Names of fields to remove.

    Returns:
        New StructType without the specified fields.
    """
    exclude = set(field_names)
    return StructType([f for f in schema.fields if f.name not in exclude])


# ---------------------------------------------------------------------------
# JSON Schema → PySpark StructType conversion
# ---------------------------------------------------------------------------

# Mapping from JSON Schema type+format to PySpark DataType
_JSON_SCHEMA_TYPE_MAP: dict[str, DataType] = {
    "string": StringType(),
    "string:date": DateType(),
    "string:date-time": TimestampType(),
    "string:byte": BinaryType(),
    "integer": LongType(),
    "integer:int8": ByteType(),
    "integer:int16": ShortType(),
    "integer:int32": IntegerType(),
    "integer:int64": LongType(),
    "number": DoubleType(),
    "number:float": FloatType(),
    "number:double": DoubleType(),
    "boolean": BooleanType(),
}


def _resolve_json_schema_type(prop: dict) -> DataType:
    """Resolve a single JSON Schema property to a PySpark DataType.

    Handles: primitives, format hints, $ref (inline only), enum,
    objects (→ StructType), arrays (→ ArrayType), oneOf/anyOf,
    and additionalProperties (→ MapType).
    """
    # Handle allOf / anyOf / oneOf — pick the first schema with a type
    for combiner in ("allOf", "anyOf", "oneOf"):
        if combiner in prop:
            for sub in prop[combiner]:
                if "type" in sub or "$ref" in sub or "properties" in sub:
                    return _resolve_json_schema_type(sub)
            return StringType()

    # const / enum without type → StringType
    if ("const" in prop or "enum" in prop) and "type" not in prop:
        return StringType()

    json_type = prop.get("type", "string")
    json_format = prop.get("format", "")

    # Handle type arrays like ["string", "null"] → pick the non-null type
    if isinstance(json_type, list):
        non_null = [t for t in json_type if t != "null"]
        json_type = non_null[0] if non_null else "string"

    # Object → StructType or MapType
    if json_type == "object":
        properties = prop.get("properties")
        additional = prop.get("additionalProperties")

        if properties:
            return _json_schema_to_struct(prop)

        # additionalProperties → MapType
        if isinstance(additional, dict):
            value_type = _resolve_json_schema_type(additional)
            return MapType(StringType(), value_type)
        if additional is True or (not properties and not additional):
            return MapType(StringType(), StringType())

        return MapType(StringType(), StringType())

    # Array → ArrayType
    if json_type == "array":
        items = prop.get("items", {})
        if isinstance(items, list):
            # Tuple validation — take first item's type
            element_type = _resolve_json_schema_type(items[0]) if items else StringType()
        elif isinstance(items, dict):
            element_type = _resolve_json_schema_type(items)
        else:
            element_type = StringType()
        return ArrayType(element_type, True)

    # number with multipleOf → DecimalType hint
    if json_type == "number" and "multipleOf" in prop:
        mo = prop["multipleOf"]
        if isinstance(mo, float):
            decimal_str = str(mo)
            if "." in decimal_str:
                scale = len(decimal_str.split(".")[1])
                return DecimalType(38, scale)

    # Primitive lookup: "type:format" first, then "type"
    lookup_key = f"{json_type}:{json_format}" if json_format else json_type
    return _JSON_SCHEMA_TYPE_MAP.get(lookup_key, _JSON_SCHEMA_TYPE_MAP.get(json_type, StringType()))


def _json_schema_to_struct(schema: dict) -> StructType:
    """Convert a JSON Schema object definition to a PySpark StructType."""
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))

    fields = []
    for name, prop in properties.items():
        spark_type = _resolve_json_schema_type(prop)
        nullable = name not in required
        # Also nullable if type includes "null"
        prop_type = prop.get("type")
        if isinstance(prop_type, list) and "null" in prop_type:
            nullable = True
        fields.append(StructField(name, spark_type, nullable))

    return StructType(fields)


def from_json_schema(json_schema: dict | str) -> StructType:
    """Convert a standard JSON Schema to a PySpark StructType.

    Supports JSON Schema drafts 04, 07, and 2020-12. Handles nested objects,
    arrays, $ref-free schemas, format hints (date, date-time, int32, etc.),
    additionalProperties (→ MapType), and oneOf/anyOf/allOf combiners.

    Args:
        json_schema: JSON Schema as a dict or JSON string.

    Returns:
        PySpark StructType matching the JSON Schema.

    Raises:
        ValueError: If the schema is not an object type or is invalid.

    Type mapping:
        | JSON Schema            | PySpark          |
        |------------------------|------------------|
        | string                 | StringType       |
        | string + date          | DateType         |
        | string + date-time     | TimestampType    |
        | string + byte          | BinaryType       |
        | integer                | LongType         |
        | integer + int32        | IntegerType      |
        | integer + int64        | LongType         |
        | number                 | DoubleType       |
        | number + float         | FloatType        |
        | number + multipleOf    | DecimalType      |
        | boolean                | BooleanType      |
        | object (properties)    | StructType       |
        | object (additionalP.)  | MapType          |
        | array                  | ArrayType        |

    Example:
        >>> schema = from_json_schema({
        ...     "type": "object",
        ...     "properties": {
        ...         "name": {"type": "string"},
        ...         "age": {"type": "integer", "format": "int32"},
        ...         "scores": {"type": "array", "items": {"type": "number"}},
        ...     },
        ...     "required": ["name"],
        ... })
    """
    if isinstance(json_schema, str):
        json_schema = _json.loads(json_schema)

    if not isinstance(json_schema, dict):
        msg = f"Expected a JSON Schema dict or string, got {type(json_schema).__name__}"
        raise ValueError(msg)

    schema_type = json_schema.get("type", "object")
    if isinstance(schema_type, list):
        non_null = [t for t in schema_type if t != "null"]
        schema_type = non_null[0] if non_null else "object"

    if schema_type != "object":
        msg = f"Root JSON Schema must be type 'object', got '{schema_type}'"
        raise ValueError(msg)

    return _json_schema_to_struct(json_schema)


# =============================================================================
# Spark Schema File Validation
# =============================================================================

_VALID_SPARK_TYPES = frozenset(
    {
        "binary",
        "boolean",
        "byte",
        "date",
        "decimal",
        "double",
        "float",
        "integer",
        "long",
        "null",
        "short",
        "string",
        "timestamp",
        "timestamp_ntz",
        "void",
    }
)


class SchemaValidationResult:
    """Result of a Spark schema JSON file validation."""

    def __init__(self) -> None:
        self.valid: bool = True
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.schema: StructType | None = None

    def add_error(self, msg: str) -> None:
        """Record a validation error."""
        self.valid = False
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        """Record a non-fatal warning."""
        self.warnings.append(msg)

    def __repr__(self) -> str:
        status = "VALID" if self.valid else "INVALID"
        parts = [f"SchemaValidationResult({status})"]
        if self.errors:
            parts.append(f"  errors={self.errors}")
        if self.warnings:
            parts.append(f"  warnings={self.warnings}")
        return "\n".join(parts)

    def __bool__(self) -> bool:
        return self.valid


def validate_schema_file(
    file_path: str,
    *,
    expected_schema: StructType | None = None,
    check_duplicates: bool = True,
    check_nullability: bool = False,
) -> SchemaValidationResult:
    """Validate a Spark schema JSON file (not a data file).

    Performs enterprise-grade validation:
        - Valid JSON syntax
        - Root type is "struct"
        - "fields" exists and is an array
        - No duplicate column names (optional)
        - Valid Spark datatypes (recursively)
        - Nested schema validity
        - Nullability rules (optional)
        - Schema drift against expected baseline (optional)

    Args:
        file_path: Path to a JSON file containing a Spark schema
            (output of ``schema.jsonValue()`` or ``schema.json()``).
        expected_schema: If provided, compare against this baseline schema
            and report missing/extra fields and type mismatches.
        check_duplicates: Check for duplicate field names (default: True).
        check_nullability: Warn about non-nullable fields (default: False).

    Returns:
        SchemaValidationResult with valid flag, errors, warnings, and parsed schema.

    Example:
        >>> result = validate_schema_file("schema.json")
        >>> print(result.valid)
        True
        >>> print(result.schema)
        StructType(...)
    """
    result = SchemaValidationResult()

    # Step 1: Read and parse JSON
    try:
        with open(file_path) as f:
            schema_dict = _json.load(f)
    except FileNotFoundError:
        result.add_error(f"File not found: {file_path}")
        return result
    except _json.JSONDecodeError as e:
        result.add_error(f"Invalid JSON syntax: {e}")
        return result

    # Step 2: Validate root structure
    if not isinstance(schema_dict, dict):
        result.add_error(f"Expected JSON object at root, got {type(schema_dict).__name__}")
        return result

    root_type = schema_dict.get("type")
    if root_type != "struct":
        result.add_error(f"Root 'type' must be 'struct', got '{root_type}'")
        return result

    fields = schema_dict.get("fields")
    if fields is None:
        result.add_error("Missing 'fields' array in schema")
        return result
    if not isinstance(fields, list):
        result.add_error(f"'fields' must be an array, got {type(fields).__name__}")
        return result

    # Step 3: Validate fields recursively
    _validate_fields(fields, result, path="$", check_duplicates=check_duplicates)

    # Step 4: Nullability check
    if check_nullability:
        for field in fields:
            if isinstance(field, dict) and field.get("nullable") is False:
                result.add_warning(f"Field '{field.get('name')}' is non-nullable — may cause read failures")

    # Step 5: Parse with Spark's StructType.fromJson
    if result.valid:
        try:
            result.schema = StructType.fromJson(schema_dict)
        except Exception as e:
            result.add_error(f"Spark StructType.fromJson failed: {e}")

    # Step 6: Schema drift comparison
    if expected_schema is not None and result.schema is not None:
        _compare_schemas(expected_schema, result.schema, result)

    return result


def _validate_fields(
    fields: list,
    result: SchemaValidationResult,
    path: str,
    check_duplicates: bool,
) -> None:
    """Recursively validate schema fields."""
    seen_names: set[str] = set()

    for i, field in enumerate(fields):
        field_path = f"{path}.fields[{i}]"

        if not isinstance(field, dict):
            result.add_error(f"{field_path}: Expected object, got {type(field).__name__}")
            continue

        # Required attributes
        name = field.get("name")
        if name is None:
            result.add_error(f"{field_path}: Missing 'name' attribute")
        elif not isinstance(name, str) or not name.strip():
            result.add_error(f"{field_path}: 'name' must be a non-empty string")
        else:
            if check_duplicates:
                if name in seen_names:
                    result.add_error(f"{field_path}: Duplicate field name '{name}'")
                seen_names.add(name)

        ftype = field.get("type")
        if ftype is None:
            result.add_error(f"{field_path}: Missing 'type' attribute")
        else:
            _validate_type(ftype, result, f"{field_path}.type")

        # nullable must be boolean if present
        nullable = field.get("nullable")
        if nullable is not None and not isinstance(nullable, bool):
            result.add_error(f"{field_path}: 'nullable' must be boolean, got {type(nullable).__name__}")

        # metadata must be object if present
        metadata = field.get("metadata")
        if metadata is not None and not isinstance(metadata, dict):
            result.add_error(f"{field_path}: 'metadata' must be an object, got {type(metadata).__name__}")


def _validate_type(
    ftype: object,
    result: SchemaValidationResult,
    path: str,
) -> None:
    """Validate a Spark type definition (string or complex dict)."""
    if isinstance(ftype, str):
        # Simple type or decimal(p,s)
        normalized = ftype.lower().strip()
        if normalized.startswith("decimal("):
            return  # decimal(precision, scale) is valid
        if normalized not in _VALID_SPARK_TYPES:
            result.add_error(f"{path}: Unknown simple type '{ftype}'")
        return

    if isinstance(ftype, dict):
        type_name = ftype.get("type")
        if type_name == "struct":
            inner_fields = ftype.get("fields")
            if inner_fields is None:
                result.add_error(f"{path}: Struct type missing 'fields'")
            elif isinstance(inner_fields, list):
                _validate_fields(inner_fields, result, path, check_duplicates=True)
            else:
                result.add_error(f"{path}: Struct 'fields' must be an array")

        elif type_name == "array":
            element = ftype.get("elementType")
            if element is None:
                result.add_error(f"{path}: Array type missing 'elementType'")
            else:
                _validate_type(element, result, f"{path}.elementType")

        elif type_name == "map":
            key_type = ftype.get("keyType")
            value_type = ftype.get("valueType")
            if key_type is None:
                result.add_error(f"{path}: Map type missing 'keyType'")
            else:
                _validate_type(key_type, result, f"{path}.keyType")
            if value_type is None:
                result.add_error(f"{path}: Map type missing 'valueType'")
            else:
                _validate_type(value_type, result, f"{path}.valueType")

        elif type_name == "udt":
            # User-defined type — trust it
            pass

        elif type_name is None:
            result.add_error(f"{path}: Complex type missing 'type' attribute")

        else:
            result.add_error(f"{path}: Unknown complex type '{type_name}'")

        return

    result.add_error(f"{path}: Type must be string or object, got {type(ftype).__name__}")


def _compare_schemas(
    expected: StructType,
    actual: StructType,
    result: SchemaValidationResult,
) -> None:
    """Compare actual schema against expected baseline and report drift."""
    expected_map = {f.name: f for f in expected.fields}
    actual_map = {f.name: f for f in actual.fields}

    missing = set(expected_map.keys()) - set(actual_map.keys())
    extra = set(actual_map.keys()) - set(expected_map.keys())

    if missing:
        result.add_warning(f"Missing fields vs expected: {sorted(missing)}")
    if extra:
        result.add_warning(f"Extra fields vs expected: {sorted(extra)}")

    # Type mismatch check
    for name in sorted(set(expected_map.keys()) & set(actual_map.keys())):
        exp_type = str(expected_map[name].dataType)
        act_type = str(actual_map[name].dataType)
        if exp_type != act_type:
            result.add_warning(f"Type mismatch for '{name}': expected {exp_type}, got {act_type}")

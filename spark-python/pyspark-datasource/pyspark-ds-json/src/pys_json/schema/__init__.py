"""Schema builder utilities — constructors, modifiers, and serialization."""

from pys_json.schema._schema import (
    SchemaValidationResult,
    array_of,
    array_of_structs,
    drop_fields,
    from_json_schema,
    map_schema,
    merge_schemas,
    nested_struct,
    schema_from_dict,
    schema_from_json,
    schema_to_ddl,
    schema_to_json,
    select_fields,
    validate_schema_file,
    with_corrupt_record,
    with_rescued_data,
)

__all__ = [
    "SchemaValidationResult",
    "array_of",
    "array_of_structs",
    "drop_fields",
    "from_json_schema",
    "map_schema",
    "merge_schemas",
    "nested_struct",
    "schema_from_dict",
    "schema_from_json",
    "schema_to_ddl",
    "schema_to_json",
    "select_fields",
    "validate_schema_file",
    "with_corrupt_record",
    "with_rescued_data",
]

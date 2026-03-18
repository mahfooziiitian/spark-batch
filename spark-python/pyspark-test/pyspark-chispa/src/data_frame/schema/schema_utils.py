"""Schema inspection and comparison utilities."""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType


def get_column_names_by_type(df: DataFrame, data_type: str) -> list[str]:
    """Return column names matching a given data type.

    Args:
        df: Input DataFrame.
        data_type: Spark type name to filter by (e.g. ``"string"``, ``"long"``).

    Returns:
        List of column names whose type matches ``data_type``.
    """
    return [field.name for field in df.schema.fields if field.dataType.typeName() == data_type]


def schema_to_dict(schema: StructType) -> dict[str, str]:
    """Convert a StructType schema to a {name: type} dictionary.

    Args:
        schema: PySpark StructType.

    Returns:
        Dictionary mapping column names to their type names.
    """
    return {field.name: field.dataType.typeName() for field in schema.fields}


def add_nullable_fields(schema: StructType) -> StructType:
    """Return a copy of the schema with all fields set to nullable.

    Args:
        schema: Input StructType.

    Returns:
        New StructType with every field's nullable set to True.
    """
    return StructType([StructField(f.name, f.dataType, nullable=True) for f in schema.fields])

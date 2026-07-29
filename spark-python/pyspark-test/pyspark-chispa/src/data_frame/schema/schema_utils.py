from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType


def get_column_names_by_type(df: DataFrame, type_name: str) -> list[str]:
    """Return column names matching a given Spark data type.

    Args:
        df: Input DataFrame.
        type_name: Spark type name (e.g., ``"string"``, ``"long"``, ``"double"``).

    Returns:
        List of column names with the matching type.
    """
    return [field.name for field in df.schema.fields if field.dataType.simpleString() == type_name]


def schema_to_dict(schema: StructType) -> dict[str, str]:
    """Convert a ``StructType`` schema to a ``{name: type}`` dictionary.

    Args:
        schema: PySpark schema.

    Returns:
        Dictionary mapping column names to their type names.
    """
    return {field.name: field.dataType.simpleString() for field in schema.fields}


def add_nullable_fields(schema: StructType) -> StructType:
    """Return a copy of the schema with all fields set to nullable.

    Args:
        schema: Input schema.

    Returns:
        New schema with all fields nullable.
    """
    return StructType([StructField(f.name, f.dataType, nullable=True) for f in schema.fields])

"""JSON parsing utilities — helpers for from_json, schema inference, and transformations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from pys_json._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import ArrayType, StructType

logger = get_logger("parsing")


def parse_json_column(
    col: str | Column,
    schema: StructType | ArrayType | str,
    options: dict[str, str] | None = None,
) -> Column:
    """Parse a JSON string column into a structured column.

    Args:
        col: Column name or Column expression containing JSON strings.
        schema: StructType, ArrayType, MapType, or DDL string for target schema.
        options: Optional parsing options (e.g., mode, dateFormat).

    Returns:
        Column with parsed structure.

    Example:
        >>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        >>> schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
        >>> df.withColumn("parsed", parse_json_column("json_col", schema))
    """
    if options:
        logger.debug("Parsing JSON column with options: %s", options)
        return F.from_json(col, schema, options)
    return F.from_json(col, schema)


def parse_json_to_map(
    col: str | Column,
    key_type=None,
    value_type=None,
) -> Column:
    """Parse a JSON string column into a MapType.

    Useful when JSON keys are dynamic/unknown.

    Args:
        col: Column containing JSON strings.
        key_type: Type for map keys (default: StringType).
        value_type: Type for map values (default: StringType).

    Returns:
        Column with MapType.
    """
    if key_type is None:
        key_type = StringType()
    if value_type is None:
        value_type = StringType()
    return F.from_json(col, MapType(key_type, value_type))  # type: ignore[arg-type]


def to_json_string(col: str | Column, options: dict[str, str] | None = None) -> Column:
    """Serialize a struct/map/array column to a JSON string.

    Args:
        col: Struct, map, or array column.
        options: Optional formatting options (e.g., dateFormat).

    Returns:
        Column with JSON string representation.
    """
    if options:
        return F.to_json(col, options)
    return F.to_json(col)


def struct_to_json(*cols: str | Column) -> Column:
    """Create a struct from columns and serialize to JSON string.

    Args:
        *cols: Column names or Column expressions to combine into a struct.

    Returns:
        Column with JSON string.

    Example:
        >>> df.withColumn("json", struct_to_json("name", "age", "city"))
    """
    return F.to_json(F.struct(*cols))


def extract_json_keys(df: DataFrame, col: str, *keys: str) -> DataFrame:
    """Extract multiple top-level keys from a JSON string column using json_tuple.

    More efficient than multiple get_json_object calls — parses JSON once.

    Args:
        df: Source DataFrame.
        col: Column name containing JSON strings.
        *keys: Key names to extract.

    Returns:
        DataFrame with original columns plus extracted key columns.

    Example:
        >>> extract_json_keys(df, "raw_json", "name", "age", "city")
    """
    return df.select("*", F.json_tuple(F.col(col), *keys).alias(*keys))


def get_json_field(col: str | Column, path: str) -> Column:
    """Extract a single value from JSON using a JSONPath expression.

    Args:
        col: Column containing JSON strings.
        path: JSONPath expression (e.g., "$.address.city", "$.items[0].name").

    Returns:
        Column with extracted string value.

    Example:
        >>> df.withColumn("city", get_json_field("data", "$.address.city"))
    """
    return F.get_json_object(col, path)


def get_json_fields(col: str | Column, **paths: str) -> dict[str, Column]:
    """Extract multiple JSON fields by JSONPath, returning named columns.

    Args:
        col: Column containing JSON strings.
        **paths: Keyword args mapping output name to JSONPath expression.

    Returns:
        Dict of output_name -> Column for use in select().

    Example:
        >>> fields = get_json_fields("data", city="$.address.city", zip="$.address.zip")
        >>> df.select("id", *[c.alias(n) for n, c in fields.items()])
    """
    return {name: F.get_json_object(col, path) for name, path in paths.items()}


def json_array_length(col: str | Column) -> Column:
    """Get the number of elements in a JSON array string.

    Args:
        col: Column containing JSON array strings.

    Returns:
        Column with integer array length.
    """
    return F.json_array_length(col)


def json_object_keys(col: str | Column) -> Column:
    """Get the keys of a JSON object as an array of strings.

    Args:
        col: Column containing JSON object strings.

    Returns:
        Column with array of key strings.
    """
    return F.json_object_keys(col)


def infer_schema_from_sample(spark: SparkSession, sample_json: str) -> str:
    """Infer a DDL schema string from a sample JSON value.

    Useful for generating schemas during development.

    Args:
        spark: Active SparkSession.
        sample_json: A representative JSON string.

    Returns:
        DDL schema string (e.g., "STRUCT<name: STRING, age: BIGINT>").

    Example:
        >>> schema_ddl = infer_schema_from_sample(spark, '{"name": "Alice", "age": 30}')
        >>> df = spark.read.schema(schema_ddl).json(path)
    """
    result = spark.range(1).select(F.schema_of_json(F.lit(sample_json))).collect()
    schema_ddl: str = result[0][0]
    logger.debug("Inferred schema from sample: %s", schema_ddl)
    return schema_ddl


def explode_json_array(df: DataFrame, col: str, alias: str = "element") -> DataFrame:
    """Explode a JSON array column into individual rows.

    Parses the JSON array string and explodes it into separate rows.

    Args:
        df: Source DataFrame.
        col: Column name containing JSON array strings.
        alias: Name for the exploded column.

    Returns:
        DataFrame with one row per array element.
    """
    from pyspark.sql.types import ArrayType, StringType

    array_schema = ArrayType(StringType())
    return df.withColumn(alias, F.explode(F.from_json(F.col(col), array_schema)))


def flatten_json(df: DataFrame, col: str, prefix: str = "") -> DataFrame:
    """Flatten a struct column into top-level columns.

    Args:
        df: Source DataFrame.
        col: Name of the struct column to flatten.
        prefix: Optional prefix for flattened column names.

    Returns:
        DataFrame with struct fields promoted to top-level columns.

    Example:
        >>> df_flat = flatten_json(df, "address", prefix="addr_")
    """
    from pyspark.sql.types import StructType

    struct_field = df.schema[col]
    if not isinstance(struct_field.dataType, StructType):
        msg = f"Column '{col}' is not a StructType, got {struct_field.dataType}"
        logger.error(msg)
        raise TypeError(msg)

    logger.debug("Flattening struct column '%s' with %d fields", col, len(struct_field.dataType.fields))

    select_cols = [F.col(c) for c in df.columns if c != col]
    for field in struct_field.dataType.fields:
        select_cols.append(F.col(f"{col}.{field.name}").alias(f"{prefix}{field.name}"))

    return df.select(*select_cols)

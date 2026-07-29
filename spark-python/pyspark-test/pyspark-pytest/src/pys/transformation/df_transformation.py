"""DataFrame text transformation utilities."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def remove_extra_spaces(df: DataFrame, column_name: str) -> DataFrame:
    """Remove consecutive whitespace from a column, replacing with single space.

    Args:
        df: Input DataFrame.
        column_name: Name of the column to clean.

    Returns:
        DataFrame with extra spaces removed from the specified column.
    """
    return df.withColumn(column_name, F.regexp_replace(F.col(column_name), "\\s+", " "))


def trim_all_columns(df: DataFrame) -> DataFrame:
    """Trim leading and trailing whitespace from all string columns.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with all string columns trimmed.
    """
    from pyspark.sql.types import StringType

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def to_lower_case(df: DataFrame, column_name: str) -> DataFrame:
    """Convert a column's values to lower case.

    Args:
        df: Input DataFrame.
        column_name: Name of the column to convert.

    Returns:
        DataFrame with the specified column in lower case.
    """
    return df.withColumn(column_name, F.lower(F.col(column_name)))


def to_upper_case(df: DataFrame, column_name: str) -> DataFrame:
    """Convert a column's values to upper case.

    Args:
        df: Input DataFrame.
        column_name: Name of the column to convert.

    Returns:
        DataFrame with the specified column in upper case.
    """
    return df.withColumn(column_name, F.upper(F.col(column_name)))


def to_title_case(df: DataFrame, column_name: str) -> DataFrame:
    """Convert a column's values to title case (capitalize first letter of each word).

    Args:
        df: Input DataFrame.
        column_name: Name of the column to convert.

    Returns:
        DataFrame with the specified column in title case.
    """
    return df.withColumn(column_name, F.initcap(F.col(column_name)))


def replace_nulls_with_default(df: DataFrame, column_name: str, default: str) -> DataFrame:
    """Replace null values in a string column with a default value.

    Args:
        df: Input DataFrame.
        column_name: Name of the column to fill.
        default: Default value to use for nulls.

    Returns:
        DataFrame with nulls replaced in the specified column.
    """
    return df.withColumn(column_name, F.coalesce(F.col(column_name), F.lit(default)))

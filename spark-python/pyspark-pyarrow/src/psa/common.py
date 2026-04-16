from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def remove_extra_spaces(df: DataFrame, column_name: str) -> DataFrame:
    """Collapse consecutive whitespace into a single space."""
    return df.withColumn(column_name, F.regexp_replace(F.col(column_name), "\\s+", " "))


def filter_senior_citizen(df: DataFrame, column_name: str) -> DataFrame:
    """Keep rows where the given column value is >= 60."""
    return df.filter(F.col(column_name) >= 60)

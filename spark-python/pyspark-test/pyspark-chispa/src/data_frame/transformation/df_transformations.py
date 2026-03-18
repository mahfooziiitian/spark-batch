"""DataFrame-level transformation utilities.

Each function receives a DataFrame and returns a new DataFrame.
"""

from typing import Callable, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def modify_column_names(df: DataFrame, fun: Callable[[str], str]) -> DataFrame:
    """Rename all columns in a DataFrame using a transformation function.

    Args:
        df: Input DataFrame.
        fun: Function that maps old column name to new column name.

    Returns:
        DataFrame with renamed columns.
    """
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, fun(col_name))
    return df


def with_row_number(df: DataFrame, order_col: str, alias: str = "row_number") -> DataFrame:
    """Add a sequential row number column ordered by the given column.

    Args:
        df: Input DataFrame.
        order_col: Column name to order by.
        alias: Name for the new row number column.

    Returns:
        DataFrame with an additional row number column.
    """
    w = Window.orderBy(order_col)
    return df.withColumn(alias, F.row_number().over(w))


def with_running_total(
    df: DataFrame,
    value_col: str,
    order_col: str,
    partition_col: Optional[str] = None,
    alias: str = "running_total",
) -> DataFrame:
    """Add a running total column over an ordered window.

    Args:
        df: Input DataFrame.
        value_col: Column to sum.
        order_col: Column to order the window by.
        partition_col: Optional column to partition the window by.
        alias: Name for the new running total column.

    Returns:
        DataFrame with an additional running total column.
    """
    w = Window.orderBy(order_col).rowsBetween(Window.unboundedPreceding, 0)
    if partition_col:
        w = Window.partitionBy(partition_col).orderBy(order_col).rowsBetween(Window.unboundedPreceding, 0)
    return df.withColumn(alias, F.sum(F.col(value_col)).over(w))


def deduplicate(df: DataFrame, subset: list[str], order_col: str, keep: str = "first") -> DataFrame:
    """Remove duplicate rows keeping the first or last occurrence.

    Args:
        df: Input DataFrame.
        subset: Columns to check for duplicates.
        order_col: Column to determine ordering within each group.
        keep: ``"first"`` to keep the earliest row, ``"last"`` to keep the latest.

    Returns:
        DataFrame with duplicates removed.

    Raises:
        ValueError: If ``keep`` is not ``"first"`` or ``"last"``.
    """
    if keep not in ("first", "last"):
        raise ValueError(f"keep must be 'first' or 'last', got '{keep}'")
    order_expr = F.col(order_col).asc() if keep == "first" else F.col(order_col).desc()
    w = Window.partitionBy(*subset).orderBy(order_expr)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def filter_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
    """Remove rows where any of the specified columns is null.

    Args:
        df: Input DataFrame.
        columns: Column names to check for nulls.

    Returns:
        DataFrame with null-containing rows removed.
    """
    condition: Column = F.lit(True)
    for col_name in columns:
        condition = condition & F.col(col_name).isNotNull()
    return df.filter(condition)


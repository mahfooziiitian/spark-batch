from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def modify_column_names(df: DataFrame, fun: Callable[[str], str]) -> DataFrame:
    """Rename all columns using a transformation function.

    Args:
        df: Input DataFrame.
        fun: A ``str → str`` function applied to each column name.

    Returns:
        DataFrame with renamed columns.
    """
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, fun(col_name))
    return df


def with_row_number(df: DataFrame, order_col: str, alias: str = "row_number") -> DataFrame:
    """Add a sequential row number column ordered by the specified column.

    Args:
        df: Input DataFrame.
        order_col: Column name to order by.
        alias: Name for the new row number column.

    Returns:
        DataFrame with an added row number column.
    """
    window = Window.orderBy(order_col)
    return df.withColumn(alias, F.row_number().over(window))


def with_running_total(
    df: DataFrame,
    value_col: str,
    order_col: str,
    partition_col: str | None = None,
    alias: str = "running_total",
) -> DataFrame:
    """Add a cumulative sum column over an ordered window.

    Args:
        df: Input DataFrame.
        value_col: Column to sum.
        order_col: Column to order by.
        partition_col: Optional column to partition by.
        alias: Name for the new running total column.

    Returns:
        DataFrame with an added running total column.
    """
    if partition_col:
        window = (
            Window.partitionBy(partition_col)
            .orderBy(order_col)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    else:
        window = Window.orderBy(order_col).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return df.withColumn(alias, F.sum(F.col(value_col)).over(window))


def deduplicate(
    df: DataFrame,
    subset: list[str],
    order_col: str,
    keep: str = "first",
) -> DataFrame:
    """Remove duplicate rows, keeping the first or last occurrence.

    Args:
        df: Input DataFrame.
        subset: Columns to group by for deduplication.
        order_col: Column to order by to determine first/last.
        keep: ``"first"`` to keep the earliest, ``"last"`` to keep the latest.

    Returns:
        Deduplicated DataFrame.

    Raises:
        ValueError: If ``keep`` is not ``"first"`` or ``"last"``.
    """
    if keep not in ("first", "last"):
        raise ValueError(f"keep must be 'first' or 'last', got '{keep}'")

    ascending = keep == "first"
    order = F.col(order_col).asc() if ascending else F.col(order_col).desc()
    window = Window.partitionBy(*subset).orderBy(order)
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def filter_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
    """Remove rows where any of the specified columns contain null values.

    Args:
        df: Input DataFrame.
        columns: Column names to check for nulls.

    Returns:
        DataFrame with null-containing rows removed.
    """
    condition = F.lit(True)
    for col_name in columns:
        condition = condition & F.col(col_name).isNotNull()
    return df.filter(condition)

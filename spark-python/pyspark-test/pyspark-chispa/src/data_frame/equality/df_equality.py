"""DataFrame sorting and comparison utilities."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def sort_columns(df: DataFrame, sort_order: str) -> DataFrame:
    """Reorder DataFrame columns alphabetically.

    Args:
        df: Input DataFrame.
        sort_order: ``"asc"`` for ascending or ``"desc"`` for descending.

    Returns:
        DataFrame with columns sorted in the specified order.

    Raises:
        ValueError: If ``sort_order`` is not ``"asc"`` or ``"desc"``.
    """
    if sort_order == "asc":
        sorted_col_names = sorted(df.columns)
    elif sort_order == "desc":
        sorted_col_names = sorted(df.columns, reverse=True)
    else:
        raise ValueError(
            f"['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'"
        )
    return df.select(*sorted_col_names)


def columns_match(df1: DataFrame, df2: DataFrame) -> bool:
    """Check whether two DataFrames have identical column names in the same order.

    Args:
        df1: First DataFrame.
        df2: Second DataFrame.

    Returns:
        True if columns match exactly, False otherwise.
    """
    return df1.columns == df2.columns


def row_diff(left: DataFrame, right: DataFrame) -> DataFrame:
    """Return rows present in ``left`` but not in ``right``.

    Args:
        left: Source DataFrame.
        right: DataFrame to subtract.

    Returns:
        DataFrame containing rows only in ``left``.
    """
    return left.subtract(right)


def union_dedup(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """Union two DataFrames and remove duplicates.

    Args:
        df1: First DataFrame.
        df2: Second DataFrame.

    Returns:
        Deduplicated union of both DataFrames.
    """
    return df1.unionByName(df2).distinct()

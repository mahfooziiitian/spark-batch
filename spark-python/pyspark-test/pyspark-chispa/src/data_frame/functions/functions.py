"""Column-level arithmetic and logic functions.

Each function operates on PySpark Columns and returns a Column,
suitable for use with ``df.withColumn()``.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def divide_by_three(col: Column) -> Column:
    """Divide a numeric column by three.

    Args:
        col: Input numeric column.

    Returns:
        Column with each value divided by three.
    """
    return col / 3


def null_safe_divide(numerator: Column, denominator: Column) -> Column:
    """Divide two columns, returning null instead of error on zero denominator.

    Args:
        numerator: Dividend column.
        denominator: Divisor column.

    Returns:
        Column with division result, or null when denominator is zero or null.
    """
    return F.when(
        (denominator != 0) & denominator.isNotNull(),
        numerator / denominator,
    )


def percentage(part: Column, total: Column, decimals: int = 2) -> Column:
    """Calculate percentage of part relative to total.

    Args:
        part: Numerator column.
        total: Denominator column.
        decimals: Number of decimal places to round to.

    Returns:
        Column with percentage value, or null when total is zero or null.
    """
    return F.round(null_safe_divide(part, total) * 100, decimals)


def clamp(col: Column, lower: float, upper: float) -> Column:
    """Clamp column values to a range.

    Args:
        col: Input numeric column.
        lower: Minimum allowed value.
        upper: Maximum allowed value.

    Returns:
        Column with values clamped between lower and upper.

    Raises:
        ValueError: If lower > upper.
    """
    if lower > upper:
        raise ValueError(f"lower ({lower}) must be <= upper ({upper})")
    return F.greatest(F.lit(lower), F.least(F.lit(upper), col))


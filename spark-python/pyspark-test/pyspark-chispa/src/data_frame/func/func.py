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
    """Divide two columns, returning ``null`` when the denominator is zero or null.

    Args:
        numerator: Dividend column.
        denominator: Divisor column.

    Returns:
        Column with the division result, or ``null`` for zero/null denominators.
    """
    return F.when(
        denominator.isNotNull() & (denominator != 0),
        numerator / denominator,
    )


def percentage(part: Column, total: Column, decimals: int = 2) -> Column:
    """Calculate the percentage of ``part`` relative to ``total``.

    Args:
        part: Numerator column.
        total: Denominator column.
        decimals: Number of decimal places to round to.

    Returns:
        Column with the percentage value, or ``null`` if ``total`` is zero or null.
    """
    return F.round(null_safe_divide(part, total) * 100, decimals)


def clamp(col: Column, lower: float, upper: float) -> Column:
    """Restrict column values to a ``[lower, upper]`` range.

    Args:
        col: Input numeric column.
        lower: Minimum allowed value.
        upper: Maximum allowed value.

    Returns:
        Column with values clamped to the specified range.

    Raises:
        ValueError: If ``lower`` is greater than ``upper``.
    """
    if lower > upper:
        raise ValueError(f"lower ({lower}) must not be greater than upper ({upper})")
    return F.greatest(F.lit(lower), F.least(F.lit(upper), col))

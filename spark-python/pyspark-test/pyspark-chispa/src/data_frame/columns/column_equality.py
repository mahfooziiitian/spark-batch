"""Column-level transformation functions.

Each function receives a PySpark Column and returns a transformed Column,
making them composable inside ``withColumn`` calls.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def remove_non_word_characters(col: Column) -> Column:
    """Remove all non-word characters from a string column.

    Strips everything except word characters (``\\w``) and whitespace (``\\s``).

    Args:
        col: Input string column.

    Returns:
        Column with non-word characters removed.
    """
    return F.regexp_replace(col, "[^\\w\\s]+", "")


def normalize_whitespace(col: Column) -> Column:
    """Collapse consecutive whitespace into a single space and trim.

    Args:
        col: Input string column.

    Returns:
        Column with normalized whitespace.
    """
    return F.trim(F.regexp_replace(col, "\\s+", " "))


def extract_email_domain(col: Column) -> Column:
    """Extract the domain part from an email address column.

    Args:
        col: Input string column containing email addresses.

    Returns:
        Column containing only the domain (after ``@``), or null if no ``@``.
    """
    return F.when(col.contains("@"), F.split(col, "@").getItem(1))


def title_case(col: Column) -> Column:
    """Convert a string column to title case.

    Args:
        col: Input string column.

    Returns:
        Column with the first letter of each word capitalized.
    """
    return F.initcap(col)


def null_safe_trim(col: Column) -> Column:
    """Trim whitespace from a string column, preserving nulls.

    Args:
        col: Input string column.

    Returns:
        Trimmed column, or null if the input is null.
    """
    return F.when(col.isNotNull(), F.trim(col))


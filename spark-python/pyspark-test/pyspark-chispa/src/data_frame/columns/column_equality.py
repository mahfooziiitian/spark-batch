from pyspark.sql import Column
from pyspark.sql import functions as F


def remove_non_word_characters(col: Column) -> Column:
    """Remove all non-word characters from a string column.

    Strips everything except word characters (``\\w``) and whitespace (``\\s``).

    Args:
        col: Input string column.

    Returns:
        Column with non-word characters removed.

    Example:
        >>> df.withColumn("clean", remove_non_word_characters(F.col("name")))
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
    """Extract the domain portion (after ``@``) from an email address column.

    Args:
        col: Input string column containing email addresses.

    Returns:
        Column with the domain part, or ``null`` if no ``@`` is present.
    """
    return F.when(col.contains("@"), F.element_at(F.split(col, "@"), 2))


def title_case(col: Column) -> Column:
    """Capitalise the first letter of each word.

    Args:
        col: Input string column.

    Returns:
        Column with title-cased text.
    """
    return F.initcap(col)


def null_safe_trim(col: Column) -> Column:
    """Trim whitespace while preserving ``null`` values.

    Args:
        col: Input string column.

    Returns:
        Trimmed column, with ``null`` inputs remaining ``null``.
    """
    return F.when(col.isNotNull(), F.trim(col))

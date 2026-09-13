"""Schema builder utilities for HTML-derived DataFrames.

Provides ready-made StructType schemas for the common shapes produced by the
reader/parsing modules (links, meta tags, generic scraped tables).
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    MapType,
    StringType,
    StructField,
    StructType,
)


def link_schema() -> StructType:
    """Schema for a single scraped ``<a>`` element: text + href."""
    return StructType(
        [
            StructField("text", StringType(), True),
            StructField("href", StringType(), True),
        ]
    )


def links_array_schema() -> ArrayType:
    """Schema for an array of scraped links, as produced by parsing.extract_links()."""
    return ArrayType(link_schema())


def meta_tags_schema() -> MapType:
    """Schema for a map of meta tag name/property -> content."""
    return MapType(StringType(), StringType())


def table_row_schema(columns: list[str]) -> StructType:
    """Build a flat, all-string schema for a table extracted via pandas.read_html().

    HTML tables have no inherent typing, so every column is read as StringType
    by default; cast individual columns downstream as needed.

    Args:
        columns: Column names, typically taken from the table's header row.

    Returns:
        StructType with one nullable StringType field per column.

    Example:
        >>> table_row_schema(["Rank", "Country", "Population"])
        StructType([StructField('Rank', StringType(), True), ...])
    """
    return StructType([StructField(name, StringType(), True) for name in columns])


def with_source_url(schema: StructType, column_name: str = "_source_url") -> StructType:
    """Append a source-URL tracking column to an existing schema.

    Useful when unioning tables scraped from multiple pages, to retain provenance.

    Args:
        schema: Base schema to extend.
        column_name: Name of the tracking column.

    Returns:
        New StructType with the tracking field appended.
    """
    return StructType([*schema.fields, StructField(column_name, StringType(), True)])

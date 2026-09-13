"""HTML parsing utilities — Spark column UDFs and DataFrame helpers built on BeautifulSoup.

These functions operate on a Spark column of raw HTML strings (e.g., a scraped-pages
column), letting you extract structured data without leaving the DataFrame API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

from pys_html._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

logger = get_logger("parsing")

_LINK_SCHEMA = ArrayType(
    StructType(
        [
            StructField("text", StringType(), True),
            StructField("href", StringType(), True),
        ]
    )
)


def _attr_str(value: object) -> str | None:
    """Coerce a BeautifulSoup attribute value (str, list of str, or None) to a plain string.

    BeautifulSoup returns list-valued attributes (e.g. multi-valued class/rel) as a list;
    this joins them with a space so downstream code can treat every attribute as ``str | None``.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return " ".join(str(v) for v in value)
    return str(value)


def _strip_tags(html: str | None) -> str | None:
    if html is None:
        return None
    return BeautifulSoup(html, "lxml").get_text(separator=" ", strip=True)


def _extract_links(html: str | None) -> list[dict[str, str | None]]:
    if html is None:
        return []
    soup = BeautifulSoup(html, "lxml")
    return [{"text": a.get_text(strip=True), "href": _attr_str(a.get("href"))} for a in soup.find_all("a")]


def _extract_meta(html: str | None) -> dict[str, str]:
    if html is None:
        return {}
    soup = BeautifulSoup(html, "lxml")
    meta: dict[str, str] = {}
    for tag in soup.find_all("meta"):
        name = _attr_str(tag.get("name")) or _attr_str(tag.get("property"))
        content = _attr_str(tag.get("content"))
        if name and content:
            meta[name] = content
    return meta


def _extract_tag_text(html: str | None, tag: str) -> list[str]:
    if html is None:
        return []
    soup = BeautifulSoup(html, "lxml")
    return [el.get_text(strip=True) for el in soup.find_all(tag)]


def strip_html_tags(col: str | Column) -> Column:
    """Return a Column expression with HTML tags removed, leaving only visible text.

    Args:
        col: Column name or Column expression containing HTML strings.

    Returns:
        Column of plain text.

    Example:
        >>> df.withColumn("plain_text", strip_html_tags("html_col"))
    """
    html_col = F.col(col) if isinstance(col, str) else col
    return F.udf(_strip_tags, StringType())(html_col)


def extract_links(col: str | Column) -> Column:
    """Extract all ``<a href="...">`` links from an HTML column as an array of structs.

    Args:
        col: Column name or Column expression containing HTML strings.

    Returns:
        Column of ``array<struct<text:string,href:string>>``.

    Example:
        >>> df.withColumn("links", extract_links("html_col")).select(explode("links"))
    """
    html_col = F.col(col) if isinstance(col, str) else col
    return F.udf(_extract_links, _LINK_SCHEMA)(html_col)


def extract_meta_tags(col: str | Column) -> Column:
    """Extract ``<meta name="..." content="...">`` tags from an HTML column as a map.

    Args:
        col: Column name or Column expression containing HTML strings.

    Returns:
        Column of ``map<string,string>`` keyed by the meta tag's name/property.
    """
    html_col = F.col(col) if isinstance(col, str) else col
    return F.udf(_extract_meta, MapType(StringType(), StringType()))(html_col)


def extract_tag_text(col: str | Column, tag: str) -> Column:
    """Extract the text content of every occurrence of a tag from an HTML column.

    Args:
        col: Column name or Column expression containing HTML strings.
        tag: HTML tag name (e.g., "h1", "p", "li").

    Returns:
        Column of ``array<string>``, one entry per matched tag.
    """
    html_col = F.col(col) if isinstance(col, str) else col
    return F.udf(lambda html: _extract_tag_text(html, tag), ArrayType(StringType()))(html_col)


def explode_links(df: DataFrame, html_col: str = "html", link_col: str = "link") -> DataFrame:
    """Convenience wrapper: extract and explode links from an HTML column in one call.

    Args:
        df: Source DataFrame containing a column of raw HTML strings.
        html_col: Name of the column holding HTML markup.
        link_col: Name for the resulting exploded struct column.

    Returns:
        DataFrame with one row per link, plus ``<link_col>.text`` and ``<link_col>.href``.
    """
    logger.debug("Exploding links from column %r", html_col)
    return df.withColumn(link_col, F.explode(extract_links(html_col)))

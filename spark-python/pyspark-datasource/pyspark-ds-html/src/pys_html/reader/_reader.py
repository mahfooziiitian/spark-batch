"""Reusable HTML reader — bridges pyspark.pandas.read_html() and BeautifulSoup into Spark DataFrames.

Spark has no native HTML data source, so this module provides two complementary paths:

- ``read_tables()``    — extract ``<table>`` elements via ``pyspark.pandas.read_html()``
                          (Spark's own pandas-on-Spark bridge, lxml/html5lib backed), one
                          DataFrame per table.
- ``read_elements()``  — scrape arbitrary tags (links, headings, meta tags, ...) via
                          BeautifulSoup and materialize the results as a Spark DataFrame.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import pyspark.pandas as ps
from bs4 import BeautifulSoup
from pyspark.pandas.utils import PandasAPIOnSparkAdviceWarning
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pys_html._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger("reader")

DEFAULT_PARSER = "lxml"


def _load_html_text(source: str) -> str:
    """Read HTML content from a local file path or return the raw string as-is."""
    candidate = Path(source)
    if candidate.exists():
        return candidate.read_text(encoding="utf-8")
    return source


def _is_url_or_path(source: str) -> bool:
    """Return True if source looks like a URL or an existing local file path."""
    return source.startswith(("http://", "https://")) or Path(source).exists()


def _attr_to_str(value: object) -> str:
    """Coerce a BeautifulSoup attribute value (str or list of str) into a plain string."""
    if isinstance(value, list):
        return " ".join(str(v) for v in value)
    return str(value)


@dataclass
class HtmlReader:
    """Configurable HTML reader producing Spark DataFrames.

    Args:
        spark: Active SparkSession.
        parser: BeautifulSoup/pandas parser backend ("lxml", "html5lib", "html.parser").
        options: Additional pyspark.pandas.read_html() keyword arguments (match, header, attrs, ...).

    Example:
        >>> reader = HtmlReader(spark)
        >>> tables = reader.read_tables("https://example.com/page.html")
        >>> df = reader.read_links("<html>...</html>")
    """

    spark: SparkSession
    parser: str = DEFAULT_PARSER
    options: dict[str, object] = field(default_factory=dict)

    def with_option(self, key: str, value: object) -> HtmlReader:
        """Return a new reader with an additional pyspark.pandas.read_html() option set."""
        new_options = {**self.options, key: value}
        return HtmlReader(spark=self.spark, parser=self.parser, options=new_options)

    def with_parser(self, parser: str) -> HtmlReader:
        """Return a new reader using the given parser backend."""
        return HtmlReader(spark=self.spark, parser=parser, options=self.options)

    def match(self, pattern: str) -> HtmlReader:
        """Only keep tables whose text matches the given regex pattern."""
        return self.with_option("match", pattern)

    def header(self, row: int | list[int]) -> HtmlReader:
        """Set the row(s) to use as column header(s)."""
        return self.with_option("header", row)

    def attrs(self, attributes: dict[str, str]) -> HtmlReader:
        """Only keep tables matching the given HTML attributes (e.g., {"id": "results"})."""
        return self.with_option("attrs", attributes)

    # --- Table extraction (pyspark.pandas.read_html bridge) ---

    def read_tables(self, source: str) -> list[DataFrame]:
        """Extract every ``<table>`` element from HTML into a list of Spark DataFrames.

        Uses ``pyspark.pandas.read_html()`` — Spark's own pandas-on-Spark bridge — so each
        result converts directly to a Spark DataFrame via ``.to_spark()`` without a manual
        ``spark.createDataFrame()`` round trip.

        Args:
            source: URL, local file path, or raw HTML string.

        Returns:
            One Spark DataFrame per ``<table>`` found, in document order.
        """
        logger.info("Reading HTML tables from %s (parser=%s, options=%s)", source, self.parser, self.options)
        html_source = source if _is_url_or_path(source) else StringIO(source)
        read_options = dict(self.options)
        # Local files are read via pandas' internal file handling, which otherwise falls back to
        # the platform's locale-default encoding (mangling non-ASCII text, e.g. Hindi headers).
        # URLs already carry their own encoding via HTTP headers, so leave those alone.
        if (
            "encoding" not in read_options
            and isinstance(html_source, str)
            and not html_source.startswith(("http://", "https://"))
        ):
            read_options["encoding"] = "utf-8"
        # options is an intentionally open-ended dict[str, object] (fluent builder pass-through of
        # any pyspark.pandas.read_html() keyword argument), which mypy cannot precisely match
        # against read_html()'s narrowly-typed overloads.
        pandas_on_spark_frames: list[ps.DataFrame] = ps.read_html(
            html_source,
            flavor=self.parser,
            **read_options,  # type: ignore[arg-type]
        )
        logger.debug("Found %d table(s)", len(pandas_on_spark_frames))

        with warnings.catch_warnings():
            # The pandas index is intentionally dropped — HTML tables have no meaningful index.
            warnings.simplefilter("ignore", category=PandasAPIOnSparkAdviceWarning)
            return [psdf.astype(str).to_spark() for psdf in pandas_on_spark_frames]

    def read_table(self, source: str, index: int = 0) -> DataFrame:
        """Extract a single ``<table>`` element (by position) as a Spark DataFrame.

        Args:
            source: URL, local file path, or raw HTML string.
            index: Zero-based index of the table to return.

        Returns:
            Spark DataFrame for the requested table.
        """
        tables = self.read_tables(source)
        return tables[index]

    def read_matching_tables(self, sources: list[str], source_column: str | None = "_source_file") -> DataFrame:
        """Read the table(s) matching this reader's filters from *multiple* sources and merge them.

        Apply ``.attrs()``, ``.match()``, and/or ``.header()`` beforehand to narrow each source
        down to the table you want (e.g. the same ``id="results"`` table repeated across several
        report files), then merge every matching table into a single Spark DataFrame via
        ``unionByName(allowMissingColumns=True)`` — tolerant of minor column drift between files.

        Args:
            sources: URLs, local file paths, or raw HTML strings to read and merge.
            source_column: Name of a column to add recording which source each row came from
                (set to ``None`` to skip).

        Returns:
            A single Spark DataFrame containing every matching table's rows, in source order.

        Raises:
            ValueError: If no source yields a matching table, or ``sources`` is empty.
        """
        if not sources:
            raise ValueError("No sources given to read_matching_tables().")

        frames: list[DataFrame] = []
        for source in sources:
            try:
                tables = self.read_tables(source)
            except ValueError as exc:
                # pyspark.pandas.read_html() raises ValueError (rather than returning an empty
                # list) when a match/attrs filter excludes every table in a source.
                logger.warning("No matching table found in %s (%s)", source, exc)
                continue
            if not tables:
                logger.warning("No matching table found in %s", source)
                continue
            for table_df in tables:
                if source_column:
                    table_df = table_df.withColumn(source_column, F.lit(source))
                frames.append(table_df)

        if not frames:
            raise ValueError(f"No matching table found in any of the {len(sources)} given source(s).")

        logger.info("Merging %d matching table(s) from %d source(s)", len(frames), len(sources))
        merged = frames[0]
        for table_df in frames[1:]:
            merged = merged.unionByName(table_df, allowMissingColumns=True)
        return merged

    # --- General scraping (BeautifulSoup bridge) ---

    def read_elements(self, source: str, tag: str, attrs: dict[str, str] | None = None) -> DataFrame:
        """Scrape all occurrences of an HTML tag into a Spark DataFrame of text/attributes.

        Args:
            source: URL-fetched HTML, local file path, or raw HTML string.
            tag: HTML tag name to search for (e.g., "a", "img", "meta").
            attrs: Optional attribute filter passed to BeautifulSoup.find_all().

        Returns:
            DataFrame with columns "text" and one column per captured attribute.
        """
        html_text = _load_html_text(source)
        soup = BeautifulSoup(html_text, self.parser)
        elements = soup.find_all(tag, attrs=attrs or {})  # type: ignore[arg-type]
        logger.info("Scraped %d <%s> element(s)", len(elements), tag)

        rows = [{"text": el.get_text(strip=True), **{k: str(v) for k, v in el.attrs.items()}} for el in elements]
        if not rows:
            return self.spark.createDataFrame([], schema="text STRING")
        return self.spark.createDataFrame(pd.DataFrame(rows).astype(str))

    def read_links(self, source: str) -> DataFrame:
        """Scrape all ``<a href="...">`` links into a DataFrame with "text" and "href" columns."""
        return self.read_elements(source, "a")

    def read_text(self, source: str) -> str:
        """Extract all visible text from an HTML document (tags stripped)."""
        html_text = _load_html_text(source)
        soup = BeautifulSoup(html_text, self.parser)
        return soup.get_text(separator=" ", strip=True)

    def list_tables(self, source: str) -> list[dict[str, object]]:
        """List every ``<table>`` element in a document along with its HTML attributes.

        Useful for discovering how to target a specific table with ``.attrs()`` before
        calling ``read_table()``/``read_tables()`` — e.g. find which table has ``id="results"``.

        Args:
            source: URL-fetched HTML, local file path, or raw HTML string.

        Returns:
            One dict per ``<table>`` found (in document order), each with:
                - ``index``: zero-based position among all ``<table>`` elements.
                - ``attrs``: dict of the table's own HTML attributes (id, class, ...).
                - ``num_rows``: number of ``<tr>`` rows (including any header row).
                - ``num_cols``: number of cells in the first row (0 if the table is empty).
                - ``headers``: text of ``<th>`` cells in the first row, if any.
                - ``caption``: text of the table's ``<caption>`` element, if present.
        """
        html_text = _load_html_text(source)
        soup = BeautifulSoup(html_text, self.parser)
        tables = soup.find_all("table")
        logger.info("Listing %d <table> element(s) in %s", len(tables), source)

        summaries: list[dict[str, object]] = []
        for i, table in enumerate(tables):
            rows = table.find_all("tr")
            first_row_cells = rows[0].find_all(["th", "td"]) if rows else []
            header_cells = rows[0].find_all("th") if rows else []
            caption = table.find("caption")

            summaries.append(
                {
                    "index": i,
                    "attrs": {k: _attr_to_str(v) for k, v in table.attrs.items()},
                    "num_rows": len(rows),
                    "num_cols": len(first_row_cells),
                    "headers": [c.get_text(strip=True) for c in header_cells],
                    "caption": caption.get_text(strip=True) if caption else None,
                }
            )
        return summaries

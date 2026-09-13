"""Reusable HTML writer — bridges Spark DataFrames to HTML via pandas.to_html()."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from pys_html._logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger("writer")


@dataclass
class HtmlWriter:
    """Configurable HTML writer wrapping DataFrame.toPandas().to_html().

    Spark has no native HTML sink, so this collects the DataFrame to the driver
    (via toPandas()) before rendering markup. Intended for small/aggregated
    result sets (reports, dashboards), not large distributed datasets.

    Args:
        index: Whether to include the DataFrame index column.
        classes: CSS class name(s) applied to the rendered <table> element.
        options: Additional pandas.DataFrame.to_html() keyword arguments.

    Example:
        >>> writer = HtmlWriter(classes="table table-striped").border(1)
        >>> writer.write(df, "/output/report.html")
    """

    index: bool = False
    classes: str | list[str] | None = None
    options: dict[str, object] = field(default_factory=dict)

    def with_option(self, key: str, value: object) -> HtmlWriter:
        """Return a new writer with an additional to_html() option set."""
        new_options = {**self.options, key: value}
        return HtmlWriter(index=self.index, classes=self.classes, options=new_options)

    def with_index(self, enabled: bool = True) -> HtmlWriter:
        """Return a new writer that includes/excludes the DataFrame index column."""
        return HtmlWriter(index=enabled, classes=self.classes, options=self.options)

    def with_classes(self, classes: str | list[str]) -> HtmlWriter:
        """Return a new writer that applies the given CSS class(es) to the table."""
        return HtmlWriter(index=self.index, classes=classes, options=self.options)

    def border(self, width: int) -> HtmlWriter:
        """Set the HTML table border width in pixels."""
        return self.with_option("border", width)

    def escape(self, enabled: bool = True) -> HtmlWriter:
        """Enable/disable HTML entity escaping of cell values (default: True)."""
        return self.with_option("escape", enabled)

    def table_id(self, element_id: str) -> HtmlWriter:
        """Set the ``id`` attribute of the rendered <table> element."""
        return self.with_option("table_id", element_id)

    def render(self, df: DataFrame) -> str:
        """Render a DataFrame as an HTML ``<table>`` string.

        Args:
            df: DataFrame to render. Collected to the driver via toPandas().

        Returns:
            HTML markup string.
        """
        pdf: pd.DataFrame = df.toPandas()
        logger.info("Rendering DataFrame (%d rows, %d cols) to HTML", len(pdf), len(pdf.columns))
        markup: str = pdf.to_html(index=self.index, classes=self.classes, **self.options)
        return markup

    def write(self, df: DataFrame, path: str) -> str:
        """Render a DataFrame as HTML and write it to a single file.

        Args:
            df: DataFrame to write.
            path: Destination file path.

        Returns:
            The path written to.
        """
        markup = self.render(df)
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(markup)
        logger.debug("Wrote HTML table to %s", path)
        return path

    def write_page(self, df: DataFrame, path: str, title: str = "Report") -> str:
        """Render a DataFrame and wrap it in a minimal standalone HTML document.

        Args:
            df: DataFrame to write.
            path: Destination file path.
            title: Document ``<title>`` and page heading.

        Returns:
            The path written to.
        """
        table_markup = self.render(df)
        page = (
            "<!DOCTYPE html>\n<html>\n<head>\n"
            f'<meta charset="utf-8"><title>{title}</title>\n'
            "</head>\n<body>\n"
            f"<h1>{title}</h1>\n{table_markup}\n"
            "</body>\n</html>\n"
        )
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(page)
        logger.debug("Wrote standalone HTML page to %s", path)
        return path

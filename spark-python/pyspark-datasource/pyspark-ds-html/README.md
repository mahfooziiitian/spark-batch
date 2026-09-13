# pyspark-ds-html

PySpark 4 HTML datasource — tutorials, demos, and a reusable library (`pys_html`) for reading,
scraping, parsing, and writing HTML content with Apache Spark.

Spark has no native HTML data source, so this project bridges:

- **`pandas.read_html()`** — extract `<table>` elements into Spark DataFrames.
- **BeautifulSoup** — scrape arbitrary tags (links, headings, meta tags, article text).
- **`pandas.to_html()`** — render DataFrame results back out as HTML reports.

## Quick Start

```bash
uv sync --group dev
uv run python examples/01_data_source/01_read_html_tables.py
```

```python
from pys_html import HtmlReader, get_spark, print_dataframe

spark = get_spark("quickstart")
df = HtmlReader(spark).read_table("path/to/page.html")
print_dataframe(df)
spark.stop()
```

## Documentation

Full docs (MkDocs Material) live under `docs/` — serve locally with:

```bash
uv run mkdocs serve
```

See `docs/index.md` for the Quick Start, architecture overview, and full project structure,
or `examples/README.md` for a guide to the runnable examples.

## Development

```bash
make install   # Install dependencies
make test      # Run pytest
make lint      # Run ruff
make docs      # Build documentation
make ci        # Full CI pipeline
```

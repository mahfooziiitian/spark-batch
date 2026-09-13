# Getting Started

## Prerequisites

- Python ≥ 3.11
- Java 17 (LTS)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Install

```bash
cd pyspark-ds-html
uv sync --group dev
```

This installs PySpark 4.x, pandas, `beautifulsoup4`, `lxml`, `html5lib`, and Rich, plus the
dev tooling (pytest, ruff, mypy, mkdocs).

## Run your first example

```bash
uv run python examples/01_data_source/01_read_html_tables.py
```

You should see a Rich-formatted table showing the extracted `<table>` contents and its
inferred (all-string) Spark schema.

## Next steps

- [Data Source](../data-source/index.md) — reading tables and scraping tags
- [Parsing](../parsing/index.md) — column-level UDFs for links, text, and meta tags
- [DataFrame Patterns](../dataframe/index.md) — transformations, writing, pandas round trips

# PySpark HTML Datasource — Copilot Instructions

## Project Overview

PySpark HTML datasource reference and reusable library (`pys_html`). Spark has no native
HTML data source, so this project bridges `pandas.read_html()` (table extraction) and
BeautifulSoup (general scraping) into Spark DataFrames, and `pandas.to_html()` for writing
results back out as HTML.

## Tech Stack

- **Python** ≥ 3.11
- **PySpark** ≥ 4.0.0
- **pandas** (HTML table read/write bridge)
- **beautifulsoup4 + lxml/html5lib** (scraping, column-level parsing)
- **rich** (formatted console output)
- **Build**: hatchling (pyproject.toml with `hatchling.build` backend)
- **Package manager**: uv
- **Testing**: pytest with `pythonpath = ["src"]` and `testpaths = ["tests"]`

## Source Structure

```
src/pys_html/
├── __init__.py       # Public API re-exports
├── config.py         # get_spark(), DATA_HOME, output_path(), write_html_file()
├── session.py        # create_spark_session() for library/test usage
├── _logging.py       # Rich-powered logging & print helpers
├── reader/           # HtmlReader — pandas.read_html + BeautifulSoup bridge
├── writer/           # HtmlWriter — pandas.to_html bridge
├── parsing/          # Column UDFs: extract_links, strip_html_tags, extract_meta_tags
└── schema/           # Schema builders: link_schema, table_row_schema, meta_tags_schema

examples/
├── 01_data_source/   # Reading <table> elements and scraping arbitrary tags
├── 02_parsing/       # Column-level parsing UDFs applied within a DataFrame
└── 03_dataframe/     # Create, transform, write, pandas round trips

tests/
└── data_frame/test_html_dataframe.py
```

## Modular Instruction Files

| File                                          | Scope                                             | Purpose                                      |
|-----------------------------------------------|---------------------------------------------------|----------------------------------------------|
| `instructions/python.instructions.md`         | `**/*.py`                                         | Python style and conventions                 |
| `instructions/pyspark-html.instructions.md`   | `src/**/*.py`, `examples/**/*.py`, `docs/**/*.md` | HTML datasource patterns and Spark API usage |
| `instructions/testing.instructions.md`        | `**/test_*.py`, `**/*_test.py`                    | pytest conventions and SparkSession fixtures |
| `instructions/mkdocs.instructions.md`         | `mkdocs.yml`, `docs/**/*.md`                      | MkDocs Material documentation style          |
| `instructions/examples.instructions.md`       | `examples/**/*.py`                                | Example script conventions                   |
| `instructions/logging-rich.instructions.md`   | `src/**/*.py`, `examples/**/*.py`                 | Rich console/logging helper usage            |
| `instructions/project-config.instructions.md` | `pyproject.toml`                                  | Package configuration and build setup        |

## Things to Avoid

- Do not assume `spark.read.format("html")` exists — Spark has no native HTML source.
- Do not use `findspark` — PySpark is installed via uv/pip and available directly.
- Do not hardcode absolute file paths — use `pys_html.config.data_path()` / `output_path()`.
- Do not call `df.toPandas()` on large/unbounded DataFrames when writing HTML — aggregate or
  `.limit()` first.
- Do not access extracted map columns with dot notation (`meta.author`) — use bracket access
  (`col("meta")["author"]`).
- Do not create a SparkSession without checking `SPARK_MASTER` env var for the master URL.
- Do not skip `spark.stop()` — always stop the session at the end of each script.

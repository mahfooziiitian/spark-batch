---
applyTo: "{src/**/*.py,examples/**/*.py,docs/**/*.md}"
---

# PySpark 4 HTML Datasource Patterns

## PySpark Version

This project targets **PySpark 4.x** (Apache Spark 4.0+). Use PySpark 4 APIs and features:

- **Spark Connect** — preferred client mode for Databricks and remote clusters.
- **Python Data Source API v2** — could be used to implement a true custom `.format("html")`
  source, but this project favors the simpler pandas/BeautifulSoup bridge documented below.

## No Native HTML Data Source

Unlike JSON/CSV/Parquet, Spark has **no built-in `spark.read.html()`**. Every read/write path
in this project bridges through one of:

1. **`pandas.read_html()`** — parses `<table>` elements (uses `lxml` or `html5lib` under the
   hood) and returns a list of `pandas.DataFrame`. Bridge into Spark via `spark.createDataFrame()`.
2. **BeautifulSoup** — for arbitrary tags (links, headings, meta tags, article text) that
   aren't laid out as `<table>` elements.
3. **`pandas.DataFrame.to_html()`** — for writing DataFrame results back out as HTML markup
   (via `df.toPandas().to_html()`).

Always prefer the `pys_html` library wrappers (`HtmlReader`, `HtmlWriter`,
`pys_html.parsing.*`) over calling pandas/BeautifulSoup directly in examples — they centralize
logging, option handling, and schema conventions.

## SparkSession Initialization

### Local Mode (examples and tests)

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .appName("html-example")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ... work with spark ...

spark.stop()
```

Prefer `pys_html.get_spark()` in examples — it wraps the above plus `JAVA_HOME`/
`PYSPARK_PYTHON` environment configuration.

## Reading HTML Tables

```python
from pys_html import HtmlReader, get_spark

spark = get_spark("html-example")
reader = HtmlReader(spark)

# All tables in a document
tables = reader.read_tables("path/to/page.html")  # list[DataFrame]

# A single table by position
df = reader.read_table("path/to/page.html", index=0)

# Target a specific table via pandas.read_html() options
df = reader.attrs({"id": "results"}).read_table("path/to/page.html")
```

`source` accepts a local file path, a raw HTML string, or a URL — anything
`pandas.read_html()` accepts.

!!! important
    Extracted table columns are always `StringType` — HTML has no type system. Cast columns
    explicitly with `.withColumn(name, col(name).cast(...))` after reading.

## Scraping Arbitrary Tags

```python
reader = HtmlReader(spark)

links_df = reader.read_links("path/to/page.html")           # <a> text + href
headings_df = reader.read_elements("path/to/page.html", "h2")
plain_text = reader.read_text("path/to/page.html")           # tags stripped
```

## Column-Level Parsing (existing DataFrame of HTML strings)

Use `pys_html.parsing` UDFs when HTML markup is already a column in a DataFrame (e.g., raw
pages you scraped and stored), rather than re-reading files:

```python
from pys_html.parsing import extract_links, extract_meta_tags, strip_html_tags

df = df.withColumn("links", extract_links("html_col"))
df = df.withColumn("meta", extract_meta_tags("html_col"))
df = df.withColumn("plain_text", strip_html_tags("html_col"))
```

Map columns use bracket access, not dot notation: `col("meta")["author"]`.

## Writing HTML

```python
from pys_html import HtmlWriter

writer = HtmlWriter(classes="table table-striped").border(1)
writer.write(df, "/output/report.html")            # <table> fragment
writer.write_page(df, "/output/report.html", title="Report")  # standalone document
```

`HtmlWriter` collects the DataFrame via `toPandas()` before rendering — only use it for
small/aggregated result sets, never large distributed datasets. Aggregate, filter, or
`.limit()` first.

## Parser Backend Choice

- `lxml` (default) — fast, lenient; good default for well-formed pages.
- `html5lib` — stricter, browser-like parsing; use for malformed/legacy markup via
  `HtmlReader(spark).with_parser("html5lib")`.

## Things to Avoid

- Do not assume a native `spark.read.format("html")` exists — it does not.
- Do not call `df.toPandas()` on large/unbounded DataFrames when writing HTML — aggregate
  or limit first.
- Do not use dot-notation to access extracted map columns (`meta.author`) — use bracket
  access (`col("meta")["author"]`).
- Do not skip `spark.stop()` — always stop the session at the end of each script.
- Do not hardcode absolute file paths — use `pys_html.config.data_path()` / `output_path()`.

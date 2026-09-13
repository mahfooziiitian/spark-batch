# Reading HTML Tables

`HtmlReader.read_tables()` wraps `pyspark.pandas.read_html()` — Spark's own pandas-on-Spark
bridge — which parses every `<table>` element in a document and returns one
`pyspark.pandas.DataFrame` per table. Each is then converted to a Spark DataFrame directly via
`.to_spark()`, with no manual `spark.createDataFrame()` round trip required.

```python
--8<-- "examples/01_data_source/01_read_html_tables.py"
```

## Targeting a specific table

Use `.attrs()`, `.match()`, or `.header()` to narrow down which table(s) are extracted —
these map directly onto `pyspark.pandas.read_html()` keyword arguments.

```python
--8<-- "examples/01_data_source/02_read_html_targeted_table.py"
```

!!! note "All columns arrive as strings"
    HTML tables carry no type information, so every extracted column is read as `StringType`.
    Cast columns explicitly downstream — see [Transformations](../dataframe/transformations.md).

## Generic file/URL reader (CLI)

`04_read_html_file_cli.py` exposes every `HtmlReader` option as an `argparse` flag, so the same
script reads any local file or URL without editing code:

```python
--8<-- "examples/01_data_source/04_read_html_file_cli.py"
```

```bash
uv run python examples/01_data_source/04_read_html_file_cli.py \
    --input path/to/page.html --attrs id=results --header 0 --table-index 0
```

Run `--help` for the full flag reference (`--input`/`--url`, `--parser`, `--attrs`, `--match`,
`--header`, `--table-index`).

## Discovering tables and their attributes

Before targeting a specific table with `--attrs`/`--match`, use `HtmlReader.list_tables()` to
see every `<table>` element in a document along with its HTML attributes (`id`, `class`, ...),
shape (`num_rows`/`num_cols`), column headers, and caption — without loading the table data
itself. The example accepts a single `--input`/`--url`, or multiple sources at once via
`--inputs`/`--input-glob` (handy for scanning a whole directory of reports before merging them):

```python
--8<-- "examples/01_data_source/05_list_tables_and_attrs.py"
```

```bash
# A single file or URL
uv run python examples/01_data_source/05_list_tables_and_attrs.py --input path/to/page.html

# Every file in a directory
uv run python examples/01_data_source/05_list_tables_and_attrs.py --input-glob "data/reports/*.html"

# Several explicit files
uv run python examples/01_data_source/05_list_tables_and_attrs.py --inputs a.html b.html c.html
```

`list_tables()` returns a plain `list[dict[str, object]]` (one dict per table, in document
order) with keys `index`, `attrs`, `num_rows`, `num_cols`, `headers`, and `caption` — handy for
scripting or for the Rich-table console output shown above (printed per-source, followed by a
grand total across all sources).

## Merging a matching table across multiple files

When several HTML files/URLs each contain the *same* table (e.g. daily/branch reports sharing
an `id="results"` table), narrow it down with `.attrs()`/`.match()`/`.header()` and merge every
source's matching table into one Spark DataFrame with `HtmlReader.read_matching_tables()`:

```python
--8<-- "examples/01_data_source/06_merge_matching_tables.py"
```

```bash
uv run python examples/01_data_source/06_merge_matching_tables.py \
    --inputs report_jan.html report_feb.html report_mar.html --attrs id=results

# or match every file in a directory
uv run python examples/01_data_source/06_merge_matching_tables.py \
    --input-glob "data/reports/*.html" --attrs id=results
```

Rows are unioned via `unionByName(allowMissingColumns=True)` (tolerant of minor column drift
between files) and tagged with a `_source_file` column (configurable via `--source-column`,
or set `source_column=None` to omit it) so you can trace each row back to its origin. A source
whose filters exclude every table is skipped with a warning rather than failing the whole run;
a `ValueError` is only raised if *no* source has a matching table.

## Options reference

| Method | pyspark.pandas.read_html() option | Purpose |
|--------|-----------------------------------|---------|
| `.match(pattern)` | `match` | Only keep tables whose text matches a regex |
| `.header(row)` | `header` | Row index (or list) to use as column header(s) |
| `.attrs(dict)` | `attrs` | Only keep tables matching given HTML attributes |
| `.with_parser(name)` | `flavor` | Parser backend: `lxml`, `html5lib`, or `bs4` |
| `.with_option(key, value)` | any | Pass through any other `read_html()` keyword argument |

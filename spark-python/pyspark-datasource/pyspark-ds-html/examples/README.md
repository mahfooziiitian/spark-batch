# Examples

Runnable examples for the PySpark HTML datasource library (`pys_html`).

Every example accepts `--help` and works with zero arguments (falling back to a bundled
sample), while exposing CLI flags to point it at your own HTML file, URL, or options:

```bash
uv run python examples/01_data_source/01_read_html_tables.py                     # bundled sample
uv run python examples/01_data_source/01_read_html_tables.py --input page.html   # your own file
uv run python examples/01_data_source/01_read_html_tables.py --url https://...   # a URL
uv run python examples/01_data_source/01_read_html_tables.py --help             # full flag reference
```

Or via the Makefile:

```bash
make run-example EX=examples/01_data_source/01_read_html_tables.py
make run-examples             # run everything
make run-examples CAT=01_data_source   # run one category
```

## Common CLI flags

Shared via `pys_html.cli` helpers so behavior/`--help` text stays consistent:

| Flag | Meaning |
|------|---------|
| `--input` / `-i` | Local HTML file to read (mutually exclusive with `--url`) |
| `--url` / `-u` | HTTP(S) URL to fetch instead of a local file |
| `--parser` | Parser backend: `lxml` (default), `html5lib`, or `bs4` |
| `--attrs KEY=VALUE` | Only consider `<table>` elements with this HTML attribute |
| `--match` | Only consider tables whose text matches this regex |
| `--header` | Row index to use as the column header |
| `--inputs FILE [FILE ...]` | One or more local HTML files to read and merge |
| `--input-glob PATTERN` | Glob pattern matching multiple local HTML files to merge |
| `--query` | Search text for the semantic-search example |
| `--search-column` | Column to fuzzy-search against `--query` |
| `--top-k` | Maximum number of top-scoring rows to return |
| `--min-score` | Minimum similarity score (0.0-1.0) required to keep a row |
| `--scorer` | rapidfuzz strategy: `ratio` (whole-string, default), `partial_ratio` (query as substring), `token_sort_ratio` (word-order independent), `token_set_ratio` (token overlap, good for multi-name cells) |

## Categories

| Directory         | Focus                                                                                                                                                                                                                    |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `01_data_source/` | Reading `<table>` elements (`pyspark.pandas.read_html` bridge) and scraping arbitrary tags (BeautifulSoup bridge). Includes a generic, argparse-driven CLI reader (`04_read_html_file_cli.py`) for arbitrary files/URLs, a table discovery tool (`05_list_tables_and_attrs.py`, supports `--input`/`--url` or multiple sources via `--inputs`/`--input-glob`) that lists every `<table>` with its attributes and shape, and a multi-file merge tool (`06_merge_matching_tables.py`) that unions the matching table across several HTML files/URLs. |
| `02_parsing/`     | Column-level UDFs — link extraction, tag stripping, meta tag maps — applied within a DataFrame. Accept `--html`/`--input` to process custom content instead of the bundled samples.                                    |
| `03_dataframe/`   | End-to-end DataFrame workflows: typing, transformations, writing HTML output (customizable via `--output-dir`/`--classes`/`--border`), pandas round trips, and a bilingual (Hindi/English) fuzzy semantic search over merged multi-file tables (`05_semantic_search_bilingual.py`, powered by `pys_html.search.semantic_search()` / rapidfuzz). |

# Spark SQL

A learning resource for **Apache Spark 4 SQL** — tutorials, demos, architecture
deep-dives, and performance optimization, covering both open-source Spark and
Databricks Runtime (Unity Catalog, Delta Lake, system tables).

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-blue)](https://mahfooziiitian.github.io/spark-batch/)
[![Spark](https://img.shields.io/badge/spark-4.0-orange)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)](https://www.python.org/)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

## What's Here

- **`sql/`** — runnable `.sql` examples organized by topic (DML, joins, window
    functions, aggregation, CTEs, subqueries, optimization, SCD patterns, time
    series, Databricks-only features, and more). This is the primary content.
- **`docs/`** — MkDocs Material documentation site with narrative guides,
    pattern catalogs (`docs/patterns/`), and Databricks-specific system-table
    examples for real-world observability (billing, audit, lineage).
- **`src/spark_sql/`** — Python helpers that load and execute the `.sql`
    examples for testing, plus a Databricks MCP server (`dbx_mcp/`) and small
    CLI utilities.
- **`tests/`** — pytest + [chispa](https://github.com/MrPowers/chispa) suite
    that runs every SQL example against a local Spark session and asserts on
    the results.

## Quick Start

```bash
# Install dependencies (uv-managed virtualenv)
make install

# Run the full test suite
make test

# Format, lint, type-check, and lint changed SQL
make quality

# Build and serve the documentation site locally
make docs-serve   # http://0.0.0.0:8000
```

See `make help` (or the [`Makefile`](Makefile) / [`justfile`](justfile) for a
cross-platform equivalent) for the full list of available targets — testing,
linting, security scans, coverage reports, and docs builds.

## Requirements

- Python 3.11 or 3.12
- [`uv`](https://docs.astral.sh/uv/) for dependency management
- PySpark 4.0+ (installed automatically via `make install`)

## Documentation

Full guides and SQL pattern catalogs live in [`docs/`](docs/index.md) and are
published as a static site — see [`mkdocs.yml`](mkdocs.yml) for navigation.
Pages marked **[Databricks]** use Delta Lake, Unity Catalog, or other
Databricks-only features and may not run on open-source Spark.

## Contributing

Run `make ci` before opening a pull request — it mirrors the full CI
pipeline (formatting, linting, type-checking, SQL linting, tests with
coverage, and a strict docs build) without mutating files.

## License

MIT — see `pyproject.toml` for the full license declaration.

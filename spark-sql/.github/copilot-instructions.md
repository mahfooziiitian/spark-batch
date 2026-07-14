# GitHub Copilot Instructions — spark-sql

## What This Repository Is

A **Spark SQL learning resource** covering tutorials, demos, architecture deep-dives,
and performance optimization — targeting **Apache Spark 3.5** and **Databricks Runtime**.

Content spans: SQL syntax, query patterns, window functions, joins, aggregation,
SCD patterns, time-series analysis, Catalyst optimizer internals, AQE, and execution plans.

> Items marked **[Databricks]** use Databricks-only features (Delta Lake DML, Unity Catalog,
> `OPTIMIZE`, `ZORDER`, `COPY INTO`, etc.) and may not work on open-source Spark.

## Repository Layout

| Directory | Content |
|-----------|---------|
| `src/` | Spark SQL (`.sql`) and PySpark (`.py`) examples organised by topic |
| `tests/` | pytest + chispa test suite validating SQL logic |
| `docs/` | MkDocs Material documentation site |
| `pyproject.toml` | Single config for all tools and tasks |

Package manager: **uv** (`uv run task <name>` for all workflows).

## Modular Instructions

| Scope | File |
|-------|------|
| `docs/**/*.md`, `mkdocs.yml` | [docs.instructions.md](instructions/docs.instructions.md) |
| `src/**/*.sql` | [sql.instructions.md](instructions/sql.instructions.md) |
| `src/**/*.py`, `tests/**/*.py` | [python.instructions.md](instructions/python.instructions.md) |
| `src/**/*.sql`, `docs/**/*.md` | [databricks.instructions.md](instructions/databricks.instructions.md) |
| `**/test_*.py`, `**/*_test.py` | [testing.instructions.md](instructions/testing.instructions.md) |
| `pyproject.toml`, `.github/**`, `src/**`, `tests/**` | [quality.instructions.md](instructions/quality.instructions.md) |

## Core Conventions

1. **`uv run task <name>`** — single entry point for quality, test, docs, build.
2. **All config in `pyproject.toml`** — no standalone config files.
3. **No side effects on import** — Python modules must not execute code at import time.
4. **Spark 3.5 SQL dialect** — `spark.sql.ansi.enabled = true`. Target open-source Spark unless noted.
5. **Label Databricks-only content** — prefix with `[Databricks]` in docs/comments when using Delta DML, Unity Catalog, `OPTIMIZE`, `ZORDER`, `COPY INTO`, or runtime-specific functions.
6. **`:material-xxx:` icons only** — no Unicode emoji in docs.
7. **Use lowercase fenced code blocks** — SQL examples should use ` ```sql ` fences, and other code samples should use the matching lowercase fence type.
8. **Navigation is owned by awesome-pages** — use `.pages` files for section navigation and do not add `nav:` entries to `mkdocs.yml`.
9. **Strict MkDocs build** — `uv run task docs_build` must pass with zero warnings.
10. **Project tasks default to uv** — prefer `uv run task <name>` for quality, tests, docs, and build workflows; use direct commands only for one-off debugging or when no matching task exists.
11. **Row hash**: `md5(concat_ws('||', col1, col2, ...))` for SCD change detection.
12. **Two-step MERGE for SCD Type 2/6** — one MERGE cannot expire + insert for the same key. **[Databricks]**

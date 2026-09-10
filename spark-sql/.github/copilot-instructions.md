# GitHub Copilot Instructions — spark-sql

## What This Repository Is

A **Spark SQL learning resource** covering tutorials, demos, architecture deep-dives,
and performance optimization — targeting **Apache Spark 4** and **Databricks Runtime**.

Content spans: SQL syntax, query patterns, window functions, joins, aggregation,
SCD patterns, time-series analysis, Catalyst optimizer internals, AQE, and execution plans.

> Items marked **[Databricks]** use Databricks-only features (Delta Lake DML, Unity Catalog,
> `OPTIMIZE`, `ZORDER`, `COPY INTO`, etc.) and may not work on open-source Spark.

## Repository Layout

| Directory / File        | Content                                                                                           |
|-------------------------|---------------------------------------------------------------------------------------------------|
| `sql/`                  | Spark SQL (`.sql`) examples organised by topic — **the primary content**                          |
| `src/spark_sql/`        | Python package: SQL test helpers (`_helpers.py`) + the `dbx_mcp` Databricks MCP server + `util`/`model` CLI tooling |
| `tests/`                | pytest + chispa suite that executes the `sql/` examples and validates results                     |
| `docs/`                 | MkDocs Material documentation site (navigation via per-directory `.pages` files)                  |
| `Makefile`              | **Primary task runner** (`make <target>`) — see Commands below                                    |
| `pyproject.toml`        | Central config for ruff, mypy, pytest, coverage, sqlfluff, bandit, taskipy                        |
| `.sqlfluffignore`       | Databricks-only `.sql` files excluded from the open-source `sparksql` linter                      |
| `.safety-policy.yml`    | Safety (v3) `scan` policy                                                                          |
| `.github/instructions/` | Path-scoped Copilot instruction files                                                             |

## Commands

The **Makefile is the primary entry point** — run `make help` to list all 35 targets.
A parallel (older) set of `taskipy` tasks exists in `[tool.taskipy.tasks]` for
`uv run task <name>`, but the **Makefile is authoritative** and kept up to date
(e.g. `safety scan`, sqlfluff parallelism, changed-only SQL targets).

| Task         | Makefile                              | Purpose                                       |
|--------------|---------------------------------------|-----------------------------------------------|
| Fast tests   | `make test-fast`                      | pytest, stop on first failure                 |
| Coverage     | `make test-cov`                       | pytest + coverage gate (60%)                  |
| Quality      | `make quality`                        | format + lint + type-check + SQL              |
| SQL lint/fix | `make sql` / `make sql-lint-changed`  | sqlfluff fix+lint (`SQLFLUFF_PROCESSES=0`)    |
| Security     | `make secure`                         | bandit + `safety scan`                        |
| Docs build   | `make docs-build`                     | MkDocs strict build                           |
| CI pipeline  | `make ci`                             | non-mutating full pipeline                    |
| Clean        | `make clean`                          | remove caches **and** Spark artifacts         |

Prefer the smallest relevant target (e.g. `make sql-lint-changed` for changed SQL)
over the full pipeline.

## Modular Instructions

| Scope                                                            | File                                                                  |
|-----------------------------------------------------------------|-----------------------------------------------------------------------|
| `docs/**/*.md`, `mkdocs.yml`                                    | [docs.instructions.md](instructions/docs.instructions.md)             |
| `sql/**/*.sql`                                                  | [sql.instructions.md](instructions/sql.instructions.md)               |
| `src/**/*.py`, `tests/**/*.py`                                  | [python.instructions.md](instructions/python.instructions.md)         |
| `sql/**/*.sql`, `docs/**/*.md`                                  | [databricks.instructions.md](instructions/databricks.instructions.md) |
| `**/test_*.py`, `**/*_test.py`                                  | [testing.instructions.md](instructions/testing.instructions.md)       |
| `pyproject.toml`, `Makefile`, `.github/**`, `src/**`, `sql/**`, `tests/**` | [quality.instructions.md](instructions/quality.instructions.md)       |

## Core Conventions

1. **`make <target>`** is the primary entry point (see Commands). `uv run task <name>` is a legacy mirror.
2. **Config centralised in `pyproject.toml`.** A few tools need their own dotfiles by design:
   `.sqlfluffignore`, `.safety-policy.yml`, and per-directory `.pages`. Do **not** add redundant
   configs (`.flake8`, `setup.cfg`, `ruff.toml`, `.mypy.ini`, `.isort.cfg`).
3. **Open-source Spark 4 first** — `spark.sql.ansi.enabled = true`, sqlfluff `dialect = sparksql`.
   Databricks-only features are labelled `[Databricks]` and excluded from linting via `.sqlfluffignore`.
4. **Label Databricks-only content** — see [databricks.instructions.md](instructions/databricks.instructions.md).
5. **No side effects on import** — Python modules must not execute code at import time.

# GitHub Copilot Instructions — spark-sql

This repository is a **Spark SQL knowledge base** with:

- `src/` — Spark SQL files (`.sql`) and PySpark scripts (`.py`), organised by topic
- `tests/` — pytest test suite using PySpark and chispa
- `docs/` — MkDocs Material site, navigation driven by `.pages` files
- `pyproject.toml` — single config file for all tools and tasks
- Package manager: **uv** (`uv run <tool>`)

## Modular Instructions

| Topic | Applies To | File |
|-------|-----------|------|
| MkDocs documentation | `docs/**/*.md`, `mkdocs.yml` | [docs.instructions.md](instructions/docs.instructions.md) |
| Spark SQL / SQL files | `src/**/*.sql` | [sql.instructions.md](instructions/sql.instructions.md) |
| Python / PySpark | `src/**/*.py` | [python.instructions.md](instructions/python.instructions.md) |
| Testing | `tests/**/*.py` | [testing.instructions.md](instructions/testing.instructions.md) |
| Quality & CI | `pyproject.toml`, workflows | [quality.instructions.md](instructions/quality.instructions.md) |

## Source Layout

```
src/
  aggregation/    # GROUP BY, ROLLUP, CUBE, GROUPING SETS
  application/    # end-to-end application queries
  column/         # column expressions, aliases, casting
  condition/      # CASE WHEN, IF, IFF
  control/        # flow control, LOOP, IF blocks
  cte/            # WITH clause patterns
  dml/            # INSERT, UPDATE, DELETE, MERGE, CREATE TABLE
  filter/         # WHERE, HAVING, QUALIFY
  function/       # built-in functions by category
  having/         # HAVING clause patterns
  join/           # all join types
  nulls/          # NULL handling
  operator/       # arithmetic, comparison, logical
  optimization/   # AQE, hints, partitioning, Z-ordering
  partition/      # PARTITION BY, window specs
  scd/            # Slowly Changing Dimensions (Type 1–6)
  subquery/       # correlated/uncorrelated subqueries
  timeseries/     # tumbling, hopping, sliding, session, gap-fill
  types/          # data type examples and casting
  view/           # CREATE VIEW, TEMP VIEW
  window/         # window functions
```

## Core Conventions

1. **Package manager is `uv`** — always prefix tool invocations with `uv run`.
2. **All config lives in `pyproject.toml`** — never create `.flake8`, `setup.cfg`, `.mypy.ini`, `.bandit`, `.isort.cfg`, or `ruff.toml`.
3. **Taskipy tasks are the entry points** — use `uv run task <name>` for quality, testing, docs, and builds.
4. **No side effects on import** — Python modules must not execute code at import time.
5. **Databricks Spark dialect** — SQL targets Databricks Runtime / Spark 3.5 with `ansi.enabled = true`.
6. **Material icons only** — use `:material-xxx:` icons in all docs; never use Unicode emoji.
7. **No `nav:` in `mkdocs.yml`** — navigation is owned entirely by `.pages` files in each directory.
8. **Strict MkDocs build** — `NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict` must always pass with zero warnings.
9. **Row hash pattern** — use `md5(concat_ws('||', col1, col2, ...))` for SCD change detection.
10. **Two-step MERGE for SCD Type 2/6** — a single MERGE cannot both expire and insert a new version row for the same key; always use two separate statements.

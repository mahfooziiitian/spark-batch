# GitHub Copilot Instructions — spark-sql

## Repository Overview

| Directory | Content |
|-----------|---------|
| `src/` | Spark SQL (`.sql`) and PySpark (`.py`) examples by topic |
| `tests/` | pytest + chispa test suite |
| `docs/` | MkDocs Material site (`.pages`-driven navigation) |
| `pyproject.toml` | Single config for all tools |

Package manager: **uv** (`uv run task <name>` for all workflows).

## Modular Instructions

| Scope | File |
|-------|------|
| `docs/**/*.md`, `mkdocs.yml` | [docs.instructions.md](instructions/docs.instructions.md) |
| `src/**/*.sql` | [sql.instructions.md](instructions/sql.instructions.md) |
| `src/**/*.py`, `tests/**/*.py` | [python.instructions.md](instructions/python.instructions.md) |
| `**/test_*.py`, `**/*_test.py` | [testing.instructions.md](instructions/testing.instructions.md) |
| `pyproject.toml`, `.github/**`, `src/**`, `tests/**` | [quality.instructions.md](instructions/quality.instructions.md) |

## Core Conventions

1. **`uv run task <name>`** — single entry point for quality, test, docs, build.
2. **All config in `pyproject.toml`** — no standalone config files.
3. **No side effects on import** — Python modules must not execute code at import time.
4. **Databricks Spark 3.5 dialect** — `ansi.enabled = true`.
5. **`:material-xxx:` icons only** — no Unicode emoji in docs.
6. **`.pages` owns navigation** — never add `nav:` to `mkdocs.yml`.
7. **Strict MkDocs build** — `uv run task docs_build` must pass with zero warnings.
8. **Row hash**: `md5(concat_ws('||', col1, col2, ...))` for SCD change detection.
9. **Two-step MERGE for SCD Type 2/6** — one MERGE cannot expire + insert for the same key.

# GitHub Copilot Instructions — spark-sql

This repository is a **Spark SQL knowledge base** with:

- `src/` — Spark SQL files (`.sql`) and PySpark scripts (`.py`)
- `tests/` — pytest test suite using PySpark and chispa
- `docs/` — MkDocs site with Material theme and awesome-pages plugin
- `pyproject.toml` — single config file for all tools
- Package manager: **uv** (`uv run <tool>`)

## Modular Instructions

| Topic | Applies To | File |
|-------|-----------|------|
| MkDocs documentation | `docs/**/*.md`, `mkdocs.yml` | [docs.instructions.md](instructions/docs.instructions.md) |
| Spark SQL / SQL files | `src/**/*.sql` | [sql.instructions.md](instructions/sql.instructions.md) |
| Python / PySpark | `src/**/*.py` | [python.instructions.md](instructions/python.instructions.md) |
| Testing | `tests/**/*.py` | [testing.instructions.md](instructions/testing.instructions.md) |
| Quality & CI | `pyproject.toml`, workflows | [quality.instructions.md](instructions/quality.instructions.md) |

## Core Conventions

1. **Package manager is `uv`** — always prefix tool invocations with `uv run`.
2. **All config lives in `pyproject.toml`** — do not create separate config files for black, ruff, mypy, flake8, bandit, pytest, or coverage.
3. **Taskipy tasks are the entry points** — use `uv run task <name>` for quality, testing, docs, and builds.
4. **No side effects on import** — Python modules must not execute code at import time.
5. **Databricks Spark dialect** — SQL targets Databricks Runtime / Spark 3.5.

# GitHub Copilot Instructions — pyspark-chispa

## Project Overview

A **PySpark testing reference project** demonstrating DataFrame quality testing with
the [chispa](https://github.com/MrPowers/chispa) library and pytest. The project
provides reusable PySpark utility functions under `src/` and comprehensive tests
under `tests/`.

## Tech Stack

| Component        | Version / Tool       |
| ---------------- | -------------------- |
| Python           | ≥ 3.11               |
| PySpark          | < 4.0.0 (3.5.x)     |
| Testing          | pytest, chispa       |
| Linting          | ruff                 |
| Type checking    | mypy                 |
| Docs             | MkDocs Material      |
| Package manager  | uv                   |
| Task runner      | taskipy              |

## Project Layout

```
pyspark-chispa/
├── src/
│   └── data_frame/
│       ├── columns/           # Column-level transformations
│       ├── equality/          # DataFrame comparison utilities
│       ├── functions/         # Column functions (math, etc.)
│       ├── helper/            # Pure Python helpers (no Spark dependency)
│       ├── schema/            # Schema utilities
│       └── transformation/    # DataFrame-level transformations
├── tests/
│   ├── conftest.py            # Shared session-scoped SparkSession fixture
│   ├── columns/               # Mirrors src/data_frame/columns/
│   ├── equality/              # Mirrors src/data_frame/equality/
│   ├── functions/             # Mirrors src/data_frame/functions/
│   ├── schema/                # Mirrors src/data_frame/schema/
│   └── transformation/        # Mirrors src/data_frame/transformation/
├── pyproject.toml             # All config: deps, pytest, ruff, mypy, taskipy
└── uv.lock
```

## Conventions

- Source in `src/`, tests in `tests/` — test layout mirrors source layout.
- `from pyspark.sql import functions as F` — never `import *`.
- Pure Python helpers (no Spark) go in `src/data_frame/helper/`.
- All config lives in `pyproject.toml` — no standalone `.flake8`, `mypy.ini`, etc.
- Use `uv` for dependency management, `taskipy` for tasks.
- Use **Google-style docstrings** on all public functions, classes, and modules.

## Taskipy Commands

```bash
uv run task test            # pytest -x --tb=short
uv run task test_parallel   # pytest -n auto --tb=short
uv run task lint            # ruff check src tests
uv run task lint_fix        # ruff check --fix src tests
uv run task format          # ruff format src tests
uv run task format_check    # ruff format --check src tests
uv run task typecheck       # mypy src
uv run task docs            # mkdocs build --strict
uv run task check           # lint + format_check + typecheck + test
uv run task clean           # remove caches and build artifacts
```

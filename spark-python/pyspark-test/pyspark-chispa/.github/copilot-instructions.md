# GitHub Copilot Instructions — pyspark-chispa

## Project Overview

A **PySpark testing reference project** demonstrating DataFrame quality testing with
the [chispa](https://github.com/MrPowers/chispa) library and pytest. The project
provides reusable PySpark utility functions under `src/data_frame/` and comprehensive
tests under `tests/`.

## Tech Stack

| Component        | Version / Tool       |
| ---------------- | -------------------- |
| Python           | ≥ 3.11               |
| PySpark          | ≥ 3.5, < 5.0         |
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
│       ├── columns/              ← Column-level transformations
│       │   └── column_equality.py
│       ├── equality/             ← DataFrame comparison utilities
│       │   └── df_equality.py
│       ├── functions/            ← Arithmetic column functions
│       │   └── functions.py
│       ├── helper/               ← Pure Python helpers (no Spark dependency)
│       │   └── string_helper.py
│       ├── schema/               ← Schema utilities
│       │   └── schema_utils.py
│       └── transformation/       ← DataFrame-level transformations
│           └── df_transformations.py
├── tests/
│   ├── conftest.py               ← Shared session-scoped SparkSession fixture
│   ├── columns/
│   │   └── test_column_equality.py
│   ├── equality/
│   │   └── test_df_equality.py
│   ├── functions/
│   │   └── test_functions.py
│   ├── helper/
│   │   └── test_string_helper.py
│   ├── schema/
│   │   └── test_schema.py
│   └── transformation/
│       └── test_df_transformation.py
├── docs/                         ← MkDocs Material documentation
├── pyproject.toml                ← All config: deps, pytest, ruff, mypy, taskipy
└── .python-version
```

## Conventions

- Source in `src/data_frame/`, tests in `tests/` — test layout mirrors source layout.
- `from pyspark.sql import functions as F` — never `import *`.
- Pure Python helpers (no Spark) go in `src/data_frame/helper/`.
- All config lives in `pyproject.toml` — no standalone `.flake8`, `mypy.ini`, etc.
- Use `uv` for dependency management, `taskipy` for tasks.
- Use **Google-style docstrings** on all public functions, classes, and modules.
- All functions must have type hints.
- Tests use a **shared conftest.py** fixture — do not create inline SparkSession fixtures.
- Tests are organised into classes by function: `class TestFunctionName`.

## Taskipy Commands

```bash
uv run task test            # pytest -x --tb=short
uv run task test_parallel   # pytest -n auto --tb=short
uv run task test_verbose    # pytest -v --tb=long
uv run task lint            # ruff check src tests
uv run task lint_fix        # ruff check --fix src tests
uv run task format          # ruff format src tests
uv run task format_check    # ruff format --check src tests
uv run task typecheck       # mypy src
uv run task docs            # mkdocs build --strict
uv run task check           # lint + format_check + typecheck + test
uv run task clean           # remove caches and build artifacts
```

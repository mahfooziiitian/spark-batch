# GitHub Copilot Instructions — pyspark-test (Root)

## Project Overview

A **mono-repo of PySpark testing reference projects**, each demonstrating a different
approach to testing PySpark applications. The projects are independent — each has its
own dependencies, config, tests, and (optionally) documentation.

## Child Projects

| Project          | Focus                                   | Package Manager    | Testing Library          |
| ---------------- | --------------------------------------- | ------------------ | ------------------------ |
| `pyspark-chispa` | DataFrame quality testing with chispa   | uv                 | pytest + chispa          |
| `pyspark-deepu`  | Data quality with PyDeequ              | uv                 | pytest + pydeequ         |
| `pyspark-pytest` | General PySpark testing patterns        | uv                 | pytest + pyspark.testing |

## Repository Layout

```
pyspark-test/                     ← you are here (root)
├── .github/
│   ├── copilot-instructions.md   ← this file
│   └── instructions/             ← root-level Copilot instruction files
├── .python-version               ← pinned Python version (3.11)
├── pyproject.toml                ← root project metadata
├── docs/                         ← root-level documentation
│   ├── index.md
│   └── getting-started/
├── pyspark-chispa/               ← chispa-based testing (uv, ruff, mypy, MkDocs)
│   ├── src/data_frame/           ← importable library modules (columns, equality, functions, helper, schema, transformation)
│   ├── tests/                    ← pytest + chispa assertions (shared conftest.py)
│   ├── docs/                     ← MkDocs Material documentation
│   └── pyproject.toml            ← all config: deps, pytest, ruff, mypy, taskipy
├── pyspark-deepu/                ← PyDeequ data quality (pip, setup.py)
│   ├── src/constraints/          ← verification & suggestion scripts
│   ├── src/mertics/              ← analyzers, profiles, repository scripts
│   ├── tests/                    ← pytest tests with pydeequ (per-file fixtures)
│   ├── setup.py                  ← package config
│   └── requirements.txt          ← pinned dependencies
└── pyspark-pytest/               ← general PySpark testing (poetry)
    ├── src/                      ← data processing, reader, transformation, faker utilities
    ├── tests/                    ← pytest + pyspark.testing assertions (shared conftest.py)
    ├── spark_docker/             ← Docker support for Spark
    └── pyproject.toml            ← poetry config with pytest settings
```

## Prerequisites

- **Java 11** (LTS) — required by PySpark; must be on `PATH` with `JAVA_HOME` set.
- **Python 3.11** — pinned via `.python-version` in each project.
- **Apache Spark / PySpark** — installed per-project via each project's package manager.

## Cross-Project Conventions

- **PySpark version**: 3.5.x preferred (except pyspark-deepu which uses 3.0.2 for PyDeequ compatibility, and pyspark-chispa which uses 3.3.2).
- **Python version**: 3.11 preferred; pinned in `.python-version` files.
- **Import style**: `from pyspark.sql import functions as F` — never `import *`.
- **SparkSession**: use `local[*]` for local mode; `setLogLevel("ERROR")` in tests, `"WARN"` in scripts.
- **Output format**: Parquet preferred over CSV.
- **Cleanup**: always call `spark.stop()` at the end of standalone scripts and in test fixture teardown (`yield` + `spark.stop()`).
- Each child project is self-contained — run commands from the child project directory.
- **No shared conftest.py at root** — each child project manages its own test fixtures.

## Working in This Repo

- Always `cd` into the specific child project before running commands.
- Each project has its own dependency installation and test commands — see the child project's `copilot-instructions.md`.
- Do not create cross-project imports; child projects are independent.
- When adding a new child project, follow the existing layout pattern with `.github/copilot-instructions.md` and `.github/instructions/`.
- Each child project should have its own `.python-version` file.

## Adding a New Child Project

1. Create project directory: `pyspark-<name>/`
2. Add `.github/copilot-instructions.md` with project overview, tech stack, layout, and conventions.
3. Add `.github/instructions/` with at minimum:
   - `pyspark.instructions.md` — source code conventions
   - `testing.instructions.md` — test patterns and fixtures
   - `project-structure.instructions.md` — dependency and layout rules
4. Add `.python-version` file pinning the Python version.
5. Set up package management (`setup.py`, `pyproject.toml`, or `requirements.txt`).
6. Create `src/` and `tests/` directories.
7. Update this root `copilot-instructions.md` to add the new project to the child projects table.

## Git Conventions

- Write clear, imperative commit messages (e.g., "Add null handling tests for column equality").
- Prefix commits with the child project name when changes are scoped to one project (e.g., "chispa: Add schema comparison tests").
- Keep each commit focused on a single logical change.
- Do not commit IDE-specific files, `__pycache__/`, `.pytest_cache/`, or virtual environments.

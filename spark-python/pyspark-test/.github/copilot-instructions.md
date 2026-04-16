# GitHub Copilot Instructions — pyspark-test (Root)

## Project Overview

A **mono-repo of PySpark testing reference projects**, each demonstrating a different
approach to testing PySpark applications. The projects are independent — each has its
own dependencies, config, tests, and (optionally) documentation.

## Child Projects

| Project          | Focus                                   | Package Manager | Testing Library        |
| ---------------- | --------------------------------------- | --------------- | ---------------------- |
| `pyspark-chispa` | DataFrame quality testing with chispa   | uv              | pytest + chispa        |
| `pyspark-deepu`  | Data quality with PyDeequ              | pip (setup.py)  | pytest + pydeequ       |
| `pyspark-pytest` | General PySpark testing patterns        | poetry          | pytest + pyspark.testing |

## Repository Layout

```
pyspark-test/                     ← you are here (root)
├── pyspark-chispa/               ← chispa-based testing (uv, ruff, mypy, MkDocs)
│   ├── src/data_frame/           ← importable library modules
│   ├── tests/                    ← pytest + chispa assertions
│   ├── docs/                     ← MkDocs Material documentation
│   └── pyproject.toml            ← all config in one place
├── pyspark-deepu/                ← PyDeequ data quality (pip, setup.py)
│   ├── src/constraints/          ← verification & suggestion scripts
│   ├── src/mertics/              ← analyzers, profiles, repository scripts
│   └── tests/                    ← pytest tests with pydeequ
└── pyspark-pytest/               ← general PySpark testing (poetry)
    ├── src/                      ← data processing, reader, transformation, faker utilities
    ├── tests/                    ← pytest + pyspark.testing assertions
    └── spark_docker/             ← Docker support for Spark
```

## Cross-Project Conventions

- **PySpark version**: 3.5.x preferred (except pyspark-deepu which uses 3.0.2 for PyDeequ compatibility).
- **Python version**: 3.11 preferred.
- **Import style**: `from pyspark.sql import functions as F` — never `import *`.
- **SparkSession**: use `local[*]` or `local[2]` for local mode; `setLogLevel("ERROR")` in tests, `"WARN"` in scripts.
- **Output format**: Parquet preferred over CSV.
- **Cleanup**: always call `spark.stop()` at the end of standalone scripts.
- Each child project is self-contained — run commands from the child project directory.

## Working in This Repo

- Always `cd` into the specific child project before running commands.
- Each project has its own dependency installation and test commands — see the child project's `copilot-instructions.md`.
- Do not create cross-project imports; child projects are independent.
- When adding a new child project, follow the existing layout pattern with `.github/copilot-instructions.md` and `.github/instructions/`.

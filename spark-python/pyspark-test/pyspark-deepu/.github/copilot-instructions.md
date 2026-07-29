# GitHub Copilot Instructions — pyspark-deepu

## Project Overview

A **PySpark data quality reference project** demonstrating the
[PyDeequ](https://github.com/awslabs/python-deequ) library — the Python wrapper
for AWS Deequ. It provides examples for constraint verification, constraint
suggestion, metric computation (analyzers & profilers), and metrics repositories.

## Tech Stack

| Component     | Version / Tool       |
| ------------- | -------------------- |
| Python        | ≥ 3.11               |
| PySpark       | ≥ 3.5                |
| PyDeequ       | ≥ 1.6                |
| Testing       | pytest ≥ 8.0         |
| Package mgmt  | uv                   |
| Task runner   | taskipy              |
| Linting       | ruff ≥ 0.11          |
| Type checking | mypy ≥ 1.15          |
| Documentation | MkDocs Material ≥ 9.7|

> **Note:** `SPARK_VERSION` env var must be set to `"3.5"` **before** importing
> pydeequ, as pydeequ reads it at import time to select the correct Deequ JAR.

## Project Layout

```
pyspark-deepu/
├── src/
│   ├── constraints/
│   │   ├── suggestions/
│   │   │   └── constraint_suggestions.py   ← ConstraintSuggestionRunner demo
│   │   └── verifications/
│   │       └── constraint_verification.py  ← VerificationSuite + Check demo
│   └── mertics/
│       ├── computations/
│       │   ├── analyzers/
│       │   │   └── analyzers.py            ← AnalysisRunner demo
│       │   └── profiles/
│       │       └── mertics_profile.py      ← ColumnProfilerRunner demo
│       └── repository/
│           └── repository.py               ← FileSystemMetricsRepository demo
├── tests/
│   ├── conftest.py                         ← Shared SparkSession + Deequ fixture
│   ├── constraints/
│   │   ├── test_verification.py            ← Verification constraint tests
│   │   └── test_suggestions.py             ← Suggestion runner tests
│   └── mertics/computations/
│       ├── test_analyzers.py               ← Analyzer metric tests
│       └── test_profiling.py               ← Column profiler tests
├── docs/                                   ← MkDocs documentation
├── mkdocs.yml
├── pyproject.toml                          ← All configuration (deps, pytest, ruff, mypy, taskipy)
└── README.md
```

## Conventions

### SPARK_VERSION Environment Variable

PyDeequ reads `SPARK_VERSION` at import time. Always set it **before** any
pydeequ import:

```python
import os
os.environ["SPARK_VERSION"] = "3.5"

import pydeequ  # noqa: E402
```

### SparkSession with PyDeequ

All scripts configure the PyDeequ Maven JAR in the SparkSession:

```python
spark = (SparkSession.builder
         .appName("deequ-example")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

### Script Pattern

Source files are **runnable scripts** with extractable functions and a `main()` entry point:

```python
def run_analysis(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Run analysis and return results."""
    ...

def main() -> None:
    """Run the demo."""
    spark = ...
    result = run_analysis(spark, df)
    spark.stop()

if __name__ == "__main__":
    main()
```

### Fixture Pattern

This project uses a **shared conftest.py** (`tests/conftest.py`) with a
session-scoped SparkSession configured with the Deequ JAR:

```python
os.environ["SPARK_VERSION"] = "3.5"

import pydeequ  # noqa: E402

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local[2]")
               .config("spark.jars.packages", pydeequ.deequ_maven_coord)
               .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

### Dependencies

```bash
uv sync --group dev                        # install all dependencies
```

### Available Tasks

```bash
uv run task test                           # run tests (stop on first failure)
uv run task test_verbose                   # verbose test output
uv run task lint                           # lint with ruff
uv run task format                         # format with ruff
uv run task typecheck                      # type check with mypy
uv run task check                          # full CI pipeline
uv run task docs                           # build docs
uv run task docs_serve                     # serve docs locally
uv run task clean                          # remove caches
```

## Key PyDeequ Modules Used

- `pydeequ.analyzers` — `AnalysisRunner`, `Size`, `Completeness`, `Mean`, etc.
- `pydeequ.checks` — `Check`, `CheckLevel`
- `pydeequ.verification` — `VerificationSuite`, `VerificationResult`
- `pydeequ.suggestions` — `ConstraintSuggestionRunner`
- `pydeequ.profiles` — `ColumnProfilerRunner`
- `pydeequ.repository` — `FileSystemMetricsRepository`, `ResultKey`

## Known Limitations

- PyDeequ requires the Deequ JAR to be downloaded at SparkSession startup, which
  needs internet access on first run.
- The `f2j` exclusion (`spark.jars.excludes`) is required to avoid classpath conflicts.
- `SPARK_VERSION` must be `"3.5"` — pydeequ reads it at import time, not at SparkSession creation.

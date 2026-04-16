# Project Structure

## Layout

```
pyspark-deepu/
├── src/
│   ├── constraints/
│   │   ├── suggestions/
│   │   │   └── constraint_suggestions.py   ← ConstraintSuggestionRunner demo
│   │   └── verifications/
│   │       └── constraint_verification.py  ← VerificationSuite demo
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
│   └── mertics/computations/
│       └── test_analyzers.py               ← Analyzer tests
├── docs/                                   ← MkDocs documentation
├── mkdocs.yml
└── pyproject.toml                          ← All configuration
```

## Design Principles

### Script-based demos

Source files are **runnable scripts** demonstrating individual PyDeequ features.
Each script has its own `if __name__ == '__main__':` block.

### SparkSession with Deequ JAR

Every script and test configures the Deequ Maven JAR:

```python
import pydeequ

spark = (SparkSession.builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())
```

### Test mirrors source

```
src/mertics/computations/analyzers/analyzers.py
    → tests/mertics/computations/test_analyzers.py
```

### Single conftest.py

The shared SparkSession fixture (with Deequ JAR) lives in `tests/conftest.py`.
Individual test files inject it via the `spark` parameter.

# GitHub Copilot Instructions — pyspark-deepu

## Project Overview

A **PySpark data quality reference project** demonstrating the
[PyDeequ](https://github.com/awslabs/python-deequ) library — the Python wrapper
for AWS Deequ. It provides examples for constraint verification, constraint
suggestion, metric computation (analyzers & profilers), and metrics repositories.

## Tech Stack

| Component     | Version / Tool       |
| ------------- | -------------------- |
| Python        | 3.x                  |
| PySpark       | 3.0.2                |
| PyDeequ       | 1.0.1                |
| Testing       | pytest               |
| Package mgmt  | pip + setup.py       |

## Project Layout

```
pyspark-deepu/
├── src/
│   ├── constraints/
│   │   ├── suggestions/
│   │   │   └── constraint_suggestions.py   ← ConstraintSuggestionRunner example
│   │   └── verifications/
│   │       └── constraint_verification.py  ← VerificationSuite + Check example
│   └── mertics/
│       ├── computations/
│       │   ├── analyzers/
│       │   │   └── analyzers.py            ← AnalysisRunner example
│       │   └── profiles/
│       │       └── mertics_profile.py      ← ColumnProfilerRunner example
│       └── repository/
│           └── repository.py               ← FileSystemMetricsRepository example
├── tests/
│   └── mertics/computations/
│       └── test_analyzers.py               ← pytest test for analyzers
├── requirements.txt
├── setup.py
└── Readme.md
```

## Conventions

### SparkSession with PyDeequ

All scripts configure the PyDeequ Maven JAR in the SparkSession:

```python
import os
import pydeequ
from pyspark.sql import SparkSession

os.environ["SPARK_VERSION"] = "3.0.2"

spark = (SparkSession.builder
         .appName("deequ-example")
         .master("local[*]")
         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
         .getOrCreate())
```

### Script Pattern

Source files are **runnable scripts** with `if __name__ == '__main__':` blocks,
not pure library modules. Each script demonstrates a specific PyDeequ feature.

### Environment Variable

Set `SPARK_VERSION` to match the installed PySpark version:

```python
os.environ["SPARK_VERSION"] = "3.0.2"
```

### Dependencies

Install via pip:

```bash
pip install -r requirements.txt
```

Or install in development mode:

```bash
pip install -e ".[tests]"
```

### Running Tests

```bash
pytest tests/
```

## Key PyDeequ Modules Used

- `pydeequ.analyzers` — `AnalysisRunner`, `Size`, `Completeness`, `Mean`, etc.
- `pydeequ.checks` — `Check`, `CheckLevel`
- `pydeequ.verification` — `VerificationSuite`, `VerificationResult`
- `pydeequ.suggestions` — `ConstraintSuggestionRunner`
- `pydeequ.profiles` — `ColumnProfilerRunner`
- `pydeequ.repository` — `FileSystemMetricsRepository`, `ResultKey`

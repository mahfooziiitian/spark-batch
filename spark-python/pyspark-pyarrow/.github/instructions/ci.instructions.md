---
applyTo: ".github/workflows/*.yml"
---

# GitHub Actions CI Instructions

## Workflow Structure

```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "11"
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install dependencies
        run: |
          pip install poetry
          poetry install
      - name: Run tests
        env:
          PYSPARK_PYTHON: python3
          PYSPARK_DRIVER_PYTHON: python3
          SPARK_LOCAL_IP: 127.0.0.1
        run: poetry run pytest tests/ -v --tb=short
```

## Required Environment Variables

Always set these for PySpark jobs in CI:

```yaml
env:
  PYSPARK_PYTHON: python3
  PYSPARK_DRIVER_PYTHON: python3
  SPARK_LOCAL_IP: 127.0.0.1
```

## Java Version

Use **Java 11** (Temurin distribution) for PySpark 3.5.x compatibility.
Java 17 and 21 require additional `--add-opens` JVM flags for Arrow.

## Caching

Cache Poetry virtualenvs to speed up builds:

```yaml
- uses: actions/cache@v4
  with:
    path: ~/.cache/pypoetry
    key: poetry-${{ hashFiles('**/poetry.lock') }}
    restore-keys: poetry-
```
